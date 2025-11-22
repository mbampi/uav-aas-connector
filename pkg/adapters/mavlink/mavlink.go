// Package mavlink implements an InputAdapter for the MAVLink protocol.
package mavlink

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/message"
	"gopkg.in/yaml.v3"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	"uav-aas-connector/pkg/instrumentation"
)

// MavlinkAdapter implements the adapters.InputAdapter interface for MAVLink.
type MavlinkAdapter struct {
	node    *gomavlib.Node
	address string
	outChan chan<- *core.AASUpdateEvent

	// mappingRules holds the loaded transformation logic.
	mappingRules map[string][]config.MappingEntry

	// Caches to optimize reflection performance (O(1) lookups)
	typeNameCache   map[reflect.Type]string
	fieldIndexCache map[reflect.Type]map[string]int
}

// NewAdapter creates a new MavlinkAdapter.
func NewAdapter(cfg config.AdapterConfig) (*MavlinkAdapter, error) {
	mappingCfg, err := loadMapping(cfg.MappingFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load mavlink mapping file '%s': %w", cfg.MappingFile, err)
	}
	log.Printf("Mavlink adapter loaded mapping: %+v ", mappingCfg)

	// Create the mapping rules lookup map.
	rules := make(map[string][]config.MappingEntry)
	for _, entry := range mappingCfg.Mappings {
		canonicalName := strings.ToUpper(entry.SourceMessage)
		rules[canonicalName] = append(rules[canonicalName], entry)
	}
	log.Printf("Mavlink adapter loaded %d mapping rules", len(rules))

	// Configure the gomavlib endpoint.
	var endpoint gomavlib.EndpointConf
	switch cfg.Connection {
	case "udp":
		endpoint = gomavlib.EndpointUDPServer{Address: cfg.Address}
	case "tcp":
		endpoint = gomavlib.EndpointTCPServer{Address: cfg.Address}
	case "serial":
		endpoint = gomavlib.EndpointSerial{Device: cfg.Address, Baud: 57600}
	default:
		return nil, fmt.Errorf("unsupported mavlink connection type: %s", cfg.Connection)
	}

	node := &gomavlib.Node{
		Endpoints:           []gomavlib.EndpointConf{endpoint},
		Dialect:             common.Dialect,
		HeartbeatDisable:    true,
		StreamRequestEnable: true,
		OutVersion:          2,
		OutSystemID:         1,
	}

	if err := node.Initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize gomavlib node: %w", err)
	}

	return &MavlinkAdapter{
		node:            node,
		address:         cfg.Address,
		mappingRules:    rules,
		typeNameCache:   make(map[reflect.Type]string),
		fieldIndexCache: make(map[reflect.Type]map[string]int),
	}, nil
}

// loadMapping loads the mapping configuration file.
func loadMapping(path string) (*config.MappingConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	var cfg config.MappingConfig
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to decode yaml: %w", err)
	}

	return &cfg, nil
}

// Start begins the adapter's work loop.
func (a *MavlinkAdapter) Start(ctx context.Context, outChan chan<- *core.AASUpdateEvent) error {
	a.outChan = outChan
	log.Printf("Starting Mavlink adapter for %s", a.address)

	for {
		select {
		case <-ctx.Done():
			// Context was canceled, initiate shutdown
			return a.Stop()

		case evt := <-a.node.Events():
			// Handle events from the gomavlib node
			switch e := evt.(type) {
			case *gomavlib.EventFrame:
				a.handleFrame(e)

			case *gomavlib.EventChannelOpen:
				log.Printf("Mavlink channel open")

			case *gomavlib.EventChannelClose:
				log.Printf("Mavlink channel closed")
			}
		}
	}
}

// Stop gracefully shuts down the MAVLink node.
func (a *MavlinkAdapter) Stop() error {
	log.Println("Stopping Mavlink adapter")
	if a.node != nil {
		a.node.Close()
	}
	return nil
}

// handleFrame processes a received MAVLink message.
func (a *MavlinkAdapter) handleFrame(e *gomavlib.EventFrame) {
	msg := e.Frame.GetMessage()
	if msg == nil {
		return
	}

	// Resolve Canonical Name (Cached)
	msgType := reflect.TypeOf(msg)
	if msgType.Kind() == reflect.Ptr {
		msgType = msgType.Elem()
	}

	canonical, ok := a.typeNameCache[msgType]
	if !ok {
		canonical = goTypeToMavName(msgType.Name())
		a.typeNameCache[msgType] = canonical
	}

	// Check if we have rules for this message type
	rules, ok := a.mappingRules[canonical]
	if !ok {
		return // Message ignored
	}

	ts := time.Now().UTC()

	// Iterate through all rules for this message
	for _, entry := range rules {
		// Extract field using cached index lookup
		rawValue, ok := a.extractField(msg, entry.SourceField)
		if !ok {
			// Optional: log warning if needed, but skipping to reduce I/O in high freq
			continue
		}

		newValue, err := a.applyTransform(rawValue, entry.Scale)
		if err != nil {
			log.Printf("Warning: transform error for %s: %v", entry.SourceField, err)
			continue
		}

		updateEvent := &core.AASUpdateEvent{
			SubmodelID: entry.TargetAASSubmodel,
			PropertyID: entry.GetPath(),
			Value:      newValue,
			Timestamp:  ts,
		}

		instrumentation.Record("ADAPTER", updateEvent.PropertyID, updateEvent.Timestamp)

		select {
		case a.outChan <- updateEvent:
			// Success
		default:
			log.Printf("Warning: AASUpdateEvent channel full. Discarding update for: %s", updateEvent.PropertyID)
		}
	}
}

// extractField uses cached reflection indices to get a field's value quickly.
func (a *MavlinkAdapter) extractField(msg message.Message, fieldName string) (any, bool) {
	rv := reflect.ValueOf(msg)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()

	if rv.Kind() != reflect.Struct {
		return nil, false
	}

	// Ensure cache map exists for this type
	if a.fieldIndexCache[rt] == nil {
		a.fieldIndexCache[rt] = make(map[string]int)
	}

	// Check cache for field index
	idx, ok := a.fieldIndexCache[rt][fieldName]
	if !ok {
		// Not cached: Perform linear search to find index
		found := false
		for i := 0; i < rt.NumField(); i++ {
			if rt.Field(i).Name == fieldName {
				idx = i
				found = true
				break
			}
		}
		if !found {
			return nil, false
		}
		// Store in cache
		a.fieldIndexCache[rt][fieldName] = idx
	}

	// Fast access by index
	return rv.Field(idx).Interface(), true
}

// applyTransform applies scaling.
func (a *MavlinkAdapter) applyTransform(value any, scaleStr string) (any, error) {
	if scaleStr == "" {
		return value, nil
	}

	scale, err := strconv.ParseFloat(scaleStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid scale value '%s': %w", scaleStr, err)
	}

	switch v := value.(type) {
	case float64:
		return v * scale, nil
	case float32:
		return float64(v) * scale, nil
	case int:
		return float64(v) * scale, nil
	case int8:
		return float64(v) * scale, nil
	case int16:
		return float64(v) * scale, nil
	case int32:
		return float64(v) * scale, nil
	case int64:
		return float64(v) * scale, nil
	case uint8:
		return float64(v) * scale, nil
	case uint16:
		return float64(v) * scale, nil
	case uint32:
		return float64(v) * scale, nil
	case uint64:
		return float64(v) * scale, nil
	default:
		return nil, fmt.Errorf("unsupported type for scaling: %T", value)
	}
}

// goTypeToMavName converts Go struct name to Canonical MAVLink name.
func goTypeToMavName(goName string) string {
	goName = strings.TrimPrefix(goName, "Message")
	var b strings.Builder
	runes := []rune(goName)
	for i, r := range runes {
		if i > 0 {
			prev := runes[i-1]
			if isUpper(r) && isLower(prev) {
				b.WriteRune('_')
			}
			if isUpper(r) && isUpper(prev) && i+1 < len(runes) && isLower(runes[i+1]) {
				b.WriteRune('_')
			}
		}
		b.WriteRune(r)
	}
	return strings.ToUpper(b.String())
}

func isUpper(r rune) bool { return r >= 'A' && r <= 'Z' }
func isLower(r rune) bool { return r >= 'a' && r <= 'z' }
