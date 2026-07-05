// Package mavlink implements an InputAdapter for the MAVLink protocol.
package mavlink

import (
	"context"
	"fmt"
	"log/slog"
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
	slog.Info("Mavlink mapping loaded", "mapping", mappingCfg)

	// Create the mapping rules lookup map.
	rules := make(map[string][]config.MappingEntry)
	for _, entry := range mappingCfg.Mappings {
		canonicalName := strings.ToUpper(entry.SourceMessage)
		rules[canonicalName] = append(rules[canonicalName], entry)
	}
	slog.Debug("Mavlink mapping rules loaded", "rules", len(rules))

	// Configure the gomavlib endpoint.
	var endpoint gomavlib.EndpointConf
	switch cfg.Connection {
	case "udp":
		endpoint = gomavlib.EndpointUDPServer{Address: cfg.Address}
	case "tcp":
		endpoint = gomavlib.EndpointTCPServer{Address: cfg.Address}
	case "serial":
		endpoint = gomavlib.EndpointSerial{Device: cfg.Address, Baud: cfg.Baudrate}
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
	slog.Info("Starting Mavlink adapter", "address", a.address)

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
				slog.Info("Mavlink channel open")

			case *gomavlib.EventChannelClose:
				slog.Info("Mavlink channel closed")
			}
		}
	}
}

// Stop gracefully shuts down the MAVLink node.
func (a *MavlinkAdapter) Stop() error {
	slog.Info("Stopping Mavlink adapter")
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
		slog.Debug("No mapping rules for MAVLink message type", "type", canonical)
		return // Message ignored
	}

	ts := time.Now().UTC()

	// Iterate through all rules for this message
	for _, entry := range rules {
		// Extract field using cached index lookup
		rawValue, ok := a.extractField(msg, entry.SourceField)
		if !ok {
			continue
		}

		var valueToPass any = rawValue

		rv := reflect.ValueOf(rawValue)
		if rv.IsValid() {
			k := rv.Kind()
			// Handle signed integers (including enums like MavState)
			if k >= reflect.Int && k <= reflect.Int64 {
				valueToPass = rv.Int() // Standardizes to int64
			}
			// Handle unsigned integers (including enums like MavType)
			if k >= reflect.Uint && k <= reflect.Uint64 {
				valueToPass = rv.Uint() // Standardizes to uint64
			}
		}
		newValue, err := a.applyTransform(valueToPass, entry.Scale)
		if err != nil {
			slog.Warn("Transform error", "field", entry.SourceField, "error", err)
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
			slog.Warn("AASUpdateEvent channel full, discarding update", "property", updateEvent.PropertyID)
		}
	}
}

// extractField uses cached reflection indices to get a field's value quickly.
// It supports array indexing in fieldName, e.g., "Motors[0]".
func (a *MavlinkAdapter) extractField(msg message.Message, fieldName string) (any, bool) {
	rv := reflect.ValueOf(msg)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()

	if rv.Kind() != reflect.Struct {
		return nil, false
	}

	// 1. Parse Field Name and Index (if present)
	baseName := fieldName
	arrayIdx := -1

	// Check for bracket syntax like "Field[0]"
	if left := strings.Index(fieldName, "["); left > 0 && strings.HasSuffix(fieldName, "]") {
		baseName = fieldName[:left]
		idxStr := fieldName[left+1 : len(fieldName)-1]
		if i, err := strconv.Atoi(idxStr); err == nil {
			arrayIdx = i
		}
	}

	// 2. Resolve Base Field via Cache
	if a.fieldIndexCache[rt] == nil {
		a.fieldIndexCache[rt] = make(map[string]int)
	}

	idx, ok := a.fieldIndexCache[rt][baseName]
	if !ok {
		// Not cached: Perform linear search to find index
		found := false
		for i := 0; i < rt.NumField(); i++ {
			if rt.Field(i).Name == baseName {
				idx = i
				found = true
				break
			}
		}
		if !found {
			return nil, false
		}
		// Store in cache
		a.fieldIndexCache[rt][baseName] = idx
	}

	// 3. Get Field Value
	fieldVal := rv.Field(idx)

	// 4. Handle Array/Slice Indexing
	if arrayIdx >= 0 {
		k := fieldVal.Kind()
		if k == reflect.Array || k == reflect.Slice {
			if arrayIdx < fieldVal.Len() {
				return fieldVal.Index(arrayIdx).Interface(), true
			}
			// Index out of bounds
			return nil, false
		}
		// Field is not an array, but index was requested
		return nil, false
	}

	// Fast access by index
	return fieldVal.Interface(), true
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
