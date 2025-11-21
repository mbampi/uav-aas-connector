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

	// mappingRules holds the loaded transformation logic, keyed by
	// canonical MAVLink message name (e.g., "GPS_RAW_INT").
	mappingRules map[string][]config.MappingEntry
}

// NewAdapter creates a new MavlinkAdapter.
// This function will be called by the NewInputAdapter factory.
func NewAdapter(cfg config.AdapterConfig) (*MavlinkAdapter, error) {
	// 1. Load the adapter-specific mapping file (e.g., mavlink.yaml)
	mappingCfg, err := loadMapping(cfg.MappingFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load mavlink mapping file '%s': %w", cfg.MappingFile, err)
	}
	log.Printf("Mavlink adapter loaded mapping: %+v ", mappingCfg)

	// 2. Create the mapping rules lookup map for efficient processing.
	rules := make(map[string][]config.MappingEntry)
	for _, entry := range mappingCfg.Mappings {
		// Normalize message name to canonical MAVLink uppercase (e.g., GPS_RAW_INT)
		canonicalName := strings.ToUpper(entry.SourceMessage)
		rules[canonicalName] = append(rules[canonicalName], entry)
	}
	log.Printf("Mavlink adapter loaded %d mapping rules for %d message types", len(mappingCfg.Mappings), len(rules))
	log.Printf("Mavlink adapter mapping rules: %+v", rules)

	// 3. Configure the gomavlib endpoint based on config
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

	// 4. Create the gomavlib node
	node := &gomavlib.Node{
		Endpoints:           []gomavlib.EndpointConf{endpoint},
		Dialect:             common.Dialect,
		HeartbeatDisable:    true,
		StreamRequestEnable: true, // Automatically request streams from the UAV
		OutVersion:          2,
		OutSystemID:         1,
	}

	err = node.Initialize()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize gomavlib node: %w", err)
	}

	return &MavlinkAdapter{
		node:         node,
		address:      cfg.Address,
		mappingRules: rules,
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

// Start begins the adapter's work loop, listening for MAVLink messages.
// It now sends core.AASUpdateEvent messages to the outChan.
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
				// A MAVLink message frame was received
				log.Printf("Received MAVLink message: %T", e.Frame.GetMessage())
				a.handleFrame(e)

			case *gomavlib.EventChannelOpen:
				log.Printf("Mavlink channel open")

			case *gomavlib.EventChannelClose:
				log.Printf("Mavlink channel closed")
			default:
				log.Printf("Mavlink adapter received unhandled event: %T", e)
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
// This is the core 1-step transformation logic.
func (a *MavlinkAdapter) handleFrame(e *gomavlib.EventFrame) {
	msg := e.Frame.GetMessage()
	if msg == nil {
		return
	}

	// Get canonical message name (e.g., "GPS_RAW_INT")
	goTypeName := reflect.TypeOf(msg).Elem().Name()
	canonical := goTypeToMavName(goTypeName)

	// Check if we have any mapping rules for this message type
	rules, ok := a.mappingRules[canonical]
	if !ok {
		log.Printf("Message ignored: %s", canonical)
		return // We don't care about this message, ignore it
	}
	log.Printf("Processing message: %s", canonical)

	ts := time.Now().UTC() // T0 and our TraceID

	// This message is mapped. Iterate through all rules defined for it.
	for _, entry := range rules {
		// 1. Extract the specific source field (e.g., "Lat")
		rawValue, ok := a.extractField(msg, entry.SourceField)
		if !ok {
			log.Printf("Warning: field %s not found in message %s", entry.SourceField, canonical)
			continue
		}

		// 2. Apply transformations (e.g., scale)
		newValue, err := a.applyTransform(rawValue, entry.Scale)
		if err != nil {
			log.Printf("Warning: failed to apply transform for %s: %v", entry.SourceField, err)
			continue
		}

		// 3. Create the specific AASUpdateEvent
		updateEvent := &core.AASUpdateEvent{
			SubmodelID: entry.TargetAASSubmodel,
			PropertyID: entry.TargetAASProperty,
			Value:      newValue,
			Timestamp:  ts,
		}

		// T0: Record the ingress
		instrumentation.Record("ADAPTER", updateEvent.PropertyID, updateEvent.Timestamp)

		// 4. Send the event to the core
		select {
		case a.outChan <- updateEvent:
			log.Printf("Sent AASUpdateEvent: %+v", updateEvent)
		default:
			log.Printf("Warning: AASUpdateEvent channel full. Discarding update for: %s", updateEvent.PropertyID)
		}
	}
}

// applyTransform applies configured transformations (like scaling) to a raw value.
func (a *MavlinkAdapter) applyTransform(value any, scaleStr string) (any, error) {
	// 1. No transformation needed
	if scaleStr == "" {
		return value, nil
	}

	// 2. Parse scale factor
	scale, err := strconv.ParseFloat(scaleStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid scale value '%s': %w", scaleStr, err)
	}

	// 3. Apply scale based on the raw value's type
	// We promote all scaled values to float64 for consistency in the AAS.
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

// goTypeToMavName converts a Go struct type name for a generated MAVLink message
// (for example: MessageGpsRawInt or MessageHeartbeat) into the canonical
// MAVLink message name used in mappings (for example: GPS_RAW_INT or
// HEARTBEAT).
func goTypeToMavName(goName string) string {
	// Drop leading "Message" prefix if present
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

// extractField uses reflection to get a single field's value from a
// MAVLink message struct by its name.
func (a *MavlinkAdapter) extractField(msg message.Message, fieldName string) (any, bool) {
	// Message implementations are pointers to structs.
	rv := reflect.ValueOf(msg)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, false
	}

	// Find the field by its Go struct name (e.g., "Lat", "Lon")
	fieldValue := rv.FieldByName(fieldName)
	if !fieldValue.IsValid() {
		return nil, false // Field doesn't exist
	}

	return fieldValue.Interface(), true
}
