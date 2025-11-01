// Package mavlink implements an InputAdapter for the MAVLink protocol.
package mavlink

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/message"
	"gopkg.in/yaml.v3"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
)

// mavlinkMappingConfig defines the structure of the adapter-specific YAML file
// (e.g., config/mappings/mavlink.yaml).
// It lists which MAVLink messages should be processed.
type mavlinkMappingConfig struct {
	// Messages is a list of MAVLink message names to capture (e.g., "HEARTBEAT", "GPS_RAW_INT").
	Messages []string `yaml:"messages"`
}

// MavlinkAdapter implements the adapters.InputAdapter interface for MAVLink.
type MavlinkAdapter struct {
	node    *gomavlib.Node
	address string
	outChan chan<- *core.RawDataPoint

	// messageAllowSet contains the set of message names to process,
	// loaded from the config file for fast lookups.
	messageAllowSet map[string]struct{}
}

// NewAdapter creates a new MavlinkAdapter.
// This function will be called by the NewInputAdapter factory.
func NewAdapter(cfg config.AdapterConfig) (*MavlinkAdapter, error) {
	// 1. Load the adapter-specific message mapping file
	mappingCfg, err := loadMapping(cfg.MappingFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load mavlink mapping file '%s': %w", cfg.MappingFile, err)
	}

	// 2. Create the allow-set for efficient message filtering. Normalize mapping
	// entries to canonical MAVLink uppercase names (e.g., GPS_RAW_INT).
	allowSet := make(map[string]struct{})
	for _, msgName := range mappingCfg.Messages {
		allowSet[strings.ToUpper(msgName)] = struct{}{}
	}
	log.Printf("Mavlink adapter will process %d message types: %v", len(allowSet), mappingCfg.Messages)

	// 3. Configure the gomavlib endpoint based on config
	var endpoint gomavlib.EndpointConf
	switch cfg.Connection {
	case "udp":
		endpoint = gomavlib.EndpointUDPClient{Address: cfg.Address}
	case "tcp":
		endpoint = gomavlib.EndpointTCPClient{Address: cfg.Address}
	// Add other connection types like "serial" if needed
	default:
		return nil, fmt.Errorf("unsupported mavlink connection type: %s", cfg.Connection)
	}

	// 4. Create the gomavlib node
	// CORRECTED: Instantiate Node and call Initialize()
	node := &gomavlib.Node{
		Endpoints: []gomavlib.EndpointConf{endpoint},
		// Use the "common" MAVLink dialect.
		Dialect:             common.Dialect,
		HeartbeatDisable:    true, // This adapter only listens
		StreamRequestEnable: true, // Automatically request streams from the UAV
	}

	// Initialize the node
	err = node.Initialize()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize gomavlib node: %w", err)
	}

	return &MavlinkAdapter{
		node:            node,
		address:         cfg.Address,
		messageAllowSet: allowSet,
	}, nil
}

// loadMapping loads the list of messages to watch from the adapter's config file.
func loadMapping(path string) (*mavlinkMappingConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	var cfg mavlinkMappingConfig
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to decode yaml: %w", err)
	}
	return &cfg, nil
}

// Start begins the adapter's work loop, listening for MAVLink messages.
func (a *MavlinkAdapter) Start(ctx context.Context, outChan chan<- *core.RawDataPoint) error {
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
				a.handleFrame(e)

			case *gomavlib.EventChannelOpen:
				log.Printf("Mavlink channel open: %s", e.Channel.Endpoint())

			case *gomavlib.EventChannelClose:
				log.Printf("Mavlink channel closed: %s", e.Channel.Endpoint())
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

	var goTypeName string
	t := reflect.TypeOf(msg)
	if t.Kind() == reflect.Ptr {
		goTypeName = t.Elem().Name()
	} else {
		goTypeName = t.Name()
	}

	// Convert Go type name (e.g., MessageGpsRawInt) into the canonical MAVLink
	// name (e.g., GPS_RAW_INT) and check the allow-list using that canonical
	// form. Also accept mappings that accidentally used the Go type name.
	canonical := goTypeToMavName(goTypeName)
	if _, ok := a.messageAllowSet[canonical]; !ok {
		if _, ok2 := a.messageAllowSet[goTypeName]; !ok2 {
			return // We don't care about this message, ignore it
		}
	}

	rdp := &core.RawDataPoint{
		SourceID:    fmt.Sprintf("mavlink-sys-%d-comp-%d", e.Frame.GetSystemID(), e.Frame.GetComponentID()),
		MessageName: canonical,
		Timestamp:   time.Now().UTC(),
		Fields:      a.extractFields(msg),
	}

	select {
	case a.outChan <- rdp:
	default:
		log.Printf("Warning: RawDataPoint channel full. Discarding mavlink message: %s", canonical)
	}
}

// goTypeToMavName converts a Go struct type name for a generated MAVLink message
// (for example: MessageGpsRawInt or MessageHeartbeat) into the canonical
// MAVLink message name used in mappings (for example: GPS_RAW_INT or
// HEARTBEAT).
func goTypeToMavName(goName string) string {
	// Drop leading "Message" prefix if present
	goName = strings.TrimPrefix(goName, "Message")

	// Convert PascalCase to SNAKE_UPPER. Insert underscore before transitions
	// from lower->upper and from upper->upper followed by lower (to split
	// acronyms). Then uppercase the result.
	var b strings.Builder
	runes := []rune(goName)
	for i, r := range runes {
		if i > 0 {
			prev := runes[i-1]
			// If current is upper and previous is lower, split: aB -> a_B
			if isUpper(r) && isLower(prev) {
				b.WriteRune('_')
			}
			// If current is upper and previous is upper and next is lower,
			// split before current to separate acronym from next word: GPSRaw -> GPS_Raw
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

// extractFields uses reflection to generically get all exported fields
// from any MAVLink message struct.
func (a *MavlinkAdapter) extractFields(msg message.Message) map[string]any {
	fields := make(map[string]any)
	// Use reflection but be defensive: message.Message implementations are
	// usually pointers to structs. Handle both pointer and non-pointer types.
	rv := reflect.ValueOf(msg)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return fields
	}

	t := rv.Type()
	for i := 0; i < rv.NumField(); i++ {
		field := t.Field(i)
		value := rv.Field(i)

		if !field.IsExported() {
			continue
		}

		fieldName := field.Name
		if fieldName == "Def" {
			continue
		}

		fields[fieldName] = value.Interface()
	}

	return fields
}
