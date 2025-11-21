package opcua

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	"uav-aas-connector/pkg/instrumentation"

	aastypes "github.com/aas-core-works/aas-core3.0-golang/types"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

type Exporter struct {
	client    *opcua.Client
	core      *core.AASCore // Reference to core to query types
	endpoint  string
	namespace int
}

func NewExporter(cfg config.ExporterConfig, core *core.AASCore) (*Exporter, error) {
	host := "localhost"
	if cfg.Host != "" {
		host = cfg.Host
	}
	endpoint := fmt.Sprintf("opc.tcp://%s:%d/freeopcua/server/", host, cfg.Port)

	log.Printf("[OPCUA] Initializing exporter for: %s", endpoint)

	c, err := opcua.NewClient(endpoint, opcua.SecurityMode(ua.MessageSecurityModeNone))
	if err != nil {
		return nil, fmt.Errorf("failed to create OPC UA client: %w", err)
	}

	return &Exporter{
		client:    c,
		core:      core, // Store the core
		endpoint:  endpoint,
		namespace: 2,
	}, nil
}

func (e *Exporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	log.Printf("[OPCUA] Connecting...")
	if err := e.client.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer e.client.Close(ctx)
	log.Println("[OPCUA] Connected.")

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-inChan:
			if !ok {
				log.Println("[OPCUA] Input channel closed, stopping.")
				return nil
			}
			if event == nil {
				continue
			}
			if err := e.writeDynamic(ctx, event); err != nil {
				// Log debug only to avoid flooding if a node doesn't exist yet
				// log.Printf("[OPCUA Debug] Write failed for %s: %v", event.PropertyID, err)
			}
		}
	}
}

func (e *Exporter) Stop() error {
	if e.client != nil {
		return e.client.Close(context.Background())
	}
	return nil
}

func (e *Exporter) writeDynamic(ctx context.Context, event *core.AASUpdateEvent) error {
	// 1. Query the AAS Model for the definition of this property
	dataType, err := e.core.GetElementDataType(event.SubmodelID, event.PropertyID)
	if err != nil {
		return fmt.Errorf("unknown property in AAS: %w", err)
	}

	// 2. Convert string value to the specific OPC UA Variant
	variant, err := e.convertToVariant(event.Value, dataType)
	if err != nil {
		return err
	}

	// 3. Construct NodeID dynamically based on the Property Path
	// Example: PropertyID="Position.Latitude" -> NodeID="ns=2;s=Position.Latitude"
	// This requires the OPC UA Server to use the same naming convention.
	id := ua.NewStringNodeID(uint16(e.namespace), event.PropertyID)

	req := &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{
			{
				NodeID:      id,
				AttributeID: ua.AttributeIDValue,
				Value: &ua.DataValue{
					EncodingMask: ua.DataValueValue,
					Value:        variant,
				},
			},
		},
	}

	resp, err := e.client.Write(ctx, req)

	// T2: Record egress time
	// We use event.Timestamp (T0) as the TraceID
	instrumentation.Record("EXPORTER", event.PropertyID, event.Timestamp)

	if err != nil {
		return err
	}
	if len(resp.Results) > 0 && resp.Results[0] != ua.StatusOK {
		return fmt.Errorf("status: %v", resp.Results[0])
	}

	return nil
}

// convertToVariant maps AAS XSD Types -> Go Types -> OPC UA Variants
// Optimized: accepts 'any' to allow direct float64 handling
func (e *Exporter) convertToVariant(val any, aasType aastypes.DataTypeDefXSD) (*ua.Variant, error) {

	// If val is already a float64 (from MAVLink adapter), use it directly.
	if f, ok := val.(float64); ok {
		switch aasType {
		case aastypes.DataTypeDefXSDDouble:
			return ua.MustVariant(f), nil
		case aastypes.DataTypeDefXSDFloat:
			return ua.MustVariant(float32(f)), nil
		case aastypes.DataTypeDefXSDInt, aastypes.DataTypeDefXSDInteger,
			aastypes.DataTypeDefXSDPositiveInteger, aastypes.DataTypeDefXSDNonNegativeInteger:
			return ua.MustVariant(int32(f)), nil
		case aastypes.DataTypeDefXSDBoolean:
			return ua.MustVariant(f != 0), nil
		default:
			// For other types (e.g. string), fall through to the string conversion below
		}
	}

	// Fallback: Convert to string and parse.
	// This handles types not handled in the fast path.
	valStr := fmt.Sprintf("%v", val)

	switch aasType {
	case aastypes.DataTypeDefXSDBoolean:
		v, err := strconv.ParseBool(valStr)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(v), nil

	case aastypes.DataTypeDefXSDFloat:
		v, err := strconv.ParseFloat(valStr, 32)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(float32(v)), nil

	case aastypes.DataTypeDefXSDDouble:
		v, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(v), nil

	case aastypes.DataTypeDefXSDInt, aastypes.DataTypeDefXSDPositiveInteger, aastypes.DataTypeDefXSDNonNegativeInteger:
		// Try parsing as int first
		if v, err := strconv.ParseInt(valStr, 10, 32); err == nil {
			return ua.MustVariant(int32(v)), nil
		}
		// Fallback: Parse as float and cast to int (handles "12.0")
		if f, err := strconv.ParseFloat(valStr, 64); err == nil {
			return ua.MustVariant(int32(f)), nil
		}
		return nil, fmt.Errorf("cannot parse '%s' as int32", valStr)

	case aastypes.DataTypeDefXSDLong, aastypes.DataTypeDefXSDInteger:
		v, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(v), nil

	case aastypes.DataTypeDefXSDString:
		return ua.MustVariant(valStr), nil

	default:
		// Fallback for complex types or unknowns
		return ua.MustVariant(valStr), nil
	}
}
