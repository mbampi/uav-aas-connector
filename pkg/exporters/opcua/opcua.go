package opcua

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	"uav-aas-connector/pkg/instrumentation"

	aastypes "github.com/aas-core-works/aas-core3.0-golang/types"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

// Exporter implements an OPC UA output exporter for the UAV AAS Connector.
type Exporter struct {
	client   *opcua.Client
	core     *core.AASCore
	endpoint string

	namespace   uint16
	typeCache   map[string]aastypes.DataTypeDefXSD
	nodeIDCache map[string]*ua.NodeID
}

// NewExporter creates a new OPC UA exporter instance.
func NewExporter(cfg config.ExporterConfig, core *core.AASCore) (*Exporter, error) {
	host := "localhost"
	if cfg.Host != "" {
		host = cfg.Host
	}
	endpoint := fmt.Sprintf("opc.tcp://%s:%d", host, cfg.Port)

	log.Printf("[OPCUA] Initializing exporter for: %s", endpoint)

	c, err := opcua.NewClient(endpoint,
		opcua.SecurityMode(ua.MessageSecurityModeNone),
		opcua.AuthAnonymous(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OPC UA client: %w", err)
	}

	ns := uint16(2)

	return &Exporter{
		client:      c,
		core:        core,
		endpoint:    endpoint,
		namespace:   ns,
		typeCache:   make(map[string]aastypes.DataTypeDefXSD),
		nodeIDCache: make(map[string]*ua.NodeID),
	}, nil
}

// Start begins processing AAS update events and exporting them to the OPC UA server.
func (e *Exporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	log.Printf("[OPCUA] Connecting to %s (NS=%d)...", e.endpoint, e.namespace)
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
				return nil
			}
			if event != nil {
				if err := e.writeToNode(ctx, event); err != nil {
					log.Printf("[OPCUA Error] %s (value=%v): %v", event.PropertyID, event.Value, err)
				}
			}
		}
	}
}

// Stop terminates the OPC UA exporter and closes the client connection.
func (e *Exporter) Stop() error {
	if e.client != nil {
		return e.client.Close(context.Background())
	}
	return nil
}

// writeToNode writes the given AAS update event to the corresponding OPC UA node.
func (e *Exporter) writeToNode(ctx context.Context, event *core.AASUpdateEvent) error {
	cacheKey := event.SubmodelID + "." + event.PropertyID

	// 1. Check Cache for DataType (Avoids Core Lock)
	dataType, ok := e.typeCache[cacheKey]
	if !ok {
		var err error
		dataType, err = e.core.GetElementDataType(event.SubmodelID, event.PropertyID)
		if err != nil {
			return fmt.Errorf("AAS definition not found: %v", err)
		}
		e.typeCache[cacheKey] = dataType
	}

	// 2. Convert Value
	variant, err := e.convertToVariant(event.Value, dataType)
	if err != nil {
		return err
	}

	// 3. Check Cache for NodeID
	nodeID, ok := e.nodeIDCache[cacheKey]
	if !ok {
		nodeID = e.generateNodeID(event.SubmodelID, event.PropertyID)
		e.nodeIDCache[cacheKey] = nodeID
	}

	// 4. Create Request
	req := &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{
			{
				NodeID:      nodeID,
				AttributeID: ua.AttributeIDValue,
				Value: &ua.DataValue{
					EncodingMask: ua.DataValueValue,
					Value:        variant,
				},
			},
		},
	}

	instrumentation.Record("OPCUA_SENDING", event.PropertyID, event.Timestamp)

	// 5. Send Request
	resp, err := e.client.Write(ctx, req)

	instrumentation.Record("OPCUA_SENT", event.PropertyID, event.Timestamp)

	if err != nil {
		return err
	}
	if len(resp.Results) > 0 && resp.Results[0] != ua.StatusOK {
		return fmt.Errorf("status: %v", resp.Results[0])
	}
	return nil
}

// generateNodeID creates an OPC UA NodeID for the given submodel and property path.
func (e *Exporter) generateNodeID(submodelID, propertyPath string) *ua.NodeID {
	// AAS path "GlobalPosition.Latitude" -> "GlobalPosition/Latitude"
	hierarchy := strings.ReplaceAll(propertyPath, ".", "/")
	// Full string "Navigation/GlobalPosition/Latitude"
	fullPath := fmt.Sprintf("%s/%s", submodelID, hierarchy)
	return ua.NewStringNodeID(e.namespace, fullPath)
}

// convertToVariant converts any value to the specific OPC UA Variant required by the AAS Type.
func (e *Exporter) convertToVariant(val any, aasType aastypes.DataTypeDefXSD) (*ua.Variant, error) {
	switch aasType {
	// --- Boolean ---
	case aastypes.DataTypeDefXSDBoolean:
		v, err := toBool(val)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(v), nil

	// --- Floating Point ---
	case aastypes.DataTypeDefXSDFloat:
		v, err := toFloat64(val)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(float32(v)), nil
	case aastypes.DataTypeDefXSDDouble:
		v, err := toFloat64(val)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(v), nil

	// --- Signed Integers ---
	case aastypes.DataTypeDefXSDByte: // SByte (int8)
		v, err := toInt64(val)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(int8(v)), nil
	case aastypes.DataTypeDefXSDShort: // Int16
		v, err := toInt64(val)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(int16(v)), nil
	case aastypes.DataTypeDefXSDInt, aastypes.DataTypeDefXSDInteger: // Int32
		v, err := toInt64(val)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(int32(v)), nil
	case aastypes.DataTypeDefXSDLong: // Int64
		v, err := toInt64(val)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(int64(v)), nil

	// --- Unsigned Integers ---
	case aastypes.DataTypeDefXSDUnsignedByte: // Byte (uint8)
		v, err := toUint64(val)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(byte(v)), nil
	case aastypes.DataTypeDefXSDUnsignedShort: // UInt16
		v, err := toUint64(val)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(uint16(v)), nil
	case aastypes.DataTypeDefXSDUnsignedInt: // UInt32
		v, err := toUint64(val)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(uint32(v)), nil
	case aastypes.DataTypeDefXSDUnsignedLong: // UInt64
		v, err := toUint64(val)
		if err != nil {
			return nil, err
		}
		return ua.MustVariant(uint64(v)), nil

	// --- Strings ---
	case aastypes.DataTypeDefXSDString:
		return ua.MustVariant(fmt.Sprintf("%v", val)), nil

	default:
		// Fallback for types we don't strictly enforce yet
		return ua.MustVariant(val), nil
	}
}

// --- Helpers to coerce any numeric type ---

func toInt64(val any) (int64, error) {
	switch v := val.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", val)
	}
}

func toUint64(val any) (uint64, error) {
	switch v := val.(type) {
	case int:
		return uint64(v), nil
	case int8:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case float32:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", val)
	}
}

func toFloat64(val any) (float64, error) {
	switch v := val.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int, int8, int16, int32, int64:
		i, _ := toInt64(v)
		return float64(i), nil
	case uint, uint8, uint16, uint32, uint64:
		u, _ := toUint64(v)
		return float64(u), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case bool:
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return 0.0, fmt.Errorf("cannot convert %T to float64", val)
	}
}

func toBool(val any) (bool, error) {
	switch v := val.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64:
		i, _ := toInt64(v)
		return i != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		u, _ := toUint64(v)
		return u != 0, nil
	case string:
		return strconv.ParseBool(v)
	default:
		return false, fmt.Errorf("cannot convert %T to bool", val)
	}
}
