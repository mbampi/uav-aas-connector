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
		client:    c,
		core:      core,
		endpoint:  endpoint,
		namespace: ns,
		typeCache: make(map[string]aastypes.DataTypeDefXSD),
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
					log.Printf("[OPCUA Error] %s: %v", event.PropertyID, err)
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

	instrumentation.Record("EXPORTER", event.PropertyID, event.Timestamp)

	// 5. Send Request
	resp, err := e.client.Write(ctx, req)

	instrumentation.Record("WROTE", event.PropertyID, event.Timestamp)

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

// convertToVariant converts a value to an OPC UA Variant based on the specified AAS data type.
func (e *Exporter) convertToVariant(val any, aasType aastypes.DataTypeDefXSD) (*ua.Variant, error) {
	if f, ok := val.(float64); ok {
		switch aasType {
		case aastypes.DataTypeDefXSDDouble:
			return ua.MustVariant(f), nil
		case aastypes.DataTypeDefXSDFloat:
			return ua.MustVariant(float32(f)), nil
		case aastypes.DataTypeDefXSDInt, aastypes.DataTypeDefXSDInteger:
			return ua.MustVariant(int32(f)), nil
		case aastypes.DataTypeDefXSDShort:
			return ua.MustVariant(int16(f)), nil
		case aastypes.DataTypeDefXSDBoolean:
			return ua.MustVariant(f != 0), nil
		}
	}

	// Fallback string parsing
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
	case aastypes.DataTypeDefXSDInt, aastypes.DataTypeDefXSDInteger:
		v, err := strconv.ParseInt(valStr, 10, 32)
		if err != nil {
			if f, err2 := strconv.ParseFloat(valStr, 64); err2 == nil {
				return ua.MustVariant(int32(f)), nil
			}
			return nil, err
		}
		return ua.MustVariant(int32(v)), nil
	case aastypes.DataTypeDefXSDString:
		return ua.MustVariant(valStr), nil
	default:
		return ua.MustVariant(valStr), nil
	}
}
