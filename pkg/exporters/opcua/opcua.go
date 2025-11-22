package opcua

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	"uav-aas-connector/pkg/instrumentation"

	aastypes "github.com/aas-core-works/aas-core3.0-golang/types"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

type Exporter struct {
	client   *opcua.Client
	core     *core.AASCore
	endpoint string

	namespace uint16
	typeCache map[string]aastypes.DataTypeDefXSD
}

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

func (e *Exporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	log.Printf("[OPCUA] Connecting to %s (NS=%d)...", e.endpoint, e.namespace)
	if err := e.client.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer e.client.Close(ctx)
	log.Println("[OPCUA] Connected.")

	// buffer for batching
	const batchSize = 20
	const flushInterval = 10 * time.Millisecond

	batch := make([]*core.AASUpdateEvent, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) > 0 {
			if err := e.writeBatch(ctx, batch); err != nil {
				log.Printf("[OPCUA Batch Error] %v", err)
			}
			// Clear batch
			batch = batch[:0]
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			flush()
		case event, ok := <-inChan:
			if !ok {
				return nil
			}
			if event != nil {
				batch = append(batch, event)
				if len(batch) >= batchSize {
					flush()
				}
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

func (e *Exporter) writeBatch(ctx context.Context, events []*core.AASUpdateEvent) error {
	req := &ua.WriteRequest{
		NodesToWrite: make([]*ua.WriteValue, len(events)),
	}

	for i, event := range events {
		cacheKey := event.SubmodelID + "." + event.PropertyID
		dataType, ok := e.typeCache[cacheKey]
		if !ok {
			var err error
			dataType, err = e.core.GetElementDataType(event.SubmodelID, event.PropertyID)
			if err != nil {
				return fmt.Errorf("AAS definition not found: %v", err)
			}
			e.typeCache[cacheKey] = dataType
		}

		variant, err := e.convertToVariant(event.Value, dataType)
		if err != nil {
			continue
		}

		req.NodesToWrite[i] = &ua.WriteValue{
			NodeID:      e.generateNodeID(event.SubmodelID, event.PropertyID),
			AttributeID: ua.AttributeIDValue,
			Value: &ua.DataValue{
				EncodingMask: ua.DataValueValue,
				Value:        variant,
			},
		}
	}

	log.Printf("[OPCUA] Writing batch of %d events", len(events))

	resp, err := e.client.Write(ctx, req)
	if err != nil {
		return err
	}

	// T1: Record the egress for all events in the batch
	for _, event := range events {
		instrumentation.Record("EXPORTER", event.PropertyID, event.Timestamp)
	}

	for i, status := range resp.Results {
		if status != ua.StatusOK {
			log.Printf("[OPCUA Write Error] Node %s: %v", req.NodesToWrite[i].NodeID.String(), status)
		}
	}

	return nil
}

func (e *Exporter) generateNodeID(submodelID, propertyPath string) *ua.NodeID {
	// AAS path "GlobalPosition.Latitude" -> "GlobalPosition/Latitude"
	hierarchy := strings.ReplaceAll(propertyPath, ".", "/")
	// Full string "Navigation/GlobalPosition/Latitude"
	fullPath := fmt.Sprintf("%s/%s", submodelID, hierarchy)
	return ua.NewStringNodeID(e.namespace, fullPath)
}

func (e *Exporter) convertToVariant(val any, aasType aastypes.DataTypeDefXSD) (*ua.Variant, error) {
	// Fast path for MAVLink floats
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
