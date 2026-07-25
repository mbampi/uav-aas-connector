package opcua_client

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

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

	// Resiliency settings
	enableDedup bool
	maxAge      time.Duration

	// Decoupling components
	pendingUpdates map[string]*core.AASUpdateEvent
	pendingMu      sync.Mutex
	triggerChan    chan struct{} // Used to wake up the network worker instantly
}

// NewExporter creates a new OPC UA exporter instance.
func NewExporter(cfg config.ExporterConfig, aasCore *core.AASCore) (*Exporter, error) {
	host := "localhost"
	if cfg.Host != "" {
		host = cfg.Host
	}
	endpoint := fmt.Sprintf("opc.tcp://%s:%d", host, cfg.Port)

	slog.Info("OPCUA initializing exporter", "endpoint", endpoint, "tls", cfg.Security.TLSEnabled)

	opts, err := clientSecurityOptions(cfg.Security)
	if err != nil {
		return nil, fmt.Errorf("invalid OPC UA client security config: %w", err)
	}

	c, err := opcua.NewClient(endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create OPC UA client: %w", err)
	}

	ns := uint16(2)

	maxAge := time.Duration(cfg.MaxAgeSeconds * float64(time.Second))

	return &Exporter{
		client:         c,
		core:           aasCore,
		endpoint:       endpoint,
		namespace:      ns,
		typeCache:      make(map[string]aastypes.DataTypeDefXSD),
		nodeIDCache:    make(map[string]*ua.NodeID),
		enableDedup:    cfg.EnableDeduplication,
		maxAge:         maxAge,
		pendingUpdates: make(map[string]*core.AASUpdateEvent),
		triggerChan:    make(chan struct{}, 1), // Buffer of 1 ensures non-blocking triggers
	}, nil
}

// Start begins the lightning-fast ingestion loop and spawns the network worker.
func (e *Exporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	slog.Info("OPCUA connecting", "endpoint", e.endpoint, "ns", e.namespace)

	if err := e.reconnect(ctx); err != nil {
		return nil
	}
	defer e.client.Close(ctx)

	// Spawn the decoupled network worker
	go e.networkWorker(ctx)

	// Main Ingestion Loop (Never Blocks)
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-inChan:
			if !ok {
				return nil
			}
			if event == nil {
				continue
			}

			// 1. Thread-safely update the pending state
			e.pendingMu.Lock()
			cacheKey := event.SubmodelID + "." + event.PropertyID
			e.pendingUpdates[cacheKey] = event
			e.pendingMu.Unlock()

			// 2. Wake up the network worker instantly (non-blocking)
			select {
			case e.triggerChan <- struct{}{}:
			default:
				// Worker is already awake and processing, no need to block
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

// networkWorker handles OPC UA writes. It protects the ingestion loop from network latency.
func (e *Exporter) networkWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.triggerChan: // Wakes up instantly when new data arrives
			// 1. Extract and clear the pending state as fast as possible
			e.pendingMu.Lock()

			// If map is empty, skip
			if len(e.pendingUpdates) == 0 {
				e.pendingMu.Unlock()
				continue
			}

			// Pre-allocate slice for speed
			batch := make([]*core.AASUpdateEvent, 0, len(e.pendingUpdates))
			for _, event := range e.pendingUpdates {
				batch = append(batch, event)
			}

			// Clear the map for the next cycle
			for k := range e.pendingUpdates {
				delete(e.pendingUpdates, k)
			}
			e.pendingMu.Unlock()

			// 2. Apply TTL (Deduplication is already handled implicitly by the map!)
			filteredBatch := e.filterBatch(batch)

			// 3. Send over the network
			if len(filteredBatch) > 0 {
				if err := e.writeBatch(ctx, filteredBatch); err != nil {
					slog.Error("OPCUA batch failed, triggering session recovery", "error", err)

					if reconnErr := e.reconnect(ctx); reconnErr != nil {
						return
					}

					// Retry after reconnect
					if err := e.writeBatch(ctx, filteredBatch); err != nil {
						slog.Error("OPCUA batch retry failed, dropping batch", "error", err)
					}
				}
			}
		}
	}
}

// prepareWriteValue creates the OPC UA write structure but does not send it.
func (e *Exporter) prepareWriteValue(event *core.AASUpdateEvent) (*ua.WriteValue, error) {
	cacheKey := event.SubmodelID + "." + event.PropertyID

	// 1. Check Cache for DataType
	dataType, ok := e.typeCache[cacheKey]
	if !ok {
		var err error
		dataType, err = e.core.GetElementDataType(event.SubmodelID, event.PropertyID)
		if err != nil {
			return nil, fmt.Errorf("AAS definition not found: %v", err)
		}
		e.typeCache[cacheKey] = dataType
	}

	// 2. Convert Value
	variant, err := e.convertToVariant(event.Value, dataType)
	if err != nil {
		return nil, err
	}

	// 3. Check Cache for NodeID
	nodeID, ok := e.nodeIDCache[cacheKey]
	if !ok {
		nodeID = e.generateNodeID(event.SubmodelID, event.PropertyID)
		e.nodeIDCache[cacheKey] = nodeID
	}

	// 4. Return the structure instead of sending
	return &ua.WriteValue{
		NodeID:      nodeID,
		AttributeID: ua.AttributeIDValue,
		Value: &ua.DataValue{
			EncodingMask: ua.DataValueValue,
			Value:        variant,
		},
	}, nil
}

// writeBatch sends multiple events in a single OPC UA network packet.
func (e *Exporter) writeBatch(ctx context.Context, events []*core.AASUpdateEvent) error {
	nodesToWrite := make([]*ua.WriteValue, 0, len(events))

	for _, event := range events {
		wv, err := e.prepareWriteValue(event)
		if err != nil {
			slog.Warn("OPCUA skipping property due to error", "property", event.PropertyID, "error", err)
			continue
		}
		nodesToWrite = append(nodesToWrite, wv)
	}

	if len(nodesToWrite) == 0 {
		return nil
	}

	req := &ua.WriteRequest{
		NodesToWrite: nodesToWrite,
	}
	for _, event := range events {
		instrumentation.Record("OPCUA_SENDING", event.PropertyID, event.Timestamp)
	}
	resp, err := e.client.Write(ctx, req)
	for _, event := range events {
		instrumentation.Record("OPCUA_SENT", event.PropertyID, event.Timestamp)
	}
	if err != nil {
		return err
	}

	if len(resp.Results) > 0 {
		for _, res := range resp.Results {
			if res != ua.StatusOK {
				return fmt.Errorf("batch contained errors, first status: %v", res)
			}
		}
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

// filterBatch applies TTL and Deduplication to a batch of events before transmission.
func (e *Exporter) filterBatch(batch []*core.AASUpdateEvent) []*core.AASUpdateEvent {
	// Fast path: return immediately if both features are disabled
	if !e.enableDedup && e.maxAge <= 0 {
		return batch
	}

	now := time.Now()
	var finalBatch []*core.AASUpdateEvent

	if e.enableDedup {
		// Deduplication enabled: Use a map to retain only the latest event per property
		latestUpdates := make(map[string]*core.AASUpdateEvent)

		for _, event := range batch {
			// TTL Check: Drop stale messages
			if e.maxAge > 0 && now.Sub(event.Timestamp) > e.maxAge {
				continue
			}

			// Map key guarantees older events for the same property are overwritten
			cacheKey := event.SubmodelID + "." + event.PropertyID
			latestUpdates[cacheKey] = event
		}

		// Re-flatten the map into a slice
		finalBatch = make([]*core.AASUpdateEvent, 0, len(latestUpdates))
		for _, event := range latestUpdates {
			finalBatch = append(finalBatch, event)
		}
	} else {
		// Deduplication disabled, only process TTL
		finalBatch = make([]*core.AASUpdateEvent, 0, len(batch))
		for _, event := range batch {
			if e.maxAge > 0 && now.Sub(event.Timestamp) > e.maxAge {
				continue
			}
			finalBatch = append(finalBatch, event)
		}
	}

	return finalBatch
}

// clientSecurityOptions translates the shared security config into gopcua
// client options. TLSEnabled switches the secure channel from None to
// Sign+Encrypt with Basic256Sha256, using the endpoint's own certificate;
// Username/Password select username authentication over anonymous.
func clientSecurityOptions(cfg config.SecurityConfig) ([]opcua.Option, error) {
	var opts []opcua.Option

	if cfg.TLSEnabled {
		if cfg.CertFile == "" || cfg.KeyFile == "" {
			return nil, fmt.Errorf("certFile and keyFile are required when tlsEnabled is true")
		}
		opts = append(opts,
			opcua.CertificateFile(cfg.CertFile),
			opcua.PrivateKeyFile(cfg.KeyFile),
			opcua.SecurityMode(ua.MessageSecurityModeSignAndEncrypt),
			opcua.SecurityPolicy(ua.SecurityPolicyURIBasic256Sha256),
		)
	} else {
		opts = append(opts, opcua.SecurityMode(ua.MessageSecurityModeNone))
	}

	if cfg.Username != "" {
		opts = append(opts, opcua.AuthUsername(cfg.Username, cfg.Password))
	} else {
		opts = append(opts, opcua.AuthAnonymous())
	}

	return opts, nil
}

// reconnect cleanly closes broken sessions, flushes server caches, and attempts
// to re-establish the OPC UA connection with exponential backoff.
func (e *Exporter) reconnect(ctx context.Context) error {
	slog.Warn("OPCUA session interrupted, initiating reconnection loop", "endpoint", e.endpoint)

	// 1. Gracefully close dead transport session if open
	if e.client != nil {
		_ = e.client.Close(ctx)
	}

	// 2. Flush cached Node IDs (server restart may invalidate session node references)
	e.nodeIDCache = make(map[string]*ua.NodeID)

	// 3. Reconnection loop with capped exponential backoff
	backoff := 1 * time.Second
	const maxBackoff = 10 * time.Second

	for {
		if err := e.client.Connect(ctx); err == nil {
			slog.Info("OPCUA connection successfully restored", "endpoint", e.endpoint)
			return nil
		} else {
			slog.Error("OPCUA reconnection attempt failed", "error", err, "retry_in", backoff)
		}

		select {
		case <-ctx.Done():
			return ctx.Err() // Graceful exit on application shutdown
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}
