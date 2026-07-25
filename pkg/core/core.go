package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"
	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/instrumentation"

	"github.com/aas-core-works/aas-core3.0-golang/jsonization"
	aastypes "github.com/aas-core-works/aas-core3.0-golang/types"
)

// AASCore is the central, stateful struct.
type AASCore struct {
	// The in-memory AAS model. Its structure (submodels, elements, nesting)
	// is fixed at load time and never mutated afterwards, so it can be
	// traversed concurrently without synchronization. Only the *values*
	// held by individual properties change at runtime; those are protected
	// by submodelLocks below.
	env aastypes.IEnvironment

	// propertyMap and submodelLocks are both built once in NewAASCore and
	// never modified afterwards, so concurrent unsynchronized reads of the
	// maps themselves are safe (Go only requires synchronization against
	// concurrent writes). submodelLocks has one lock per submodel, so an
	// update to one submodel never blocks reads/writes of another - unlike
	// a single model-wide lock, which serializes everything through one
	// mutex regardless of which property is involved.
	propertyMap   map[string]aastypes.IProperty
	submodelLocks map[string]*sync.RWMutex

	// maintainStringState controls whether property updates are written back
	// into the in-memory AAS model. Only pull-style consumers (e.g. the HTTP
	// exporter) read those values; in a push-only deployment it's left false
	// so the hot path skips the per-event string conversion and the submodel
	// write-lock entirely.
	maintainStringState bool

	subsMu      sync.Mutex
	subscribers []chan *AASUpdateEvent
}

// AASUpdateEvent is the "push" notification from the core.
// Optimization: Value is 'any' to allow zero-copy passing of floats/ints to exporters.
type AASUpdateEvent struct {
	SubmodelID string // e.g., "Navigation" (idShort)
	PropertyID string // e.g., "Position.Latitude" (idShort path)
	Value      any    // Changed from string to any
	Timestamp  time.Time
}

// NewAASCore creates a new AASCore instance. maintainStringState should be
// true when a pull-style exporter (see exporters.RequiresLiveModelState) is
// enabled; when false, the core skips writing string values back into the
// model on the hot path.
func NewAASCore(cfg config.AASConfig, maintainStringState bool) (*AASCore, error) {
	slog.Info("Loading AAS model", "file", cfg.FilePath)

	file, err := os.Open(cfg.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open AAS model file '%s': %w", cfg.FilePath, err)
	}
	defer file.Close()

	var rawData map[string]any
	if err := json.NewDecoder(file).Decode(&rawData); err != nil {
		return nil, fmt.Errorf("failed to decode AAS model JSON: %w", err)
	}

	var env aastypes.IEnvironment
	env, err = jsonization.EnvironmentFromJsonable(rawData)
	if err != nil {
		return nil, fmt.Errorf("failed to de-serialize AAS model: %w", err)
	}

	slog.Info("AAS model loaded", "submodels", len(env.Submodels()))
	return &AASCore{
		env:                 env,
		subscribers:         make([]chan *AASUpdateEvent, 0),
		propertyMap:         buildPropertyMap(env),
		submodelLocks:       buildSubmodelLocks(env),
		maintainStringState: maintainStringState,
	}, nil
}

// GetAllSubmodels retrieves all submodels in the environment
func (c *AASCore) GetAllSubmodels() []aastypes.ISubmodel {
	if c.env.Submodels() == nil {
		return []aastypes.ISubmodel{}
	}
	return c.env.Submodels()
}

// GetSubmodel retrieves a submodel by its idShort.
func (c *AASCore) GetSubmodel(idShort string) (aastypes.ISubmodel, error) {
	sm := findSubmodel(c.env, idShort)
	if sm == nil {
		return nil, fmt.Errorf("not found")
	}
	return sm, nil
}

// GetSubmodelByID retrieves a submodel by its global ID (e.g., urn:...)
func (c *AASCore) GetSubmodelByID(id string) (aastypes.ISubmodel, error) {
	sm := findSubmodelByID(c.env, id)
	if sm == nil {
		return nil, fmt.Errorf("submodel with id '%s' not found", id)
	}
	return sm, nil
}

// GetElementDataType looks up the data type defined in the AAS JSON for a
// specific path. ValueType is fixed at load time, so no locking is needed.
func (c *AASCore) GetElementDataType(submodelIdShort, propertyIdPath string) (aastypes.DataTypeDefXSD, error) {
	prop, exists := c.propertyMap[submodelIdShort+"."+propertyIdPath]
	if !exists {
		return 0, fmt.Errorf("property path '%s' not found in submodel '%s'", propertyIdPath, submodelIdShort)
	}
	return prop.ValueType(), nil
}

// GetPropertyValue safely reads a property's current raw string value,
// synchronized against concurrent updates to the same submodel.
func (c *AASCore) GetPropertyValue(submodelIdShort, propertyIdPath string) (*string, error) {
	prop, exists := c.propertyMap[submodelIdShort+"."+propertyIdPath]
	if !exists {
		return nil, fmt.Errorf("property path '%s' not found in submodel '%s'", propertyIdPath, submodelIdShort)
	}

	lock := c.submodelLock(submodelIdShort)
	lock.RLock()
	defer lock.RUnlock()
	return prop.Value(), nil
}

// SerializeGivenSubmodel returns the AAS-compliant JSON-able representation
// of an already-resolved submodel (e.g. from GetSubmodel/GetSubmodelByID),
// synchronized against concurrent property updates within it.
func (c *AASCore) SerializeGivenSubmodel(sm aastypes.ISubmodel) (any, error) {
	return c.serializeLocked(idShortOf(sm), sm)
}

// SerializeAllSubmodels returns the JSON-able representation of every
// submodel. Each is serialized under its own submodel lock rather than one
// lock for the whole call, so a busy submodel can't stall the encoding of
// the others. Submodels that fail to serialize are logged and skipped.
func (c *AASCore) SerializeAllSubmodels() []any {
	submodels := c.GetAllSubmodels()
	result := make([]any, 0, len(submodels))

	for _, sm := range submodels {
		jsonable, err := c.serializeLocked(idShortOf(sm), sm)
		if err != nil {
			slog.Error("Failed to serialize submodel", "submodel", idShortOf(sm), "error", err)
			continue
		}
		result = append(result, jsonable)
	}
	return result
}

// serializeLocked holds the submodel's lock for the full duration of
// encoding, so the JSON snapshot can't observe a value mid-update.
func (c *AASCore) serializeLocked(idShort string, sm aastypes.ISubmodel) (any, error) {
	lock := c.submodelLock(idShort)
	lock.RLock()
	defer lock.RUnlock()
	return jsonization.ToJsonable(sm)
}

// submodelLock returns the lock guarding property values for the given
// submodel. Every submodel present at load time has one (see
// buildSubmodelLocks); the fresh fallback only guards against a caller
// passing an idShort that doesn't exist in the model.
func (c *AASCore) submodelLock(idShort string) *sync.RWMutex {
	if lock, ok := c.submodelLocks[idShort]; ok {
		return lock
	}
	return &sync.RWMutex{}
}

// idShortOf safely extracts a submodel's idShort, defaulting to "" if unset.
func idShortOf(sm aastypes.ISubmodel) string {
	if sm.IDShort() == nil {
		return ""
	}
	return *sm.IDShort()
}

// Start begins processing incoming AAS update events.
func (c *AASCore) Start(ctx context.Context, inChan <-chan *AASUpdateEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-inChan:
			if !ok {
				slog.Info("AASCore input channel closed, stopping")
				return
			}
			if msg == nil {
				continue
			}
			c.updateModel(msg.SubmodelID, msg.PropertyID, msg.Value, msg.Timestamp)
		}
	}
}

// Subscribe creates a new subscription channel for AAS update events.
func (c *AASCore) Subscribe(ctx context.Context) (<-chan *AASUpdateEvent, error) {
	subChan := make(chan *AASUpdateEvent, 1000)

	c.subsMu.Lock()
	c.subscribers = append(c.subscribers, subChan)
	c.subsMu.Unlock()

	go func() {
		<-ctx.Done()
		c.subsMu.Lock()
		defer c.subsMu.Unlock()

		for i, ch := range c.subscribers {
			if ch == subChan {
				c.subscribers = append(c.subscribers[:i], c.subscribers[i+1:]...)
				close(subChan)
				break
			}
		}
	}()

	return subChan, nil
}

// updateModel updates the model with new data and notifies subscribers.
func (c *AASCore) updateModel(submodelIdShort, propertyIdPath string, value any, timestamp time.Time) {
	prop, exists := c.propertyMap[submodelIdShort+"."+propertyIdPath]
	if !exists {
		return // Property not found in model
	}

	instrumentation.Record("CORE", propertyIdPath, timestamp)

	// In a push-only deployment nothing reads the model's string values, so
	// skip the per-event string conversion and the submodel write-lock;
	// exporters use the raw event value from the broadcast below instead.
	// Only the submodel this property belongs to is locked, so writes to
	// other submodels - and reads of them, e.g. concurrent HTTP pulls -
	// proceed unimpeded instead of queuing behind a single model-wide lock.
	if c.maintainStringState {
		strValue := fastAnyToString(value)
		lock := c.submodelLock(submodelIdShort)
		lock.Lock()
		prop.SetValue(&strValue)
		lock.Unlock()
	}

	c.broadcast(&AASUpdateEvent{
		SubmodelID: submodelIdShort,
		PropertyID: propertyIdPath,
		Value:      value,
		Timestamp:  timestamp,
	})
}

// broadcast fans updateEvent out to all subscribers. Sends are
// non-blocking: a slow/stuck exporter never stalls ingestion or other
// exporters, at the cost of silently dropping updates for that subscriber.
func (c *AASCore) broadcast(updateEvent *AASUpdateEvent) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()

	for _, subChan := range c.subscribers {
		select {
		case subChan <- updateEvent:
		default:
			// Allows fast exporters to proceed even if one exporter is clogged
		}
	}
}

// fastAnyToString provides zero-reflection, low-allocation string conversion
// for the high-frequency ingestion path.
func fastAnyToString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		// 'f' formats without exponents, -1 yields the minimum necessary digits
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case int:
		return strconv.Itoa(v)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case bool:
		return strconv.FormatBool(v)
	default:
		// Fallback for unexpected types (incurs standard reflection overhead)
		return fmt.Sprintf("%v", v)
	}
}
