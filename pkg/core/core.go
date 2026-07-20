package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/instrumentation"

	"github.com/aas-core-works/aas-core3.0-golang/jsonization"
	aastypes "github.com/aas-core-works/aas-core3.0-golang/types"
)

// AASCore is the central, stateful struct.
type AASCore struct {
	// The in-memory, thread-safe model of the AAS
	env         aastypes.IEnvironment
	modelMutex  sync.RWMutex
	subscribers []chan *AASUpdateEvent

	// flat routing map for O(1) property lookups
	propertyMap map[string]aastypes.IProperty
}

// AASUpdateEvent is the "push" notification from the core.
// Optimization: Value is 'any' to allow zero-copy passing of floats/ints to exporters.
type AASUpdateEvent struct {
	SubmodelID string // e.g., "Navigation" (idShort)
	PropertyID string // e.g., "Position.Latitude" (idShort path)
	Value      any    // Changed from string to any
	Timestamp  time.Time
}

// NewAASCore creates a new AASCore instance.
func NewAASCore(cfg config.AASConfig) (*AASCore, error) {
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
		env:         env,
		subscribers: make([]chan *AASUpdateEvent, 0),
		propertyMap: buildPropertyMap(env),
	}, nil
}

// GetAllSubmodels retrieves all submodels in the environment
func (c *AASCore) GetAllSubmodels() []aastypes.ISubmodel {
	c.modelMutex.RLock()
	defer c.modelMutex.RUnlock()

	if c.env.Submodels() == nil {
		return []aastypes.ISubmodel{}
	}
	return c.env.Submodels()
}

// GetSubmodel retrieves a submodel by its idShort
func (c *AASCore) GetSubmodel(idShort string) (aastypes.ISubmodel, error) {
	c.modelMutex.RLock()
	defer c.modelMutex.RUnlock()
	sm := findSubmodel(c.env, idShort)
	if sm == nil {
		return nil, fmt.Errorf("not found")
	}
	return sm, nil
}

// GetSubmodelByID retrieves a submodel by its global ID (e.g., urn:...)
func (c *AASCore) GetSubmodelByID(id string) (aastypes.ISubmodel, error) {
	c.modelMutex.RLock()
	defer c.modelMutex.RUnlock()
	sm := findSubmodelByID(c.env, id)
	if sm == nil {
		return nil, fmt.Errorf("submodel with id '%s' not found", id)
	}
	return sm, nil
}

// GetElementDataType looks up the data type defined in the AAS JSON for a specific path.
func (c *AASCore) GetElementDataType(submodelIdShort, propertyIdPath string) (aastypes.DataTypeDefXSD, error) {
	cacheKey := submodelIdShort + "." + propertyIdPath

	c.modelMutex.RLock()
	prop, exists := c.propertyMap[cacheKey]
	c.modelMutex.RUnlock()

	if !exists {
		return 0, fmt.Errorf("property path '%s' not found in submodel '%s'", propertyIdPath, submodelIdShort)
	}

	return prop.ValueType(), nil
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
	c.modelMutex.Lock()
	defer c.modelMutex.Unlock()

	subChan := make(chan *AASUpdateEvent, 1000)
	c.subscribers = append(c.subscribers, subChan)

	go func() {
		<-ctx.Done()
		c.modelMutex.Lock()
		defer c.modelMutex.Unlock()

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
	cacheKey := submodelIdShort + "." + propertyIdPath

	// 1. Fast O(1) Lookup (Read Lock)
	c.modelMutex.RLock()
	prop, exists := c.propertyMap[cacheKey]
	c.modelMutex.RUnlock()

	if !exists {
		return // Property not found in model
	}

	// 2. Format string outside of the lock to prevent blocking concurrent updates
	strValue := fmt.Sprintf("%v", value)

	// 3. Narrow Write Lock (Only for setting the state)
	c.modelMutex.Lock()
	instrumentation.Record("CORE", propertyIdPath, timestamp)
	prop.SetValue(&strValue)
	c.modelMutex.Unlock()

	// 4. Prepare Event
	updateEvent := &AASUpdateEvent{
		SubmodelID: submodelIdShort,
		PropertyID: propertyIdPath,
		Value:      value,
		Timestamp:  timestamp,
	}

	// 5. Broadcast (Read Lock only protects the subscribers slice)
	c.modelMutex.RLock()
	defer c.modelMutex.RUnlock()
	for _, subChan := range c.subscribers {
		select {
		case subChan <- updateEvent:
		default:
			// Allows fast exporters to proceed even if one exporter is clogged
		}
	}
}
