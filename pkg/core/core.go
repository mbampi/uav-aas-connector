package core

import (
	"context"
	"fmt"
	"sync"
	"time"
	"uav-aas-connector/pkg/aas"
	"uav-aas-connector/pkg/config"
)

// AASCore is the central, stateful struct. It implements AASReader.
type AASCore struct {
	// The in-memory, thread-safe model of the AAS
	model      *aas.AssetAdministrationShell
	modelMutex sync.RWMutex

	mapping     *config.MappingConfig
	subscribers []chan *AASUpdateEvent
}

// NewAASCore creates a new AASCore instance with the provided mapping configuration.
func NewAASCore(mapping *config.MappingConfig) *AASCore {
	return &AASCore{
		model:       &aas.AssetAdministrationShell{},
		mapping:     mapping,
		subscribers: make([]chan *AASUpdateEvent, 0),
	}
}

// AASUpdateEvent is the "push" notification from the core.
// This is the new "processed data" message.
type AASUpdateEvent struct {
	SubmodelID string // e.g., "urn:submodel:navigation:001"
	PropertyID string // e.g., "Latitude"
	Value      any
	Timestamp  time.Time
}

// AASReader is the "pull" interface that the Core provides to Exporters.
// This allows exporters to get the full, current state.
type AASReader interface {
	// GetAASSnapshot returns a deep copy of the entire AAS model.
	GetAASSnapshot() (*aas.AssetAdministrationShell, error)

	// GetSubmodel returns a deep copy of a specific Submodel.
	GetSubmodel(id string) (*aas.Submodel, error)

	// GetPropertyValue returns a single value.
	GetPropertyValue(submodelId, propertyId string) (any, error)

	// Subscribe returns a channel that broadcasts AASUpdateEvents.
	// Each exporter gets its own channel.
	Subscribe(ctx context.Context) (<-chan *AASUpdateEvent, error)
}

// Start method for the Core (this is NOT part of an interface)
// It listens to the raw data channel and updates the model.
func (c *AASCore) Start(ctx context.Context, inChan <-chan *RawDataPoint) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-inChan:
			fmt.Printf("%s", msg.MessageName)
			// 1. Look up msg.MessageName in c.mapping
			// 2. Find the target AASPath (e.g., "$.submodels[...].Latitude")
			// 3. Apply transformations (scale, offset)
			// 4. Find the corresponding field in c.model (e.g., c.model.Submodels[...].Latitude)
			// 5. Update the value (using a mutex for safety)
			// 6. Broadcast the AASUpdateEvent to all subscribers
		}
	}
}

func (c *AASCore) Subscribe(ctx context.Context) (<-chan *AASUpdateEvent, error) {
	subChan := make(chan *AASUpdateEvent, 100) // buffered channel
	c.subscribers = append(c.subscribers, subChan)

	// Handle context cancellation to clean up the subscriber
	go func() {
		<-ctx.Done()
		// Remove subChan from c.subscribers
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

// In AASCore, when a new RawDataPoint arrives:
func (c *AASCore) updateModel(msg *RawDataPoint) {
	// ... find the mapping rule for msg.MessageName ...
	// ... calculate the newValue ...

	// Lock the model for writing
	c.modelMutex.Lock()
	defer c.modelMutex.Unlock()

	// Find and update the value in c.model
	// e.g., property := findProperty(c.model, "Navigation", "Latitude")
	// property.Value = newValue

	updateEvent := &AASUpdateEvent{}

	// broadcast the AASUpdateEvent to all subscribers
	for _, subChan := range c.subscribers {
		select {
		case subChan <- updateEvent:
		default:
			// If the subscriber's channel is full, skip sending to avoid blocking
		}
	}
}

// In AASCore, implementing the AASReader interface:
func (c *AASCore) GetSubmodel(idShort string) (*aas.Submodel, error) {
	// Lock the model for reading
	c.modelMutex.RLock()
	defer c.modelMutex.RUnlock()

	// Find the Submodel by idShort
	var foundSubmodel *aas.Submodel
	for _, submodel := range c.model.Submodels {
		if submodel.IDShort == idShort {
			foundSubmodel = submodel
			break
		}
	}
	if foundSubmodel == nil {
		return nil, fmt.Errorf("submodel with idShort %s not found", idShort)
	}

	return foundSubmodel.DeepCopy(), nil
}
