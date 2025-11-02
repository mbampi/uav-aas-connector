package core

import (
	"context"
	"encoding/json" // Added import
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
	"uav-aas-connector/pkg/config"

	"github.com/aas-core-works/aas-core3.0-golang/jsonization"
	aastypes "github.com/aas-core-works/aas-core3.0-golang/types"
)

// AASCore is the central, stateful struct.
type AASCore struct {
	// The in-memory, thread-safe model of the AAS
	env         aastypes.IEnvironment // This holds the interface value directly
	modelMutex  sync.RWMutex
	subscribers []chan *AASUpdateEvent
}

// AASUpdateEvent is the "push" notification from the core.
type AASUpdateEvent struct {
	SubmodelID string // e.g., "Navigation" (idShort)
	PropertyID string // e.g., "Position.Latitude" (idShort path)
	Value      string
	Timestamp  time.Time
}

// NewAASCore creates a new AASCore instance.
// REFACTORED: Now returns an error instead of using log.Fatalf.
func NewAASCore(cfg config.AASConfig) (*AASCore, error) {
	log.Printf("Loading AAS model from %s", cfg.FilePath)

	// REFACTORED: Open the file
	file, err := os.Open(cfg.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open AAS model file '%s': %w", cfg.FilePath, err)
	}
	defer file.Close()

	// REFACTORED: Decode directly into a map[string]interface{}
	var rawData map[string]any
	if err := json.NewDecoder(file).Decode(&rawData); err != nil {
		return nil, fmt.Errorf("failed to decode AAS model JSON: %w", err)
	}

	var env aastypes.IEnvironment
	env, err = jsonization.EnvironmentFromJsonable(rawData)
	if err != nil {
		return nil, fmt.Errorf("failed to de-serialize AAS model: %w", err)
	}

	log.Printf("AAS model loaded. Found %d Submodels.", len(env.Submodels()))

	// REFACTORED: Return the struct and nil error
	return &AASCore{
		env:         env, // Assign the interface value
		subscribers: make([]chan *AASUpdateEvent, 0),
	}, nil
}

// Start method for the Core (this is NOT part of an interface)
// It listens to the raw data channel and updates the model.
func (c *AASCore) Start(ctx context.Context, inChan <-chan *AASUpdateEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-inChan:
			// This log is very noisy, consider making it debug level
			// log.Printf("Core received AAS update event: %+v", msg)
			c.updateModel(msg.SubmodelID, msg.PropertyID, msg.Value, msg.Timestamp)
		}
	}
}

// Subscribe provides a channel for an exporter to receive updates.
// FIXED: Added mutex locks to prevent race conditions on c.subscribers
func (c *AASCore) Subscribe(ctx context.Context) (<-chan *AASUpdateEvent, error) {
	c.modelMutex.Lock() // Full lock to safely modify the subscribers list
	defer c.modelMutex.Unlock()

	subChan := make(chan *AASUpdateEvent, 100) // buffered channel
	c.subscribers = append(c.subscribers, subChan)

	// Handle context cancellation to clean up the subscriber
	go func() {
		<-ctx.Done()
		c.modelMutex.Lock() // Lock again to safely remove the channel
		defer c.modelMutex.Unlock()

		for i, ch := range c.subscribers {
			if ch == subChan {
				// Remove the channel from the slice
				c.subscribers = append(c.subscribers[:i], c.subscribers[i+1:]...)
				close(subChan)
				break
			}
		}
	}()

	return subChan, nil
}

// updateModel finds the specific property in the model and updates its value.
// IMPLEMENTED: This logic is no longer a stub.
func (c *AASCore) updateModel(submodelIdShort, propertyIdPath string, value string, timestamp time.Time) {
	c.modelMutex.Lock() // Use a full write-lock for updates
	defer c.modelMutex.Unlock()

	// 1. Find the Submodel
	sm := findSubmodel(c.env, submodelIdShort)
	if sm == nil {
		log.Printf("Warning: Submodel with idShort '%s' not found in model.", submodelIdShort)
		return
	}

	// 2. Find the SubmodelElement (Property) recursively
	// We split by '.' to allow for nested properties like "Position.Latitude"
	path := strings.Split(propertyIdPath, ".")
	prop, err := findProperty(sm.SubmodelElements(), path)
	if err != nil {
		log.Printf("Warning: Failed to find/update property for path '%s' in Submodel '%s': %v", propertyIdPath, submodelIdShort, err)
		return
	}

	// 3. Update the value.
	// We must type-assert to the concrete struct to modify its field.
	if p, ok := prop.(*aastypes.Property); ok {
		p.SetValue(&value)
	} else {
		log.Printf("Warning: Found element for path '%s', but it was not a *aastypes.Property", propertyIdPath)
		return
	}

	// 4. Create and broadcast the update event
	updateEvent := &AASUpdateEvent{
		SubmodelID: submodelIdShort,
		PropertyID: propertyIdPath,
		Value:      value,
		Timestamp:  timestamp,
	}

	// log.Printf("Broadcasting update to %d subscribers: %+v", len(c.subscribers), updateEvent)
	for _, subChan := range c.subscribers {
		select {
		case subChan <- updateEvent:
		default:
			// Non-blocking send. If a subscriber is full, drop the message.
			log.Printf("Warning: Subscriber channel full. Discarding update for: %s", updateEvent.PropertyID)
		}
	}
}

// findSubmodel finds a submodel in the environment by its idShort.
func findSubmodel(env aastypes.IEnvironment, idShort string) aastypes.ISubmodel {
	if env.Submodels() == nil {
		return nil
	}
	for _, sm := range env.Submodels() {
		if sm.IDShort() != nil && *sm.IDShort() == idShort {
			return sm
		}
	}
	return nil
}

// findProperty recursively searches for a property using a path of idShorts.
// Returns the concrete *aastypes.Property type so its value can be modified.
func findProperty(elements []aastypes.ISubmodelElement, path []string) (aastypes.IProperty, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("empty property path")
	}
	if elements == nil {
		return nil, fmt.Errorf("element list is nil")
	}

	currentIDShort := path[0]
	var foundElement aastypes.ISubmodelElement
	for _, el := range elements {
		if el.IDShort() != nil && *el.IDShort() == currentIDShort {
			foundElement = el
			break
		}
	}

	if foundElement == nil {
		return nil, fmt.Errorf("element '%s' not found", currentIDShort)
	}

	// If this is the last element in the path, it must be a Property
	if len(path) == 1 {
		if prop, ok := foundElement.(aastypes.IProperty); ok {
			return prop, nil
		}
		return nil, fmt.Errorf("element '%s' is not a Property", currentIDShort)
	}

	// If not the last element, it must be a Collection
	if smc, ok := foundElement.(aastypes.ISubmodelElementCollection); ok {
		// Recurse into the collection's Value (which is its list of elements)
		return findProperty(smc.Value(), path[1:])
	}

	return nil, fmt.Errorf("element '%s' is not a SubmodelElementCollection (needed for nested path)", currentIDShort)
}

// GetSubmodel (for exporters) now uses a Read-Lock and new types.
// FIXED: This function is now compatible with 'aastypes'.
func (c *AASCore) GetSubmodel(idShort string) (aastypes.ISubmodel, error) {
	c.modelMutex.RLock() // Use Read-Lock
	defer c.modelMutex.RUnlock()

	sm := findSubmodel(c.env, idShort)
	if sm == nil {
		return nil, fmt.Errorf("submodel with idShort %s not found", idShort)
	}

	// TODO: Deep copy 'sm' to prevent race conditions if the caller modifies it.
	// For now, we return a direct pointer from the model, which is fast but
	// means the exporter should *not* modify the returned object.
	return sm, nil
}
