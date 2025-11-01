package adapters

import (
	"context"
	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
)

// InputAdapter is the interface for all "Extract" components.
type InputAdapter interface {
	// Start begins the adapter's work loop (e.g., connecting, listening).
	// It sends all translated data to the outChan.
	Start(ctx context.Context, outChan chan<- *core.RawDataPoint) error

	// Stop provides a way to gracefully shut down the adapter
	// (e.g., close network connections).
	Stop() error
}

// NewInputAdapter creates a new InputAdapter based on the provided configuration.
func NewInputAdapter(cfg config.AdapterConfig) (InputAdapter, error) {
	// Implementation to create and return the appropriate InputAdapter
	// based on the provided configuration.
	// This is a placeholder; actual implementation would inspect cfg
	// and instantiate the correct adapter type.
	return nil, nil
}
