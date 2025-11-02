package adapters

import (
	"context"
	"fmt"
	"uav-aas-connector/pkg/adapters/mavlink"
	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
)

// InputAdapter is the interface for all "Extract" components.
type InputAdapter interface {
	// Start begins the adapter's work loop (e.g., connecting, listening).
	// It sends all translated data to the outChan.
	Start(ctx context.Context, outChan chan<- *core.AASUpdateEvent) error

	// Stop provides a way to gracefully shut down the adapter
	// (e.g., close network connections).
	Stop() error
}

// NewInputAdapter creates a new InputAdapter based on the provided configuration.
func NewInputAdapter(cfg config.AdapterConfig) (InputAdapter, error) {
	switch cfg.Type {
	case "mavlink":
		return mavlink.NewAdapter(cfg)
	// Add other adapter types here
	default:
		return nil, fmt.Errorf("unknown adapter type: %s", cfg.Type)
	}
}
