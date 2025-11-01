package exporters

import (
	"context"
	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
)

// OutputExporter is the interface for all "Load" components.
// Implementations exist in `pkg/exporters/*`
type OutputExporter interface {
	// Start begins the exporter's work loop.
	// It runs until the context is canceled.
	// It receives all processed data from the inChan.
	Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error

	// Stop provides a way to gracefully shut down the exporter
	// (e.g., disconnect from OPC-UA server, stop HTTP server).
	Stop() error
}

// NewOutputExporter creates a new OutputExporter based on the provided configuration.
func NewOutputExporter(cfg config.ExporterConfig) (OutputExporter, error) {
	// Implementation to create and return the appropriate OutputExporter
	// based on the provided configuration.
	// This is a placeholder; actual implementation would inspect cfg
	// and instantiate the correct exporter type.
	return nil, nil
}
