package exporters

import (
	"context"
	"fmt"
	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	"uav-aas-connector/pkg/exporters/dummy"
	"uav-aas-connector/pkg/exporters/http"
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
func NewOutputExporter(cfg config.ExporterConfig, core *core.AASCore) (OutputExporter, error) {
	if !cfg.Enabled {
		return nil, fmt.Errorf("exporter is disabled: %s", cfg.Type)
	}

	switch cfg.Type {
	case "opcua":
		// return opcua.NewExporter(cfg, core)
	case "http":
		return http.NewExporter(cfg, core)
	case "dummy":
		return dummy.NewDummyExporter(cfg), nil
	default:
		return nil, fmt.Errorf("unknown exporter type: %s", cfg.Type)
	}

	return nil, nil
}
