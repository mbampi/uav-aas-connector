package dummy

import (
	"context"
	"log/slog"
	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
)

// DummyExporter is a simple exporter that logs received AAS update events.
type DummyExporter struct {
	cfg config.ExporterConfig
}

// NewDummyExporter creates a new DummyExporter with the given configuration.
func NewDummyExporter(cfg config.ExporterConfig) *DummyExporter {
	return &DummyExporter{cfg: cfg}
}

// Start begins the exporter's work loop.
// It runs until the context is canceled.
// It receives all processed data from the inChan.
func (d *DummyExporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	slog.Info("DummyExporter started", "port", d.cfg.Port)

	for {
		select {
		case <-ctx.Done():
			slog.Info("DummyExporter shutting down")
			return nil
		case event := <-inChan:
			slog.Info("DummyExporter received AASUpdateEvent", "event", event)
		}
	}
}

// Stop provides a way to gracefully shut down the exporter.
func (d *DummyExporter) Stop() error {
	slog.Info("DummyExporter stopped")
	return nil
}
