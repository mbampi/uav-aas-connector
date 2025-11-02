package dummy

import (
	"context"
	"log"
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
	log.Printf("DummyExporter started on port %d", d.cfg.Port)

	for {
		select {
		case <-ctx.Done():
			log.Println("DummyExporter shutting down")
			return nil
		case event := <-inChan:
			log.Printf("DummyExporter received AASUpdateEvent: %+v", event)
		}
	}
}

// Stop provides a way to gracefully shut down the exporter.
func (d *DummyExporter) Stop() error {
	log.Println("DummyExporter stopped")
	return nil
}
