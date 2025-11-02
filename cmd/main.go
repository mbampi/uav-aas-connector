package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"uav-aas-connector/pkg/adapters"
	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	"uav-aas-connector/pkg/exporters"
)

// 1. Load config (using pkg/config).
// 2. Create channels (rawChan, processedChan).
// 3. Start Adapters, Core and Exporters as goroutines.
// 4. Handle graceful shutdown on interrupt signal.

func main() {
	cfg, err := config.LoadConfig("config/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Input Adapter
	rawChan := make(chan *core.AASUpdateEvent, 100)
	adapter, err := adapters.NewInputAdapter(cfg.InputAdapter)
	if err != nil {
		log.Fatalf("Failed to create adapter: %v", err)
	}
	go func(adpt adapters.InputAdapter) {
		if err := adpt.Start(ctx, rawChan); err != nil {
			log.Printf("Adapter error: %v", err)
		}
	}(adapter)

	// Start AAS Core
	coreInstance, err := core.NewAASCore(cfg.AAS)
	if err != nil {
		log.Fatalf("Failed to create AAS core: %v", err)
	}
	go coreInstance.Start(ctx, rawChan)

	// Start Output Exporters
	for _, exporterCfg := range cfg.OutputExporters {
		exporter, err := exporters.NewOutputExporter(exporterCfg)
		if err != nil {
			log.Printf("Failed to create exporter: %v", err)
			continue
		}
		go func(exp exporters.OutputExporter) {
			aasUpdateChan, err := coreInstance.Subscribe(ctx)
			if err != nil {
				log.Printf("Failed to subscribe exporter: %v", err)
				return
			}
			if err := exp.Start(ctx, aasUpdateChan); err != nil {
				log.Printf("Exporter error: %v", err)
			}
		}(exporter)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutdown signal received")

	cancel()                    // stop all goroutines
	time.Sleep(2 * time.Second) // Give some time for goroutines to finish

	log.Println("Shutdown complete")
}
