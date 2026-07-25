package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"uav-aas-connector/pkg/adapters"
	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	"uav-aas-connector/pkg/exporters"
	"uav-aas-connector/pkg/instrumentation" // Import the new package
)

// 1. Load config (using pkg/config).
// 2. Create channels (rawChan, processedChan).
// 3. Start Adapters, Core and Exporters as goroutines.
// 4. Handle graceful shutdown on interrupt signal.

func main() {
	cfg, err := config.LoadConfig("config/config.yaml")
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     cfg.Advanced.GetLogLevel(),
	}))
	slog.SetDefault(logger)

	if err := instrumentation.Init(cfg.Advanced.MetricsPath); err != nil {
		slog.Error("Failed to init instrumentation", "error", err)
	}
	defer instrumentation.Close()

	// Start Input Adapter
	rawChan := make(chan *core.AASUpdateEvent, 1000)
	adapter, err := adapters.NewInputAdapter(cfg.InputAdapter)
	if err != nil {
		slog.Error("Failed to create adapter", "error", err)
		os.Exit(1)
	}
	go func(adpt adapters.InputAdapter) {
		if err := adpt.Start(ctx, rawChan); err != nil {
			slog.Error("Adapter runtime error", "error", err)
		}
	}(adapter)

	coreInstance, err := core.NewAASCore(cfg.AAS)
	if err != nil {
		slog.Error("Failed to create AAS core", "error", err)
		os.Exit(1)
	}
	go coreInstance.Start(ctx, rawChan)

	// Start Output Exporters
	for _, exporterCfg := range cfg.OutputExporters {
		exporter, err := exporters.NewOutputExporter(exporterCfg, coreInstance)
		if err != nil {
			slog.Error("Failed to create exporter", "error", err, "exporter", exporterCfg.Type)
			continue
		}
		slog.Info("Starting exporter", "exporter", exporterCfg.Type)
		go func(exp exporters.OutputExporter) {
			aasUpdateChan, err := coreInstance.Subscribe(ctx)
			if err != nil {
				slog.Error("Failed to subscribe exporter", "error", err, "exporter", exporterCfg.Type)
				return
			}
			if err := exp.Start(ctx, aasUpdateChan); err != nil {
				slog.Error("Exporter runtime error", "error", err, "exporter", exporterCfg.Type)
			}
		}(exporter)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	slog.Info("Shutdown signal received")

	cancel()                    // stop all goroutines
	time.Sleep(2 * time.Second) // Give some time for goroutines to finish

	slog.Info("Shutdown complete")
}
