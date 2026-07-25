package main

import (
	"context"
	"fmt"
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

	// Validate the adapter config up front so a broken config fails fast
	// instead of being retried forever by the supervisor below.
	if _, err := adapters.NewInputAdapter(cfg.InputAdapter); err != nil {
		slog.Error("Failed to create adapter", "error", err)
		os.Exit(1)
	}
	go runSupervised(ctx, "adapter:"+cfg.InputAdapter.Type, func(attemptCtx context.Context) error {
		adapter, err := adapters.NewInputAdapter(cfg.InputAdapter)
		if err != nil {
			return err
		}
		return adapter.Start(attemptCtx, rawChan)
	})

	coreInstance, err := core.NewAASCore(cfg.AAS)
	if err != nil {
		slog.Error("Failed to create AAS core", "error", err)
		os.Exit(1)
	}
	go coreInstance.Start(ctx, rawChan)

	// Start Output Exporters
	for _, exporterCfg := range cfg.OutputExporters {
		if _, err := exporters.NewOutputExporter(exporterCfg, coreInstance); err != nil {
			slog.Error("Failed to create exporter", "error", err, "exporter", exporterCfg.Type)
			continue
		}

		slog.Info("Starting exporter", "exporter", exporterCfg.Type)
		go runSupervised(ctx, "exporter:"+exporterCfg.Type, func(attemptCtx context.Context) error {
			exporter, err := exporters.NewOutputExporter(exporterCfg, coreInstance)
			if err != nil {
				return err
			}
			aasUpdateChan, err := coreInstance.Subscribe(attemptCtx)
			if err != nil {
				return err
			}
			return exporter.Start(attemptCtx, aasUpdateChan)
		})
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

const (
	supervisorBaseBackoff = 1 * time.Second
	supervisorMaxBackoff  = 30 * time.Second
)

// runSupervised runs start repeatedly until ctx is canceled, recovering from
// panics and restarting after an exponential backoff whenever it returns an
// error. Each attempt gets its own child context so any state tied to the
// failed attempt (e.g. a core subscription) is torn down before the next one
// begins.
func runSupervised(ctx context.Context, name string, start func(attemptCtx context.Context) error) {
	backoff := supervisorBaseBackoff

	for ctx.Err() == nil {
		attemptCtx, cancel := context.WithCancel(ctx)
		startedAt := time.Now()
		err := runAttempt(attemptCtx, start)
		cancel()

		if err == nil || ctx.Err() != nil {
			return
		}

		// A long-lived run failing is a fresh problem, not a continuation of
		// an earlier one, so don't keep punishing it with an inflated backoff.
		if time.Since(startedAt) > supervisorMaxBackoff {
			backoff = supervisorBaseBackoff
		}

		slog.Error("Component stopped, restarting", "component", name, "error", err, "retry_in", backoff)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, supervisorMaxBackoff)
	}
}

// runAttempt executes start once, converting any panic into an error so
// runSupervised can decide whether to restart.
func runAttempt(ctx context.Context, start func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return start(ctx)
}
