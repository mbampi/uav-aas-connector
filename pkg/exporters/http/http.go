package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"

	"github.com/aas-core-works/aas-core3.0-golang/jsonization"
)

// HttpExporter implements the "pull" model for serving AAS data over HTTP.
// It does not listen to the update channel, but instead fetches
// data directly from the AASCore on demand.
type HttpExporter struct {
	cfg    config.ExporterConfig
	core   *core.AASCore
	server *http.Server
	wg     sync.WaitGroup
}

// NewExporter creates a new HttpExporter.
func NewExporter(cfg config.ExporterConfig, core *core.AASCore) (*HttpExporter, error) {
	if core == nil {
		return nil, fmt.Errorf("AASCore instance is required for HttpExporter")
	}
	return &HttpExporter{
		cfg:  cfg,
		core: core,
	}, nil
}

// Start begins the HTTP server.
// The inChan is ignored as this is a "pull" exporter.
func (h *HttpExporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	log.Printf("HttpExporter starting on port %d", h.cfg.Port)

	// Create the HTTP request multiplexer
	mux := http.NewServeMux()
	mux.HandleFunc("/submodels/", h.handleGetSubmodel)

	// Configure the HTTP server
	addr := fmt.Sprintf(":%d", h.cfg.Port)
	h.server = &http.Server{
		Addr:    addr,
		Handler: mux,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx // Propagate the application context to handlers
		},
	}

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		log.Printf("HttpExporter listening on %s", addr)
		if err := h.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("HttpExporter server error: %v", err)
		} else {
			log.Printf("HttpExporter server shut down gracefully")
		}
	}()

	// Create a separate goroutine to listen for context cancellation
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		<-ctx.Done() // Block until context is canceled
		log.Println("HttpExporter shutting down due to context cancellation...")

		// Create a shutdown context with a timeout
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := h.server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HttpExporter server shutdown error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP server.
// This is now primarily handled by the context cancellation in Start,
// but we wait for the goroutines to finish.
func (h *HttpExporter) Stop() error {
	log.Println("HttpExporter Stop() called. Waiting for shutdown...")
	// Wait for the server and context listener goroutines to complete
	h.wg.Wait()
	log.Println("HttpExporter stopped.")
	return nil
}

// handleGetSubmodel handles requests for /submodels/{idShort}.
func (h *HttpExporter) handleGetSubmodel(w http.ResponseWriter, r *http.Request) {
	// 1. Extract idShort from path: /submodels/Identification -> Identification
	idShort := strings.TrimPrefix(r.URL.Path, "/submodels/")
	if idShort == "" {
		http.Error(w, "Submodel idShort missing. Use /submodels/{idShort}", http.StatusBadRequest)
		return
	}

	// 2. Fetch Submodel from core
	sm, err := h.core.GetSubmodel(idShort)
	if err != nil {
		log.Printf("HttpExporter: Submodel '%s' not found.", idShort)
		http.Error(w, fmt.Sprintf("Submodel with idShort '%s' not found", idShort), http.StatusNotFound)
		return
	}

	// 3. Serialize Submodel to JSON
	// We use the aas-core-works jsonization package to match the format
	// used for loading the model.
	jsonable, err := jsonization.ToJsonable(sm)
	if err != nil {
		log.Printf("HttpExporter: Failed to serialize Submodel '%s': %v", idShort, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// 4. Write JSON response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(jsonable); err != nil {
		log.Printf("HttpExporter: Failed to encode JSON response for '%s': %v", idShort, err)
		// We can't write an http.Error here because headers are already sent
		return
	}

	log.Printf("HttpExporter: Served Submodel '%s' successfully.", idShort)
}
