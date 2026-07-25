package http

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
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
	if cfg.Security.TLSEnabled && (cfg.Security.CertFile == "" || cfg.Security.KeyFile == "") {
		return nil, fmt.Errorf("certFile and keyFile are required when security.tlsEnabled is true")
	}
	return &HttpExporter{
		cfg:  cfg,
		core: core,
	}, nil
}

// Start begins the HTTP server.
// The inChan is ignored as this is a "pull" exporter.
func (h *HttpExporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	slog.Info("HttpExporter starting", "port", h.cfg.Port)

	mux := http.NewServeMux()

	// List all Submodels
	mux.HandleFunc("GET /submodels", h.handleGetAllSubmodels)

	// Get specific Submodel by ID (Base64URL encoded)
	mux.HandleFunc("GET /submodels/{id}", h.handleGetSubmodelByID)

	var handler http.Handler = mux
	if h.cfg.Security.Username != "" {
		handler = basicAuthMiddleware(h.cfg.Security.Username, h.cfg.Security.Password, mux)
	}

	addr := fmt.Sprintf(":%d", h.cfg.Port)
	h.server = &http.Server{
		Addr:    addr,
		Handler: handler,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		slog.Info("HttpExporter listening", "addr", addr, "tls", h.cfg.Security.TLSEnabled)

		var err error
		if h.cfg.Security.TLSEnabled {
			err = h.server.ListenAndServeTLS(h.cfg.Security.CertFile, h.cfg.Security.KeyFile)
		} else {
			err = h.server.ListenAndServe()
		}

		if err != http.ErrServerClosed {
			slog.Error("HttpExporter server error", "error", err)
		} else {
			slog.Info("HttpExporter server shut down gracefully")
		}
	}()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		<-ctx.Done()
		slog.Info("HttpExporter shutting down")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := h.server.Shutdown(shutdownCtx); err != nil {
			slog.Error("HttpExporter server shutdown error", "error", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP server.
// This is now primarily handled by the context cancellation in Start,
// but we wait for the goroutines to finish.
func (h *HttpExporter) Stop() error {
	slog.Info("HttpExporter Stop() called. Waiting for shutdown")
	h.wg.Wait()
	slog.Info("HttpExporter stopped")
	return nil
}

// basicAuthMiddleware requires HTTP Basic credentials matching user/pass on
// every request, using a constant-time comparison so a failed attempt can't
// leak timing information about the configured password.
func basicAuthMiddleware(user, pass string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, ok := r.BasicAuth()
		validUser := subtle.ConstantTimeCompare([]byte(gotUser), []byte(user)) == 1
		validPass := subtle.ConstantTimeCompare([]byte(gotPass), []byte(pass)) == 1

		if !ok || !validUser || !validPass {
			w.Header().Set("WWW-Authenticate", `Basic realm="uav-aas-connector"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// handleGetAllSubmodels returns a list of all available submodels.
func (h *HttpExporter) handleGetAllSubmodels(w http.ResponseWriter, r *http.Request) {
	submodels := h.core.GetAllSubmodels()

	// AAS compliant serialization
	var response []any
	for _, sm := range submodels {
		jsonable, err := jsonization.ToJsonable(sm)
		if err != nil {
			slog.Error("Failed to serialize submodel", "error", err)
			continue
		}
		response = append(response, jsonable)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("Failed to encode JSON response", "error", err)
	}
}

// handleGetSubmodelByID handles requests for a specific submodel.
func (h *HttpExporter) handleGetSubmodelByID(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("id")
	if rawID == "" {
		http.Error(w, "Missing ID", http.StatusBadRequest)
		return
	}

	// AAS Compliance: IDs in the URL path must be Base64URL encoded.
	// We try to decode it.
	decodedBytes, err := base64.URLEncoding.DecodeString(rawID)

	var searchID string
	if err != nil {
		// Fallback for debugging: if decoding fails, try using the raw ID.
		// This allows you to curl /submodels/Identification for testing.
		searchID = rawID
	} else {
		searchID = string(decodedBytes)
	}

	// Try finding by Global ID (Standard compliant)
	sm, err := h.core.GetSubmodelByID(searchID)
	if err != nil {
		// Fallback: Try finding by IDShort
		sm, err = h.core.GetSubmodel(searchID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Submodel not found: %s", searchID), http.StatusNotFound)
			return
		}
	}

	// Serialize using the AAS library
	jsonable, err := jsonization.ToJsonable(sm)
	if err != nil {
		slog.Error("Failed to serialize Submodel", "id", searchID, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(jsonable); err != nil {
		slog.Error("Failed to encode JSON response", "error", err)
	}
}
