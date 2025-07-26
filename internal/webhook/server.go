package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/pkg/events"
)

// Server represents the webhook HTTP server
type Server struct {
	config     *config.WebhookConfig
	httpServer *http.Server
	handler    *Handler
	eventCh    chan<- *events.GitHubEvent
}

// NewServer creates a new webhook server
func NewServer(cfg *config.WebhookConfig, eventCh chan<- *events.GitHubEvent) *Server {
	handler := NewHandler(cfg, eventCh)

	mux := http.NewServeMux()

	// Root handler with helpful information
	mux.HandleFunc("/", rootHandler)

	// Webhook endpoint
	mux.HandleFunc("/webhook", handler.ServeHTTP)

	// Health check endpoint
	mux.HandleFunc(cfg.HealthPath, healthHandler)

	// Metrics endpoint with Prometheus
	if cfg.MetricsPath != "" {
		mux.Handle(cfg.MetricsPath, promhttp.Handler())
	}

	return &Server{
		config:  cfg,
		handler: handler,
		eventCh: eventCh,
		httpServer: &http.Server{
			Addr:         cfg.ListenAddr,
			Handler:      mux,
			ReadTimeout:  cfg.RequestTimeout,
			WriteTimeout: cfg.RequestTimeout,
			IdleTimeout:  120 * time.Second,
		},
	}
}

// Start starts the webhook server
func (s *Server) Start() error {
	slog.Info("Starting webhook server", "address", s.config.ListenAddr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("Shutting down webhook server")
	return s.httpServer.Shutdown(ctx)
}

// rootHandler provides helpful information about the webhook service
func rootHandler(w http.ResponseWriter, r *http.Request) {
	// Only handle exact root path
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	// Only accept GET requests
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, `{"error":"Method not allowed. POST requests should go to /webhook","webhook_endpoint":"/webhook"}`)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{
  "service": "octoflow-webhook-receiver",
  "version": "1.0.0",
  "description": "GitHub webhook receiver for Octoflow event processing system",
  "endpoints": {
    "/": "This help message",
    "/webhook": "GitHub webhook endpoint (POST)",
    "/health": "Health check endpoint (GET)",
    "/metrics": "Prometheus metrics endpoint (GET)"
  },
  "github_setup": {
    "payload_url": "https://your-domain.com/webhook",
    "content_type": "application/json",
    "secret": "Your webhook secret (optional but recommended)",
    "events": "Send me everything or select specific events"
  },
  "documentation": "https://github.com/zimbatm/octoflow"
}`)
}

// healthHandler handles health check requests
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"healthy","service":"webhook-receiver"}`)
}
