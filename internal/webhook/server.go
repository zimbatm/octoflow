package webhook

import (
	"io"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zimbatm/octoflow/internal/config"
)

// NewServer returns the receiver's HTTP server, ready for ListenAndServe.
func NewServer(cfg *config.WebhookConfig, publisher Publisher) *http.Server {
	handler := NewHandler(cfg, publisher)

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

	return &http.Server{
		Addr:        cfg.ListenAddr,
		Handler:     mux,
		ReadTimeout: cfg.RequestTimeout,
		// leave room to write the 503 after a publish times out.
		WriteTimeout: cfg.RequestTimeout + 5*time.Second,
		IdleTimeout:  120 * time.Second,
	}
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
		writeJSON(w, http.StatusMethodNotAllowed, `{"error":"Method not allowed. POST requests should go to /webhook","webhook_endpoint":"/webhook"}`)
		return
	}

	writeJSON(w, http.StatusOK, `{
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
	writeJSON(w, http.StatusOK, `{"status":"healthy","service":"webhook-receiver"}`)
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
