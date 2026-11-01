package webhook

import (
	"io"
	"net/http"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
)

// NewServer returns the receiver's HTTP server, ready for ListenAndServe.
func NewServer(cfg *config.WebhookConfig, publisher Publisher) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("POST /webhook", NewHandler(cfg, publisher))
	mux.HandleFunc("GET /{$}", rootHandler)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, `{"status":"healthy","service":"webhook-receiver"}`)
	})

	return &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// a 25 MB payload on a slow link needs longer than the publish timeout.
		ReadTimeout: time.Minute,
		// leave room to write the 503 after a publish times out.
		WriteTimeout: time.Minute + cfg.RequestTimeout + 5*time.Second,
		IdleTimeout:  120 * time.Second,
	}
}

// rootHandler tells a browser what this endpoint is and how to configure GitHub.
func rootHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, `{
  "service": "octoflow-webhook-receiver",
  "description": "GitHub webhook receiver for the octoflow event pipeline",
  "endpoints": {
    "/": "This help message",
    "/webhook": "GitHub webhook endpoint (POST)",
    "/health": "Health check endpoint (GET)"
  },
  "github_setup": {
    "payload_url": "https://your-domain.com/webhook",
    "content_type": "application/json",
    "secret": "Your webhook secret",
    "events": "Send me everything or select specific events"
  },
  "documentation": "https://github.com/zimbatm/octoflow"
}`)
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
