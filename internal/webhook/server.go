package webhook

import (
	"io"
	"net/http"

	"github.com/zimbatm/octoflow/internal/config"
)

// Routes registers the receiver on mux: deliveries on POST /webhook, and a
// help page on GET /webhook for whoever opens the URL in a browser.
func Routes(mux *http.ServeMux, cfg *config.WebhookConfig, publisher Publisher) {
	mux.Handle("POST /webhook", NewHandler(cfg, publisher))
	mux.HandleFunc("GET /webhook", helpHandler)
}

func helpHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, `{
  "service": "octoflow-webhook-receiver",
  "description": "GitHub webhook receiver for the octoflow event pipeline",
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
