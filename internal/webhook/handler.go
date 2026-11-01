package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/metrics"
	"github.com/zimbatm/octoflow/internal/retry"
	"github.com/zimbatm/octoflow/pkg/events"
)

// Publisher must deduplicate by event.ID: GitHub redeliveries reuse the delivery id.
type Publisher interface {
	Publish(ctx context.Context, event *events.GitHubEvent) (duplicate bool, err error)
}

// Handler receives GitHub webhook deliveries on POST /webhook.
type Handler struct {
	config    *config.WebhookConfig
	publisher Publisher
}

func NewHandler(cfg *config.WebhookConfig, publisher Publisher) *Handler {
	return &Handler{config: cfg, publisher: publisher}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var eventType, organization, status string
	var payloadSize int
	defer func() {
		metrics.WebhookRequestsTotal.WithLabelValues(eventType, organization, status).Inc()
		metrics.WebhookRequestDuration.WithLabelValues(eventType).Observe(time.Since(start).Seconds())
		metrics.WebhookPayloadSize.WithLabelValues(eventType).Observe(float64(payloadSize))
	}()
	fail := func(code int, msg, why string) {
		http.Error(w, msg, code)
		status = why
	}

	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		fail(http.StatusBadRequest, "Content-Type must be application/json", "bad_content_type")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.config.MaxPayloadSize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			fail(http.StatusRequestEntityTooLarge, "Payload too large", "too_large")
		} else {
			fail(http.StatusBadRequest, "Failed to read request body", "read_error")
		}
		return
	}
	payloadSize = len(body)

	deliveryID := r.Header.Get("X-GitHub-Delivery")
	headerEvent := r.Header.Get("X-GitHub-Event")
	if deliveryID == "" || headerEvent == "" {
		fail(http.StatusBadRequest, "Missing required GitHub headers", "missing_headers")
		return
	}
	if !h.validSignature(body, r.Header.Get("X-Hub-Signature-256")) {
		fail(http.StatusUnauthorized, "Invalid signature", "invalid_signature")
		return
	}
	// the header becomes a metric label; only verified requests may create series.
	eventType = headerEvent

	event := &events.GitHubEvent{
		ID:               deliveryID,
		Event:            eventType,
		HookID:           r.Header.Get("X-GitHub-Hook-ID"),
		HookInstallation: r.Header.Get("X-GitHub-Hook-Installation-Target-ID"),
		ReceivedAt:       time.Now().UTC(),
		Payload:          json.RawMessage(body),
	}
	if err := events.ParseCommonFields(event); err != nil {
		fail(http.StatusBadRequest, "Payload is not JSON", "invalid_payload")
		return
	}
	organization = event.Organization

	slog.Info("Received webhook event",
		"delivery_id", deliveryID,
		"event_type", eventType,
		"organization", organization,
		"organization_id", event.OrganizationID,
		"repository", event.Repository,
		"repository_id", event.RepositoryID,
		"sender", event.Sender,
		"sender_id", event.SenderID,
		"payload_size", payloadSize,
		"hook_id", event.HookID)

	if len(h.config.AllowedOrgs) > 0 && !slices.Contains(h.config.AllowedOrgs, event.Organization) {
		fail(http.StatusForbidden, "Organization not allowed", "org_not_allowed")
		return
	}

	// a ping is GitHub testing the endpoint; it is answered, not stored.
	if eventType == "ping" {
		writeJSON(w, http.StatusOK, fmt.Sprintf(`{"status":"ok","message":"pong","delivery_id":"%s"}`, deliveryID))
		status = "success_ping"
		return
	}

	// reply 200 only once the event is stored, so a failed delivery shows as
	// failed in GitHub and can be redelivered.
	ctx, cancel := context.WithTimeout(r.Context(), h.config.RequestTimeout)
	defer cancel()
	err = retry.Do(ctx, func(ctx context.Context) error {
		_, err := h.publisher.Publish(ctx, event)
		return err
	}, retry.DefaultConfig())
	if err != nil {
		slog.Error("Failed to publish event", "delivery_id", deliveryID, "event_type", eventType, "error", err)
		fail(http.StatusServiceUnavailable, "Failed to store event", "publish_error")
		return
	}

	writeJSON(w, http.StatusOK, fmt.Sprintf(`{"status":"ok","delivery_id":"%s"}`, deliveryID))
	status = "success"
}

// validSignature checks the HMAC-SHA256 that GitHub sends. Without a
// configured secret every delivery passes.
func (h *Handler) validSignature(payload []byte, signature string) bool {
	if h.config.Secret == "" {
		return true
	}
	hexSig, ok := strings.CutPrefix(signature, "sha256=")
	if !ok {
		return false
	}
	mac := hmac.New(sha256.New, []byte(h.config.Secret))
	mac.Write(payload)
	return hmac.Equal([]byte(hexSig), []byte(hex.EncodeToString(mac.Sum(nil))))
}
