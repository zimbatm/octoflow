package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	// the server has no read or write timeout, SSE streams share it; bound
	// this request here: a 25 MB payload on a slow link needs a minute, and
	// the 503 after a publish timeout must still get out.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(start.Add(time.Minute))
	_ = rc.SetWriteDeadline(start.Add(time.Minute + h.config.RequestTimeout + 5*time.Second))

	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		fail(http.StatusBadRequest, "Content-Type must be application/json", "bad_content_type")
		return
	}

	deliveryID := r.Header.Get("X-GitHub-Delivery")
	headerEvent := r.Header.Get("X-GitHub-Event")
	if deliveryID == "" || headerEvent == "" {
		fail(http.StatusBadRequest, "Missing required GitHub headers", "missing_headers")
		return
	}
	// 25 MB is read and hashed only for a request that can still pass.
	signature := r.Header.Get("X-Hub-Signature-256")
	if h.config.Secret != "" && !wellFormed(signature) {
		fail(http.StatusUnauthorized, "Invalid signature", "invalid_signature")
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

	if !h.validSignature(body, signature) {
		fail(http.StatusUnauthorized, "Invalid signature", "invalid_signature")
		return
	}
	// headers become metric labels; only verified requests may create series.
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

	slog.Debug("Received webhook event",
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
		// not a label either: a rejected sender must not create series.
		fail(http.StatusForbidden, "Organization not allowed", "org_not_allowed")
		return
	}
	organization = event.Organization

	// a ping is GitHub testing the endpoint; it is answered, not stored.
	if eventType == "ping" {
		reply(w, map[string]string{"status": "ok", "message": "pong", "delivery_id": deliveryID})
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

	reply(w, map[string]string{"status": "ok", "delivery_id": deliveryID})
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

// wellFormed is the shape GitHub sends: sha256= and 64 hex digits.
func wellFormed(signature string) bool {
	hexSig, ok := strings.CutPrefix(signature, "sha256=")
	if !ok || len(hexSig) != 64 {
		return false
	}
	_, err := hex.DecodeString(hexSig)
	return err == nil
}

func reply(w http.ResponseWriter, body map[string]string) {
	b, _ := json.Marshal(body)
	writeJSON(w, http.StatusOK, string(b))
}
