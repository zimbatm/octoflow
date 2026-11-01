package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/metrics"
	"github.com/zimbatm/octoflow/internal/retry"
	"github.com/zimbatm/octoflow/pkg/events"
)

// Publisher must deduplicate by event.ID: GitHub redeliveries reuse the delivery id.
type Publisher interface {
	PublishEvent(ctx context.Context, event *events.GitHubEvent) error
}

// Handler processes GitHub webhook requests
type Handler struct {
	config    *config.WebhookConfig
	publisher Publisher
}

// NewHandler creates a new webhook handler
func NewHandler(cfg *config.WebhookConfig, publisher Publisher) *Handler {
	return &Handler{
		config:    cfg,
		publisher: publisher,
	}
}

// ServeHTTP handles incoming webhook requests
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var eventType, organization, status string
	var payloadSize int

	defer func() {
		// Record metrics
		metrics.RecordWebhookRequest(eventType, organization, status, time.Since(start).Seconds(), payloadSize)
	}()

	// Only accept POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		status = "method_not_allowed"
		return
	}

	// Check content type
	contentType := r.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "application/json") {
		http.Error(w, "Content-Type must be application/json", http.StatusBadRequest)
		status = "bad_content_type"
		return
	}

	// Read the body with size limit
	body, err := io.ReadAll(io.LimitReader(r.Body, h.config.MaxPayloadSize))
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		status = "read_error"
		return
	}
	defer r.Body.Close()
	payloadSize = len(body)

	// Extract GitHub headers
	deliveryID := r.Header.Get("X-GitHub-Delivery")
	eventType = r.Header.Get("X-GitHub-Event")
	signature := r.Header.Get("X-Hub-Signature-256")
	hookID := r.Header.Get("X-GitHub-Hook-ID")
	hookInstallation := r.Header.Get("X-GitHub-Hook-Installation-Target-ID")

	// Validate required headers
	if deliveryID == "" || eventType == "" {
		http.Error(w, "Missing required GitHub headers", http.StatusBadRequest)
		status = "missing_headers"
		return
	}

	// Validate signature
	if !h.validateSignature(body, signature) {
		if h.config.Secret == "" && signature != "" {
			slog.Warn("Webhook signature provided but no secret configured",
				"delivery_id", deliveryID,
				"hint", "Set WEBHOOK_SECRET environment variable")
		}
		http.Error(w, "Invalid signature", http.StatusUnauthorized)
		status = "invalid_signature"
		return
	}

	// Create event
	event := &events.GitHubEvent{
		ID:                deliveryID,
		Event:             eventType,
		Signature:         signature,
		HookID:            hookID,
		HookInstallation:  hookInstallation,
		ReceivedAt:        time.Now().UTC(),
		Payload:           json.RawMessage(body),
		ProcessingVersion: 1,
		Retries:           0,
	}

	// Parse common fields from payload
	if err := events.ParseCommonFields(event); err != nil {
		// Log error but don't fail the request
		slog.Error("Failed to parse common fields", "error", err)
		metrics.EventProcessingErrors.WithLabelValues("webhook", "parse_error").Inc()
	}
	organization = event.Organization

	// Log incoming webhook
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
		"hook_id", hookID)

	// Check if organization is allowed
	if !h.isOrgAllowed(event.Organization) {
		http.Error(w, "Organization not allowed", http.StatusForbidden)
		status = "org_not_allowed"
		return
	}

	// Validate event
	if err := events.ValidateEvent(event); err != nil {
		http.Error(w, fmt.Sprintf("Invalid event: %v", err), http.StatusBadRequest)
		status = "invalid_event"
		return
	}

	// Handle ping events specially - acknowledge but don't process
	if eventType == "ping" {
		slog.Info("Received ping event, acknowledging without processing",
			"delivery_id", deliveryID,
			"hook_id", hookID)
		writeJSON(w, http.StatusOK, fmt.Sprintf(`{"status":"ok","message":"pong","delivery_id":"%s"}`, deliveryID))
		status = "success_ping"
		return
	}

	// reply 200 only once the event is stored, so a failed delivery shows as
	// failed in GitHub and can be redelivered.
	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout())
	defer cancel()
	err = retry.Do(ctx, func(ctx context.Context) error {
		return h.publisher.PublishEvent(ctx, event)
	}, retry.DefaultConfig())
	if err != nil {
		slog.Error("Failed to publish event",
			"delivery_id", deliveryID,
			"event_type", eventType,
			"error", err)
		http.Error(w, "Failed to store event", http.StatusServiceUnavailable)
		status = "publish_error"
		return
	}

	writeJSON(w, http.StatusOK, fmt.Sprintf(`{"status":"ok","delivery_id":"%s"}`, deliveryID))
	status = "success"
}

// validateSignature validates the GitHub webhook signature
func (h *Handler) validateSignature(payload []byte, signature string) bool {
	// If no secret is configured locally, skip validation
	if h.config.Secret == "" {
		return true
	}

	// If we have a secret configured but GitHub didn't send a signature,
	// this means GitHub doesn't have the secret configured
	if signature == "" {
		slog.Warn("Local webhook secret configured but GitHub sent no signature",
			"hint", "Either remove WEBHOOK_SECRET env var or configure secret in GitHub webhook settings")
		return false
	}

	// Remove "sha256=" prefix
	if !strings.HasPrefix(signature, "sha256=") {
		return false
	}
	signature = strings.TrimPrefix(signature, "sha256=")

	// Calculate expected signature
	mac := hmac.New(sha256.New, []byte(h.config.Secret))
	mac.Write(payload)
	expectedSig := hex.EncodeToString(mac.Sum(nil))

	// Constant-time comparison
	return hmac.Equal([]byte(signature), []byte(expectedSig))
}

// isOrgAllowed checks if the organization is in the allowed list
func (h *Handler) isOrgAllowed(org string) bool {
	// If no organizations specified, all are allowed
	if len(h.config.AllowedOrgs) == 0 {
		return true
	}

	// Check if org is in allowed list
	for _, allowed := range h.config.AllowedOrgs {
		if allowed == org {
			return true
		}
	}

	return false
}

// a zero timeout makes the select pick randomly between a ready channel and an expired timer.
func (h *Handler) requestTimeout() time.Duration {
	if h.config.RequestTimeout > 0 {
		return h.config.RequestTimeout
	}
	return 10 * time.Second
}
