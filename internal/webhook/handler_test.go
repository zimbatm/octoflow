package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/pkg/events"
)

func TestHandlerWebhookValidation(t *testing.T) {
	eventCh := make(chanPublisher, 10)

	tests := []struct {
		name           string
		secret         string
		payload        string
		signature      string
		expectedStatus int
	}{
		{
			name:           "valid signature",
			secret:         "test-secret",
			payload:        `{"action":"opened","repository":{"full_name":"test-org/test-repo","owner":{"login":"test-org"}},"sender":{"login":"test-user"}}`,
			signature:      "", // will be calculated
			expectedStatus: http.StatusOK,
		},
		{
			name:           "invalid signature",
			secret:         "test-secret",
			payload:        `{"action":"opened","repository":{"full_name":"test-org/test-repo","owner":{"login":"test-org"}},"sender":{"login":"test-user"}}`,
			signature:      "sha256=invalid",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "no secret configured",
			secret:         "",
			payload:        `{"action":"opened","repository":{"full_name":"test-org/test-repo","owner":{"login":"test-org"}},"sender":{"login":"test-user"}}`,
			signature:      "",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "missing required headers",
			secret:         "",
			payload:        `{"action":"opened","repository":{"full_name":"test-org/test-repo","owner":{"login":"test-org"}},"sender":{"login":"test-user"}}`,
			signature:      "",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.WebhookConfig{
				Secret:         tt.secret,
				MaxPayloadSize: 26214400, // 25MB
			}
			handler := NewHandler(cfg, eventCh)

			// Calculate signature if needed
			if tt.signature == "" && tt.secret != "" {
				h := hmac.New(sha256.New, []byte(tt.secret))
				h.Write([]byte(tt.payload))
				tt.signature = "sha256=" + hex.EncodeToString(h.Sum(nil))
			}

			req := httptest.NewRequest("POST", "/webhook", bytes.NewBufferString(tt.payload))
			if tt.name != "missing required headers" {
				req.Header.Set("X-GitHub-Event", "pull_request")
				req.Header.Set("X-GitHub-Delivery", "test-123")
				req.Header.Set("Content-Type", "application/json")
			}
			if tt.signature != "" {
				req.Header.Set("X-Hub-Signature-256", tt.signature)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rec.Code)
			}

			// Check if event was sent to channel for successful requests
			if tt.expectedStatus == http.StatusOK && tt.name != "missing required headers" {
				select {
				case event := <-eventCh:
					if event.Event != "pull_request" {
						t.Errorf("Expected event type pull_request, got %s", event.Event)
					}
				default:
					t.Error("Expected event in channel, got none")
				}
			}
		})
	}
}

func TestHandlerEventParsing(t *testing.T) {
	eventCh := make(chanPublisher, 10)
	cfg := &config.WebhookConfig{
		Secret:         "",
		MaxPayloadSize: 26214400, // 25MB
	}
	handler := NewHandler(cfg, eventCh)

	payload := `{
		"action": "opened",
		"pull_request": {
			"id": 123,
			"number": 1,
			"title": "Test PR",
			"created_at": "2025-01-26T12:00:00Z"
		},
		"repository": {
			"full_name": "test-org/test-repo",
			"owner": {
				"login": "test-org"
			}
		},
		"sender": {
			"login": "test-user"
		}
	}`

	req := httptest.NewRequest("POST", "/webhook", bytes.NewBufferString(payload))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", "test-456")
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", rec.Code)
	}

	// Check parsed event
	select {
	case event := <-eventCh:
		if event.Event != "pull_request" {
			t.Errorf("Expected event type pull_request, got %s", event.Event)
		}
		if event.Organization != "test-org" {
			t.Errorf("Expected organization test-org, got %s", event.Organization)
		}
		if event.Repository != "test-org/test-repo" {
			t.Errorf("Expected repository test-org/test-repo, got %s", event.Repository)
		}
		if event.Sender != "test-user" {
			t.Errorf("Expected sender test-user, got %s", event.Sender)
		}
		if event.ID != "test-456" {
			t.Errorf("Expected ID test-456, got %s", event.ID)
		}
	default:
		t.Error("Expected event in channel, got none")
	}
}

type chanPublisher chan *events.GitHubEvent

func (c chanPublisher) PublishEvent(ctx context.Context, e *events.GitHubEvent) error {
	c <- e
	return nil
}

type failingPublisher struct{}

func (failingPublisher) PublishEvent(ctx context.Context, e *events.GitHubEvent) error {
	return errors.New("nats down")
}

func TestHandlerPublishFailure(t *testing.T) {
	cfg := &config.WebhookConfig{MaxPayloadSize: 1 << 20, RequestTimeout: time.Second}
	handler := NewHandler(cfg, failingPublisher{})

	req := httptest.NewRequest("POST", "/webhook", bytes.NewBufferString(`{"action":"opened"}`))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", "test-789")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}
