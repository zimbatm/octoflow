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
				MaxPayloadSize: 26214400, RequestTimeout: time.Second,
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
		MaxPayloadSize: 26214400, RequestTimeout: time.Second,
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

func (c chanPublisher) Publish(ctx context.Context, e *events.GitHubEvent) (bool, error) {
	c <- e
	return false, nil
}

type failingPublisher struct{}

func (failingPublisher) Publish(ctx context.Context, e *events.GitHubEvent) (bool, error) {
	return false, errors.New("nats down")
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

func TestHandlerRejections(t *testing.T) {
	const payload = `{"action":"opened","repository":{"full_name":"test-org/test-repo","owner":{"login":"test-org"}},"sender":{"login":"test-user"}}`
	sign := func(secret, body string) string {
		h := hmac.New(sha256.New, []byte(secret))
		h.Write([]byte(body))
		return "sha256=" + hex.EncodeToString(h.Sum(nil))
	}

	tests := []struct {
		name    string
		cfg     config.WebhookConfig
		method  string
		body    string
		headers map[string]string
		want    int
	}{
		{
			name:   "GET is the help page, not a delivery",
			method: "GET",
			body:   payload,
			want:   http.StatusOK,
		},
		{
			name:    "wrong content type",
			body:    payload,
			headers: map[string]string{"Content-Type": "text/plain"},
			want:    http.StatusBadRequest,
		},
		{
			name: "body over the limit",
			cfg:  config.WebhookConfig{MaxPayloadSize: 16},
			body: payload,
			want: http.StatusRequestEntityTooLarge,
		},
		{
			name: "secret set but no signature sent",
			cfg:  config.WebhookConfig{Secret: "s"},
			body: payload,
			want: http.StatusUnauthorized,
		},
		{
			name:    "signature without the sha256 prefix",
			cfg:     config.WebhookConfig{Secret: "s"},
			body:    payload,
			headers: map[string]string{"X-Hub-Signature-256": sign("s", payload)[len("sha256="):]},
			want:    http.StatusUnauthorized,
		},
		{
			name:    "signature made with another secret",
			cfg:     config.WebhookConfig{Secret: "s"},
			body:    payload,
			headers: map[string]string{"X-Hub-Signature-256": sign("other", payload)},
			want:    http.StatusUnauthorized,
		},
		{
			name: "organization not in the allow-list",
			cfg:  config.WebhookConfig{AllowedOrgs: []string{"numtide"}},
			body: payload,
			want: http.StatusForbidden,
		},
		{
			name: "payload is not JSON",
			body: "{",
			want: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.RequestTimeout = time.Second
			if tt.cfg.MaxPayloadSize == 0 {
				tt.cfg.MaxPayloadSize = 1 << 20
			}
			published := make(chanPublisher, 1)
			mux := http.NewServeMux()
			Routes(mux, &tt.cfg, published)
			handler := mux

			method := tt.method
			if method == "" {
				method = "POST"
			}
			req := httptest.NewRequest(method, "/webhook", bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-GitHub-Event", "pull_request")
			req.Header.Set("X-GitHub-Delivery", "d-"+tt.name)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d (body %q)", rec.Code, tt.want, rec.Body.String())
			}
			if len(published) != 0 {
				t.Error("a rejected delivery was published")
			}
		})
	}
}

func TestHandlerAcknowledgesPingWithoutPublishing(t *testing.T) {
	published := make(chanPublisher, 1)
	handler := NewHandler(&config.WebhookConfig{MaxPayloadSize: 1 << 20, RequestTimeout: time.Second}, published)

	req := httptest.NewRequest("POST", "/webhook", bytes.NewBufferString(`{"zen":"Keep it logically awesome.","hook_id":1}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", "ping")
	req.Header.Set("X-GitHub-Delivery", "ping-1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content type = %q", ct)
	}
	if len(published) != 0 {
		t.Error("ping was published as an event")
	}
}

func TestHandlerTimesOutSlowPublisher(t *testing.T) {
	cfg := &config.WebhookConfig{MaxPayloadSize: 1 << 20, RequestTimeout: 50 * time.Millisecond}
	handler := NewHandler(cfg, blockingPublisher{})

	req := httptest.NewRequest("POST", "/webhook", bytes.NewBufferString(`{"action":"opened"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", "slow-1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}

type blockingPublisher struct{}

func (blockingPublisher) Publish(ctx context.Context, e *events.GitHubEvent) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}
