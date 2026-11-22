package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/natstest"
	"github.com/zimbatm/octoflow/internal/status"
	"github.com/zimbatm/octoflow/pkg/events"
)

func statusTotals(t *testing.T, js nats.JetStreamContext) (events, refused int64) {
	t.Helper()
	s, err := status.NewReader(js).Read(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for i := range s.Events {
		events += s.Events[i]
		refused += s.Refused[i]
	}
	return events, refused
}

// dispatchAll runs the dispatcher until it has handled every stored event,
// then stops it as a SIGTERM would.
func dispatchAll(t *testing.T, cfg *config.Config, js nats.JetStreamContext) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runDispatcher(ctx, cfg, js) }()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ci, err := js.ConsumerInfo(cfg.NATS.StreamName, "event-dispatcher")
		if err == nil && ci.NumPending == 0 && ci.NumAckPending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dispatcher did not catch up: %v, %+v", err, ci)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDispatcherCountsEventsAcrossARestart(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := &config.Config{}
	cfg.NATS = config.NATSConfig{SubjectPrefix: "p", StreamName: "P", StreamReplicas: 1, StreamMaxAge: time.Hour}
	if err := natsInternal.EnsureStream(js, &cfg.NATS); err != nil {
		t.Fatal(err)
	}
	pub := natsInternal.NewPublisher(js, &cfg.NATS)
	publish := func(from, to int) {
		for i := from; i < to; i++ {
			ev := &events.GitHubEvent{ID: fmt.Sprintf("d%d", i), Event: "push", OrganizationID: "1", RepositoryID: "7", SenderID: "9",
				Payload: json.RawMessage(`{}`)}
			if _, err := pub.Publish(context.Background(), ev); err != nil {
				t.Fatal(err)
			}
		}
	}

	publish(0, 3)
	dispatchAll(t, cfg, js)
	if n, _ := statusTotals(t, js); n != 3 {
		t.Fatalf("events counted = %d, want 3: the fan-out copies must not count", n)
	}

	// published while no dispatcher runs: counted when it is back
	publish(3, 5)
	dispatchAll(t, cfg, js)
	if n, _ := statusTotals(t, js); n != 5 {
		t.Errorf("events counted after a restart = %d, want 5", n)
	}
}

func TestWebhookCountsARefusedDelivery(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := &config.Config{}
	cfg.Webhook = config.WebhookConfig{ListenAddr: ":1", AllowUnsigned: true, MaxPayloadSize: 1 << 20, RequestTimeout: time.Second}
	cfg.NATS = config.NATSConfig{SubjectPrefix: "p", StreamName: "P", StreamReplicas: 1, StreamMaxAge: time.Hour}
	if err := natsInternal.EnsureStream(js, &cfg.NATS); err != nil {
		t.Fatal(err)
	}
	servers, err := httpServers(mustPick(t, "webhook", cfg), cfg, js)
	if err != nil {
		t.Fatal(err)
	}
	post := func(id, body string) int {
		req := httptest.NewRequest("POST", "/webhook", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-GitHub-Event", "push")
		req.Header.Set("X-GitHub-Delivery", id)
		rec := httptest.NewRecorder()
		servers[0].Handler.ServeHTTP(rec, req)
		return rec.Code
	}
	if code := post("ok", `{"organization":{"id":1,"login":"o"}}`); code != http.StatusOK {
		t.Fatalf("good delivery = %d", code)
	}
	if code := post("bad", `not json`); code != http.StatusBadRequest {
		t.Fatalf("bad delivery = %d", code)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		_, refused := statusTotals(t, js)
		if refused == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("refused deliveries counted = %d, want 1", refused)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
