package sse

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/natstest"
	"github.com/zimbatm/octoflow/internal/status"
)

type statusBody struct {
	Now        time.Time  `json:"now"`
	Start      time.Time  `json:"start"`
	Step       int        `json:"step"`
	Events     []int64    `json:"events"`
	Refused    []int64    `json:"refused"`
	LastEvent  *time.Time `json:"last_event"`
	Events1h   int64      `json:"events_1h"`
	Events24h  int64      `json:"events_24h"`
	Refused1h  int64      `json:"refused_1h"`
	Refused24h int64      `json:"refused_24h"`
}

func getStatus(t *testing.T, url string) (statusBody, *http.Response, string) {
	t.Helper()
	resp, err := http.Get(url + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var s statusBody
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("status body: %v: %s", err, raw)
	}
	return s, resp, string(raw)
}

func TestStatus(t *testing.T) {
	f := newFixture(t)
	now := time.Now()
	c := status.NewCounter(f.js, 1)
	c.Add(status.Events, now.Add(-3*time.Hour), 40)
	c.Add(status.Events, now.Add(-10*time.Minute), 6)
	c.Add(status.Refused, now.Add(-10*time.Minute), 2)
	c.Add(status.Refused, now.Add(-5*time.Hour), 1)
	c.Close()

	s, resp, raw := getStatus(t, f.server.URL)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
		t.Errorf("status %d, content type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Error("no CORS header on /status")
	}
	if s.Step != 60 || len(s.Events) != 1440 || len(s.Refused) != 1440 {
		t.Fatalf("step %d, %d events, %d refused; want 60, 1440, 1440", s.Step, len(s.Events), len(s.Refused))
	}
	if end := s.Start.Add(1439 * time.Minute); now.Sub(end) < 0 || now.Sub(end) > 2*time.Minute {
		t.Errorf("the series ends at %v, want the current minute", end)
	}
	i := int(now.Add(-10*time.Minute).UTC().Truncate(time.Minute).Sub(s.Start) / time.Minute)
	if s.Events[i] != 6 || s.Refused[i] != 2 {
		t.Errorf("10 minutes ago: %d events, %d refused; want 6, 2", s.Events[i], s.Refused[i])
	}
	if s.Events1h != 6 || s.Events24h != 46 || s.Refused1h != 2 || s.Refused24h != 3 {
		t.Errorf("totals = %d %d %d %d, want 6 46 2 3", s.Events1h, s.Events24h, s.Refused1h, s.Refused24h)
	}
	// the fixture stored its events just now
	if s.LastEvent == nil || now.Sub(*s.LastEvent) > time.Minute || s.Now.Sub(*s.LastEvent) < 0 {
		t.Errorf("last_event = %v, now = %v", s.LastEvent, s.Now)
	}

	// many pages ask; within the cache time nobody reads the bucket again
	c = status.NewCounter(f.js, 1)
	c.Add(status.Events, now, 100)
	c.Close()
	if _, _, again := getStatus(t, f.server.URL); again != raw {
		t.Error("the second answer was not the cached one")
	}

	req, _ := http.NewRequest(http.MethodOptions, f.server.URL+"/status", nil)
	pre, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	pre.Body.Close()
	if pre.StatusCode != http.StatusNoContent || pre.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("preflight: status %d, origin %q", pre.StatusCode, pre.Header.Get("Access-Control-Allow-Origin"))
	}
}

func TestStatusOnAnEmptyStream(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := &config.NATSConfig{SubjectPrefix: "p", StreamName: "P", StreamReplicas: 1, StreamMaxAge: time.Hour}
	if err := natsInternal.EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	Routes(mux, NewHandler(js, "P", 0), "p")
	server := httptest.NewServer(mux)
	defer server.Close()

	s, resp, raw := getStatus(t, server.URL)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, raw)
	}
	if len(s.Events) != 1440 || len(s.Refused) != 1440 || s.Step != 60 || s.Start.IsZero() {
		t.Errorf("%d events, %d refused, step %d, start %v", len(s.Events), len(s.Refused), s.Step, s.Start)
	}
	if s.Events24h != 0 || s.Refused24h != 0 {
		t.Errorf("totals = %d, %d; want 0", s.Events24h, s.Refused24h)
	}
	if s.LastEvent != nil {
		t.Errorf("last_event = %v on a stream that never had an event", s.LastEvent)
	}
	if _, err := js.KeyValue(status.Bucket); err == nil {
		t.Error("the SSE server created the bucket; it only reads")
	}
}

func TestStatusWithoutAStream(t *testing.T) {
	mux := http.NewServeMux()
	Routes(mux, NewHandler(nil, "P", 0), "p")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/status", nil))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("status %d, origin %q", rec.Code, rec.Header().Get("Access-Control-Allow-Origin"))
	}
}
