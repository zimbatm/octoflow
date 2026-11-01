package sse

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/natstest"
	"github.com/zimbatm/octoflow/pkg/events"
)

type sseEvent struct {
	id, event string
	data      events.GitHubEvent
}

type fixture struct {
	t      *testing.T
	js     nats.JetStreamContext
	pub    *natsInternal.Publisher
	h      *Handler
	server *httptest.Server
	seqs   map[string]uint64
}

// newFixture fills the stream with primary events and fan-out copies, so
// that matching messages are interleaved with others like in production:
//
//	org 1: push d1, d2, d3   org 2: push d4, d5   org 1: issues d6
//
// Every primary event also gets a repo.<id> copy.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	_, js := natstest.Start(t)
	cfg := &config.NATSConfig{SubjectPrefix: "p", StreamName: "P", StreamReplicas: 1, StreamMaxAge: time.Hour}
	if err := natsInternal.EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	pub := natsInternal.NewPublisher(js, cfg)
	f := &fixture{t: t, js: js, pub: pub, seqs: map[string]uint64{}}

	add := func(id, org, typ string) {
		f.publish(id, org, typ)
	}
	add("d1", "1", "push")
	add("d2", "1", "push")
	add("d3", "1", "push")
	add("d4", "2", "push")
	add("d5", "2", "push")
	add("d6", "1", "issues")

	f.h = NewHandler(js, "P", 0)
	mux := http.NewServeMux()
	Routes(mux, f.h, "p")
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func (f *fixture) publish(id, org, typ string) {
	f.t.Helper()
	ctx := context.Background()
	ev := &events.GitHubEvent{ID: id, Event: typ, OrganizationID: org, RepositoryID: "7", Action: "opened",
		Payload: json.RawMessage(`{}`)}
	if _, err := f.pub.Publish(ctx, ev); err != nil {
		f.t.Fatal(err)
	}
	data, _ := json.Marshal(ev)
	if err := f.pub.PublishToSubject(ctx, "p.repo.7."+typ, typ, data, id+"@repo"); err != nil {
		f.t.Fatal(err)
	}
	// the primary message is the one before the copy.
	f.seqs[id] = uint64(2*len(f.seqs) + 1)
}

// read connects and returns the first n events, then disconnects.
func (f *fixture) read(path string, lastEventID string, n int) ([]sseEvent, int) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", f.server.URL+path, nil)
	if err != nil {
		f.t.Fatal(err)
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode
	}

	var got []sseEvent
	var cur sseEvent
	var data strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for len(got) < n && sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if cur.event != "" && cur.event != "connected" {
				if err := json.Unmarshal([]byte(data.String()), &cur.data); err != nil {
					f.t.Fatalf("bad event data %q: %v", data.String(), err)
				}
				got = append(got, cur)
			}
			cur = sseEvent{}
			data.Reset()
		case strings.HasPrefix(line, "id: "):
			cur.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			cur.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data.WriteString(strings.TrimPrefix(line, "data: "))
		}
	}
	return got, resp.StatusCode
}

func ids(evs []sseEvent) []string {
	var out []string
	for _, e := range evs {
		out = append(out, e.data.ID)
	}
	return out
}

func TestReplayLastNCountsOnlyMatchingMessages(t *testing.T) {
	f := newFixture(t)
	got, _ := f.read("/events/org/1?last=2", "", 2)
	if want := "d3 d6"; strings.Join(ids(got), " ") != want {
		t.Errorf("org 1 last 2 = %v, want %s", ids(got), want)
	}
	for _, e := range got {
		if e.id != strconv.FormatUint(f.seqs[e.data.ID], 10) {
			t.Errorf("event %s has id %s, want its stream sequence %d", e.data.ID, e.id, f.seqs[e.data.ID])
		}
	}
}

func TestReplayWithMoreThanStoredStartsAtTheBeginning(t *testing.T) {
	f := newFixture(t)
	got, _ := f.read("/events/org/2?last=50", "", 2)
	if want := "d4 d5"; strings.Join(ids(got), " ") != want {
		t.Errorf("org 2 = %v, want %s", ids(got), want)
	}
}

func TestFirehoseSkipsFanOutCopies(t *testing.T) {
	f := newFixture(t)
	got, _ := f.read("/events?last=100", "", 6)
	if want := "d1 d2 d3 d4 d5 d6"; strings.Join(ids(got), " ") != want {
		t.Errorf("firehose = %v, want %s", ids(got), want)
	}
}

func TestResumeFromLastEventID(t *testing.T) {
	f := newFixture(t)
	got, _ := f.read("/events/org/1", strconv.FormatUint(f.seqs["d2"], 10), 2)
	if want := "d3 d6"; strings.Join(ids(got), " ") != want {
		t.Errorf("resume after d2 = %v, want %s", ids(got), want)
	}
}

func TestTypeRouteAndQueryFilter(t *testing.T) {
	f := newFixture(t)
	got, _ := f.read("/events/type/issues?last=10", "", 1)
	if want := "d6"; strings.Join(ids(got), " ") != want {
		t.Errorf("type issues = %v, want %s", ids(got), want)
	}
	got, _ = f.read("/events/org/1?last=10&type=issues", "", 1)
	if want := "d6"; strings.Join(ids(got), " ") != want {
		t.Errorf("org 1 filtered to issues = %v, want %s", ids(got), want)
	}
}

func TestRepoRouteReadsTheCopies(t *testing.T) {
	f := newFixture(t)
	got, _ := f.read("/events/repo/7?last=2", "", 2)
	if want := "d5 d6"; strings.Join(ids(got), " ") != want {
		t.Errorf("repo 7 last 2 = %v, want %s", ids(got), want)
	}
}

func TestLiveEventsArriveWithoutReplay(t *testing.T) {
	f := newFixture(t)
	done := make(chan []sseEvent, 1)
	go func() {
		got, _ := f.read("/events/org/2?last=0", "", 1)
		done <- got
	}()
	// let the subscription exist before the new event is published.
	time.Sleep(300 * time.Millisecond)
	f.publish("d7", "2", "push")
	select {
	case got := <-done:
		if want := "d7"; strings.Join(ids(got), " ") != want {
			t.Errorf("live = %v, want %s", ids(got), want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no live event received")
	}
}

func TestNonNumericIDsAreNotRoutes(t *testing.T) {
	f := newFixture(t)
	for _, path := range []string{"/events/org/abc", "/events/repo/*", "/events/pr/7/x", "/events/type/Push>"} {
		if _, code := f.read(path, "", 0); code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404", path, code)
		}
	}
}

func TestStatsAndPreflight(t *testing.T) {
	f := newFixture(t)
	for _, path := range []string{"/stats"} {
		resp, err := http.Get(f.server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
			t.Errorf("%s: status %d, content type %q", path, resp.StatusCode, resp.Header.Get("Content-Type"))
		}
		if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
			t.Errorf("%s: no CORS header", path)
		}
	}
	req, _ := http.NewRequest(http.MethodOptions, f.server.URL+"/events/org/1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("preflight status = %d, want 204", resp.StatusCode)
	}
}

func TestConsumersAreDeletedWhenClientsLeave(t *testing.T) {
	f := newFixture(t)
	f.read("/events/org/1?last=1", "", 1)
	f.read("/events/org/2?last=1", "", 1)

	// deletion happens after the request context ends; allow a moment.
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(f.server.URL + "/stats")
		if err != nil {
			t.Fatal(err)
		}
		var stats struct {
			TotalClients int `json:"total_clients"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&stats)
		resp.Body.Close()
		if stats.TotalClients == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d clients still registered", stats.TotalClients)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := f.consumerCount(); n != 0 {
		t.Errorf("%d consumers left on the stream, want 0", n)
	}
}

// consumerCount waits up to five seconds for the stream to have no consumers.
func (f *fixture) consumerCount() int {
	f.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := 0
		for range f.js.ConsumerNames("P") {
			n++
		}
		if n == 0 || time.Now().After(deadline) {
			return n
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestMaxClients(t *testing.T) {
	f := newFixture(t)
	f.h.MaxClients = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", f.server.URL+"/events?last=0", nil)
	first, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first stream: %d", first.StatusCode)
	}
	if _, code := f.read("/events/org/1?last=0", "", 0); code != http.StatusServiceUnavailable {
		t.Errorf("second stream with MaxClients=1: %d, want 503", code)
	}
	cancel()
	for deadline := time.Now().Add(5 * time.Second); f.h.Clients() > 0 && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	if _, code := f.read("/events/org/1", "", 1); code != http.StatusOK {
		t.Errorf("after the first client left: %d, want 200", code)
	}
}

func TestLastEventIDNeverReplaysPastTheCap(t *testing.T) {
	old := maxLast
	maxLast = 2
	t.Cleanup(func() { maxLast = old })
	f := newFixture(t)
	got, _ := f.read("/events/org/1", "0", 2)
	if ids := ids(got); strings.Join(ids, " ") != "d3 d6" {
		t.Errorf("Last-Event-ID 0 with a cap of 2 replayed %v, want the last two of org 1", ids)
	}
}
