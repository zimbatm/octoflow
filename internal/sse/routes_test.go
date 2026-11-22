package sse

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
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
			if data.Len() > 0 && cur.event != "connected" && cur.event != "error" {
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

func TestTypeRoute(t *testing.T) {
	f := newFixture(t)
	got, _ := f.read("/events/type/issues?last=10", "", 1)
	if want := "d6"; strings.Join(ids(got), " ") != want {
		t.Errorf("type issues = %v, want %s", ids(got), want)
	}
	got, _ = f.read("/events/type/issues?last=10&type=push,issues", "", 1)
	if want := "d6"; strings.Join(ids(got), " ") != want {
		t.Errorf("type issues, listed in ?type= = %v, want %s", ids(got), want)
	}
	if _, code := f.read("/events/type/issues?type=push", "", 0); code != http.StatusBadRequest {
		t.Errorf("type route with another ?type=: status %d, want 400", code)
	}
}

func TestReplayLastNCountsOnlyTheRequestedTypes(t *testing.T) {
	f := newFixture(t)
	f.publish("d7", "1", "push")
	f.publish("d8", "1", "issues")
	f.publish("d9", "1", "push")
	f.publish("d10", "1", "push")
	// the two newest issues have three pushes between and after them
	for _, path := range []string{"/events/org/1", "/events/repo/7", "/events"} {
		got, _ := f.read(path+"?last=2&type=issues", "", 2)
		if want := "d6 d8"; strings.Join(ids(got), " ") != want {
			t.Errorf("%s last 2 issues = %v, want %s", path, ids(got), want)
		}
	}
	got, _ := f.read("/events/org/1?last=3&type=issues,push,issues", "", 3)
	if want := "d8 d9 d10"; strings.Join(ids(got), " ") != want {
		t.Errorf("org 1 last 3 of two types = %v, want %s", ids(got), want)
	}
}

func TestActionFiltersAfterTheReplayCount(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for _, id := range []string{"d7", "d8"} {
		ev := &events.GitHubEvent{ID: id, Event: "issues", OrganizationID: "1", Action: "closed", Payload: json.RawMessage(`{}`)}
		if _, err := f.pub.Publish(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	f.publish("d9", "1", "issues")
	// the last 2 issues are d8 (closed) and d9 (opened): d7 is not replayed
	got, _ := f.read("/events/org/1?last=2&type=issues&action=closed", "", 1)
	if want := "d8"; strings.Join(ids(got), " ") != want {
		t.Errorf("closed among the last 2 issues = %v, want %s", ids(got), want)
	}
}

func TestSelectTypes(t *testing.T) {
	for _, tc := range []struct {
		subjects []string
		types    string
		want     string
	}{
		{[]string{"p.org.1.>"}, "", "p.org.1.>"},
		{[]string{"p.org.*.>", "p.global.>"}, "push", "p.org.*.push p.global.push"},
		{[]string{"p.user.2.*"}, "push,issues", "p.user.2.push p.user.2.issues"},
		{[]string{"p.pr.7.3.>"}, "pull_request", "p.pr.7.3.pull_request"},
		{[]string{"p.org.*.push", "p.global.push"}, "issues,push", "p.org.*.push p.global.push"},
	} {
		got, err := selectTypes(tc.subjects, tc.types)
		if err != nil || strings.Join(got, " ") != tc.want {
			t.Errorf("selectTypes(%v, %q) = %v, %v; want %s", tc.subjects, tc.types, got, err, tc.want)
		}
	}
}

func TestABadTypeIsRefused(t *testing.T) {
	f := newFixture(t)
	var many []string
	for i := 0; i <= maxTypes; i++ {
		many = append(many, "t"+strings.Repeat("a", i))
	}
	for _, q := range []string{"type=Push", "type=push,", "type=*", "type=push.%3E", "type=a%20b", "type=" + strings.Join(many, ",")} {
		if _, code := f.read("/events/org/1?"+q, "", 0); code != http.StatusBadRequest {
			t.Errorf("?%s: status %d, want 400", q, code)
		}
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

func TestLastEventIDResumesPastTheCap(t *testing.T) {
	old := maxLast
	maxLast = 2
	t.Cleanup(func() { maxLast = old })
	f := newFixture(t)
	got, _ := f.read("/events/org/1?last=1", strconv.FormatUint(f.seqs["d1"], 10), 3)
	if want := "d2 d3 d6"; strings.Join(ids(got), " ") != want {
		t.Errorf("resume after d1 with a cap of 2 = %v, want %s", ids(got), want)
	}
	got, _ = f.read("/events/org/1?last=4", "", 2)
	if want := "d3 d6"; strings.Join(ids(got), " ") != want {
		t.Errorf("last=4 with a cap of 2 = %v, want %s", ids(got), want)
	}
}

func TestLastEventIDOlderThanTheStreamStartsAtItsFirstMessage(t *testing.T) {
	f := newFixture(t)
	if err := f.js.PurgeStream("P", &nats.StreamPurgeRequest{Sequence: f.seqs["d3"]}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"0", "1"} {
		got, _ := f.read("/events/org/1", id, 2)
		if want := "d3 d6"; strings.Join(ids(got), " ") != want {
			t.Errorf("Last-Event-ID %s on a stream that starts at d3 = %v, want %s", id, ids(got), want)
		}
	}
}

func TestSinceATime(t *testing.T) {
	f := newFixture(t)
	time.Sleep(50 * time.Millisecond)
	between := time.Now().UTC().Format(time.RFC3339Nano)
	time.Sleep(50 * time.Millisecond)
	f.publish("d7", "1", "push")
	f.publish("d8", "1", "issues")

	// last=1 would give d8 only
	got, _ := f.read("/events/org/1?last=1&since="+between, "", 2)
	if want := "d7 d8"; strings.Join(ids(got), " ") != want {
		t.Errorf("since a time before d7 = %v, want %s", ids(got), want)
	}
	got, _ = f.read("/events/org/1?type=issues&since="+between, "", 1)
	if want := "d8"; strings.Join(ids(got), " ") != want {
		t.Errorf("issues since a time before d7 = %v, want %s", ids(got), want)
	}
}

func TestSinceADurationIsNotCapped(t *testing.T) {
	old := maxLast
	maxLast = 2
	t.Cleanup(func() { maxLast = old })
	f := newFixture(t)
	for _, since := range []string{"1h", "2d"} {
		got, _ := f.read("/events/org/1?last=1&since="+since, "", 4)
		if want := "d1 d2 d3 d6"; strings.Join(ids(got), " ") != want {
			t.Errorf("since=%s = %v, want %s", since, ids(got), want)
		}
	}
}

func TestLastEventIDComesBeforeSince(t *testing.T) {
	f := newFixture(t)
	got, _ := f.read("/events/org/1?since=1h&last=1", strconv.FormatUint(f.seqs["d2"], 10), 2)
	if want := "d3 d6"; strings.Join(ids(got), " ") != want {
		t.Errorf("Last-Event-ID of d2 with since=1h = %v, want %s", ids(got), want)
	}
}

func TestABadSinceIsRefused(t *testing.T) {
	f := newFixture(t)
	for _, since := range []string{"yesterday", "-5m", "0s", "2026-11-22", "99999999d", "5"} {
		if _, code := f.read("/events/org/1?since="+since, "", 0); code != http.StatusBadRequest {
			t.Errorf("since=%s: status %d, want 400", since, code)
		}
	}
}

func TestParseSince(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	for in, want := range map[string]time.Time{
		"90m":                       now.Add(-90 * time.Minute),
		"2d":                        now.Add(-48 * time.Hour),
		"2026-11-20T08:30:00Z":      time.Date(2026, 11, 20, 8, 30, 0, 0, time.UTC),
		"2026-11-20T09:30:00+01:00": time.Date(2026, 11, 20, 8, 30, 0, 0, time.UTC),
	} {
		if got, err := parseSince(in, now); err != nil || !got.Equal(want) {
			t.Errorf("parseSince(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
}

func TestShutdownEndsOpenStreams(t *testing.T) {
	f := newFixture(t)
	done := make(chan struct{})
	go func() {
		// asks for more events than exist, so only the server can end it
		f.read("/events/org/1?last=1", "", 100)
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for f.h.Clients() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("client did not connect")
		}
		time.Sleep(20 * time.Millisecond)
	}

	f.h.Shutdown()
	f.h.Shutdown()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream stayed open after Shutdown")
	}
	if n := f.consumerCount(); n != 0 {
		t.Errorf("%d consumers left on the stream, want 0", n)
	}
}

func TestStatsCountsStoredEventsWithoutCopies(t *testing.T) {
	f := newFixture(t)
	resp, err := http.Get(f.server.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var stats struct {
		TotalClients *int      `json:"total_clients"`
		EventsStored *uint64   `json:"events_stored"`
		StoredSince  time.Time `json:"stored_since"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats.TotalClients == nil || *stats.TotalClients != 0 {
		t.Errorf("total_clients = %v, want 0", stats.TotalClients)
	}
	// the fixture stores 6 events and a repo copy of each
	if stats.EventsStored == nil || *stats.EventsStored != 6 {
		t.Errorf("events_stored = %v, want 6: the copies must not count", stats.EventsStored)
	}
	if age := time.Since(stats.StoredSince); age < 0 || age > time.Minute {
		t.Errorf("stored_since = %v, want the store time of the first message", stats.StoredSince)
	}

	// the count is cached: a new event does not show at once
	f.publish("d7", "1", "push")
	if n, _, err := f.h.Stored([]string{"p.org.>", "p.global.>"}); err != nil || n != 6 {
		t.Errorf("Stored() right after = %d, %v; want the cached 6", n, err)
	}
}

func TestSiteRoute(t *testing.T) {
	f := newFixture(t)
	f.h.Site = config.SiteConfig{Title: "NixOS events", Streams: []config.SiteStream{{Label: "NixOS/nixpkgs", Path: "/events/repo/4542716"}}}
	resp, err := http.Get(f.server.URL + "/site")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var site config.SiteConfig
	if err := json.NewDecoder(resp.Body).Decode(&site); err != nil {
		t.Fatal(err)
	}
	if site.Title != "NixOS events" || len(site.Streams) != 1 || site.Streams[0].Path != "/events/repo/4542716" {
		t.Errorf("site = %+v", site)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Error("no CORS header: the page may be on another origin")
	}
}

func TestAClientThatLeavesDuringTheReplayLookupIsNotAnError(t *testing.T) {
	f := newFixture(t)
	var logged bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(old)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest("GET", "/events/org/1?last=1", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	f.server.Config.Handler.ServeHTTP(rec, req)

	if strings.Contains(logged.String(), "level=ERROR") {
		t.Errorf("a cancelled request was logged as an error:\n%s", logged.String())
	}
	if strings.Contains(rec.Body.String(), "event: error") {
		t.Errorf("an error event was written to a client that left:\n%s", rec.Body.String())
	}
	if n := f.consumerCount(); n != 0 {
		t.Errorf("%d consumers left on the stream, want 0", n)
	}
}

// raw connects and returns the data line of the first event.
func (f *fixture) raw(path string) string {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", f.server.URL+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	inEvent := false
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "id: ") {
			inEvent = true
		}
		if data, ok := strings.CutPrefix(sc.Text(), "data: "); ok && inEvent {
			return data
		}
	}
	f.t.Fatalf("%s: no event", path)
	return ""
}

func TestPayloadNoneLeavesThePayloadOut(t *testing.T) {
	f := newFixture(t)
	ev := &events.GitHubEvent{
		ID: "d7", Event: "push", Action: "opened", ReceivedAt: time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC),
		Repository: "o/r", RepositoryID: "7", Organization: "o", OrganizationID: "3", Sender: "s", SenderID: "9",
		Metadata: map[string]string{"source": "events-api"},
		Payload:  json.RawMessage(`{"big":"` + strings.Repeat("x", 20000) + `"}`),
	}
	if _, err := f.pub.Publish(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	stored, _ := json.Marshal(ev)

	for _, q := range []string{"", "&payload="} {
		if got := f.raw("/events/org/3?last=1" + q); got != string(stored) {
			t.Errorf("last=1%s: the event is not the stored message: %d bytes, want %d", q, len(got), len(stored))
		}
	}

	got := f.raw("/events/org/3?last=1&payload=none")
	var fields, want map[string]json.RawMessage
	if err := json.Unmarshal([]byte(got), &fields); err != nil {
		t.Fatalf("bad data %q: %v", got, err)
	}
	_ = json.Unmarshal(stored, &want)
	delete(want, "payload")
	if _, ok := fields["payload"]; ok {
		t.Errorf("payload=none has a payload field: %s", got)
	}
	for k, v := range want {
		if string(fields[k]) != string(v) {
			t.Errorf("payload=none: %s = %s, want %s", k, fields[k], v)
		}
	}
	if len(fields) != len(want) || len(want) != 11 {
		t.Errorf("payload=none has %d fields, the stored event %d without its payload, want 11: %s", len(fields), len(want), got)
	}
	if len(got) > 500 {
		t.Errorf("payload=none is %d bytes", len(got))
	}

	if _, code := f.read("/events/org/3?payload=small", "", 0); code != http.StatusBadRequest {
		t.Errorf("payload=small: status %d, want 400", code)
	}
}
