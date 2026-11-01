package publicingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/ghevents"
	"github.com/zimbatm/octoflow/internal/natstest"
	"github.com/zimbatm/octoflow/pkg/events"
)

type recorder struct {
	mu     sync.Mutex
	ids    []string
	onEach func(n int)
}

func (r *recorder) Publish(_ context.Context, e *events.GitHubEvent) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, e.ID)
	if r.onEach != nil {
		r.onEach(len(r.ids))
	}
	return false, nil
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}

func apiEvent(id int64) string {
	return fmt.Sprintf(`{"id":"%d","type":"PushEvent","actor":{"id":1,"login":"a"},"repo":{"id":5,"name":"o/r"},"payload":{},"created_at":"2026-10-14T13:07:00Z"}`, id)
}

// eventsAPI serves /repos/o/r/events: newest first on page 1, nothing after.
type eventsAPI struct {
	mu   sync.Mutex
	ids  []int64
	etag string
}

func (a *eventsAPI) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/o/r/events" {
			http.NotFound(w, r)
			return
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		w.Header().Set("X-Poll-Interval", "60")
		w.Header().Set("ETag", a.etag)
		if r.Header.Get("If-None-Match") == a.etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if r.URL.Query().Get("page") != "1" {
			_, _ = w.Write([]byte("[]"))
			return
		}
		var items []string
		for _, id := range a.ids {
			items = append(items, apiEvent(id))
		}
		_, _ = w.Write([]byte("[" + strings.Join(items, ",") + "]"))
	}
}

func newState(t *testing.T) nats.KeyValue {
	t.Helper()
	_, js := natstest.Start(t)
	kv, err := OpenState(js, 1)
	if err != nil {
		t.Fatal(err)
	}
	return kv
}

func TestPollRepoPublishesNewEventsInOrder(t *testing.T) {
	api := &eventsAPI{ids: []int64{103, 102, 101}, etag: "v1"}
	srv := httptest.NewServer(api.handler(t))
	defer srv.Close()

	gh := ghevents.NewClient("")
	gh.BaseURL = srv.URL
	rec := &recorder{}
	in := &Ingest{GH: gh, Publisher: rec, State: newState(t)}
	repo := &ghevents.RepoInfo{ID: 5, FullName: "o/r", Owner: ghevents.Account{ID: 9, Login: "o"}}
	etags := map[int64]string{}
	ctx := context.Background()

	interval, err := in.PollRepo(ctx, repo, etags)
	if err != nil {
		t.Fatal(err)
	}
	if interval != time.Minute {
		t.Errorf("poll interval = %v, want 1m from X-Poll-Interval", interval)
	}
	if got := strings.Join(rec.list(), " "); got != "gh-event-101 gh-event-102 gh-event-103" {
		t.Errorf("published %s, want oldest first", got)
	}
	entry, err := in.State.Get("events-api.5")
	if err != nil || string(entry.Value()) != "103" {
		t.Errorf("saved progress = %q, %v; want 103", entry.Value(), err)
	}

	// nothing new: the etag matches and GitHub answers 304.
	if _, err := in.PollRepo(ctx, repo, etags); err != nil {
		t.Fatal(err)
	}
	if len(rec.list()) != 3 {
		t.Errorf("a 304 poll published %d events", len(rec.list())-3)
	}

	// one new event: only it is published, even though old ones are listed again.
	api.mu.Lock()
	api.ids = []int64{104, 103, 102, 101}
	api.etag = "v2"
	api.mu.Unlock()
	if _, err := in.PollRepo(ctx, repo, etags); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(rec.list(), " "); got != "gh-event-101 gh-event-102 gh-event-103 gh-event-104" {
		t.Errorf("published %s, want only 104 added", got)
	}
}

func TestPollRepoResumesFromSavedProgress(t *testing.T) {
	api := &eventsAPI{ids: []int64{103, 102, 101}, etag: "v1"}
	srv := httptest.NewServer(api.handler(t))
	defer srv.Close()

	gh := ghevents.NewClient("")
	gh.BaseURL = srv.URL
	rec := &recorder{}
	state := newState(t)
	if _, err := state.PutString("events-api.5", "102"); err != nil {
		t.Fatal(err)
	}
	in := &Ingest{GH: gh, Publisher: rec, State: state}
	repo := &ghevents.RepoInfo{ID: 5, FullName: "o/r"}

	if _, err := in.PollRepo(context.Background(), repo, map[int64]string{}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(rec.list(), " "); got != "gh-event-103" {
		t.Errorf("published %s, want only the event after 102", got)
	}
}

func gzipLines(t *testing.T, lines ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write([]byte(strings.Join(lines, "\n") + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestRunGHArchiveImportsTheHourAndSavesProgress(t *testing.T) {
	hour := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	file := gzipLines(t,
		`{"id":"201","type":"IssuesEvent","actor":{"id":1,"login":"a"},"repo":{"id":5,"name":"o/r"},"payload":{"action":"opened"},"created_at":"2026-10-14T09:10:00Z"}`,
		`{"id":"202","type":"PushEvent","actor":{"id":1,"login":"a"},"repo":{"id":6,"name":"other/repo"},"payload":{},"created_at":"2026-10-14T09:11:00Z"}`,
		`{"id":"203","type":"WatchEvent","actor":{"id":2,"login":"b"},"repo":{"id":5,"name":"o/r"},"payload":{"action":"started"},"created_at":"2026-10-14T09:12:00Z"}`,
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/2026-10-14-9.json.gz" {
			_, _ = w.Write(file)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rec := &recorder{onEach: func(n int) {
		if n == 2 {
			cancel()
		}
	}}
	in := &Ingest{
		Publisher: rec,
		State:     newState(t),
		Repos:     []*ghevents.RepoInfo{{ID: 5, FullName: "o/r", Owner: ghevents.Account{ID: 9, Login: "o"}}},
		HTTP:      srv.Client(),
		Now:       func() time.Time { return hour.Add(3 * time.Hour) },
	}

	in.RunGHArchive(ctx, srv.URL, hour)

	if got := strings.Join(rec.list(), " "); got != "gh-event-201 gh-event-203" {
		t.Errorf("published %s, want the two events of repo 5", got)
	}
	entry, err := in.State.Get("gharchive.next_hour")
	if err != nil {
		t.Fatal(err)
	}
	if want := hour.Add(time.Hour).Format(time.RFC3339); string(entry.Value()) != want {
		t.Errorf("next hour = %s, want %s", entry.Value(), want)
	}
}

func TestRunGHArchiveStartsFromSavedProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var mu sync.Mutex
	var requested []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requested = append(requested, r.URL.Path)
		mu.Unlock()
		http.NotFound(w, r)
		// the hour is "not published yet", so the loop would sleep ten minutes.
		cancel()
	}))
	defer srv.Close()

	state := newState(t)
	saved := time.Date(2026, 10, 14, 11, 0, 0, 0, time.UTC)
	if _, err := state.PutString("gharchive.next_hour", saved.Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	in := &Ingest{
		Publisher: &recorder{},
		State:     state,
		HTTP:      srv.Client(),
		Now:       func() time.Time { return saved.Add(2 * time.Hour) },
	}
	in.RunGHArchive(ctx, srv.URL, saved.Add(-5*time.Hour))

	mu.Lock()
	defer mu.Unlock()

	if len(requested) != 1 || requested[0] != "/2026-10-14-11.json.gz" {
		t.Errorf("requested %v, want only the saved hour", requested)
	}
}

var _ = json.Marshal

func TestPollRepoWaitsForTheRateLimitReset(t *testing.T) {
	reset := time.Now().Add(90 * time.Second)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", fmt.Sprint(reset.Unix()))
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	gh := ghevents.NewClient("")
	gh.BaseURL = srv.URL
	rec := &recorder{}
	in := &Ingest{GH: gh, Publisher: rec, State: newState(t)}

	wait, err := in.PollRepo(context.Background(), &ghevents.RepoInfo{ID: 5, FullName: "o/r"}, map[int64]string{})
	if err != nil {
		t.Fatalf("a rate limit is not an error: %v", err)
	}
	if wait < 90*time.Second || wait > 2*time.Minute {
		t.Errorf("wait = %v, want about 100s until the reset", wait)
	}
	if len(rec.list()) != 0 {
		t.Error("published under a rate limit")
	}
}

func TestPollRepoSlowsDownWhenTheQuotaIsNearlyUsed(t *testing.T) {
	reset := time.Now().Add(5 * time.Minute)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Poll-Interval", "60")
		w.Header().Set("X-RateLimit-Remaining", "2")
		w.Header().Set("X-RateLimit-Reset", fmt.Sprint(reset.Unix()))
		if r.URL.Query().Get("page") != "1" {
			_, _ = w.Write([]byte("[]"))
			return
		}
		_, _ = w.Write([]byte("[" + apiEvent(101) + "]"))
	}))
	defer srv.Close()

	gh := ghevents.NewClient("")
	gh.BaseURL = srv.URL
	rec := &recorder{}
	in := &Ingest{GH: gh, Publisher: rec, State: newState(t)}

	wait, err := in.PollRepo(context.Background(), &ghevents.RepoInfo{ID: 5, FullName: "o/r"}, map[int64]string{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.list()) != 1 {
		t.Errorf("published %v, want the one event", rec.list())
	}
	if wait < 5*time.Minute {
		t.Errorf("wait = %v, want until the reset, not the 60s poll interval", wait)
	}
}
