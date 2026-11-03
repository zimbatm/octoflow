package wipe

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/dispatch"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/natstest"
	"github.com/zimbatm/octoflow/internal/storage"
	"github.com/zimbatm/octoflow/pkg/events"
)

func pr(id, repo string, number int, sender string, received time.Time) *events.GitHubEvent {
	payload := `{"pull_request":{"number":` + itoa(number) + `,"user":{"id":` + sender + `}},"sender":{"id":` + sender + `}}`
	return &events.GitHubEvent{ID: id, Event: "pull_request", RepositoryID: repo, OrganizationID: "9",
		SenderID: sender, ReceivedAt: received, Payload: json.RawMessage(payload)}
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestMatches(t *testing.T) {
	day := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	ev := pr("d1", "7", 42, "1", day)
	cases := []struct {
		name string
		opts Options
		want bool
	}{
		{"id", Options{IDs: []string{"d1"}}, true},
		{"other id", Options{IDs: []string{"d2"}}, false},
		{"type", Options{Events: []string{"push", "pull_request"}}, true},
		{"repo and pr", Options{RepoID: "7", PR: 42}, true},
		{"repo and other pr", Options{RepoID: "7", PR: 43}, false},
		{"issue number does not match a pr", Options{Issue: 42}, false},
		{"user as sender", Options{UserID: "1"}, true},
		{"other user", Options{UserID: "2"}, false},
		{"org", Options{OrgID: "9"}, true},
		{"day", Options{From: day, To: day}, true},
		{"day before", Options{From: day.AddDate(0, 0, -2), To: day.AddDate(0, 0, -1)}, false},
		{"from only", Options{From: day.AddDate(0, 0, -1)}, true},
		{"all fields must match", Options{IDs: []string{"d1"}, RepoID: "8"}, false},
	}
	for _, c := range cases {
		if got := c.opts.Matches(ev); got != c.want {
			t.Errorf("%s: matches = %v, want %v", c.name, got, c.want)
		}
	}
	if !(&Options{}).Empty() || (&Options{PR: 1}).Empty() {
		t.Error("Empty is wrong")
	}
}

func TestStreamDeletesTheEventAndItsCopies(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := &config.NATSConfig{SubjectPrefix: "p", StreamName: "P", StreamReplicas: 1, StreamMaxAge: time.Hour}
	if err := natsInternal.EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	pub := natsInternal.NewPublisher(js, cfg)
	ctx := context.Background()
	now := time.Now().UTC()
	for _, ev := range []*events.GitHubEvent{pr("d1", "7", 42, "1", now), pr("d2", "7", 43, "2", now), pr("d3", "8", 1, "1", now)} {
		if _, err := pub.Publish(ctx, ev); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(ev)
		if _, err := dispatch.Dispatch(ctx, pub, "p", data); err != nil {
			t.Fatal(err)
		}
	}
	// each event: 1 primary + repo + user + pr copies
	before, _ := js.StreamInfo("P")
	if before.State.Msgs != 12 {
		t.Fatalf("stream holds %d messages, want 12", before.State.Msgs)
	}

	st, err := Stream(ctx, js, "P", "p", Options{RepoID: "7", PR: 42}, true)
	if err != nil {
		t.Fatal(err)
	}
	if st.Scanned != 12 || st.Matched != 4 || st.Events != 1 || st.Deleted != 0 {
		t.Errorf("dry run: %+v", st)
	}
	after, _ := js.StreamInfo("P")
	if after.State.Msgs != 12 {
		t.Errorf("a dry run deleted %d messages", 12-after.State.Msgs)
	}

	st, err = Stream(ctx, js, "P", "p", Options{UserID: "1"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if st.Matched != 8 || st.Events != 2 || st.Deleted != 8 {
		t.Errorf("wipe by user: %+v", st)
	}
	after, _ = js.StreamInfo("P")
	if after.State.Msgs != 4 {
		t.Errorf("stream holds %d messages after the wipe, want the 4 of d2", after.State.Msgs)
	}
	st, err = Stream(ctx, js, "P", "p", Options{IDs: []string{"d2"}}, true)
	if err != nil || st.Scanned != 4 || st.Matched != 4 {
		t.Errorf("after the wipe only d2 is left: %+v, %v", st, err)
	}
}

func TestStreamOnAnEmptyStream(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := &config.NATSConfig{SubjectPrefix: "p", StreamName: "P", StreamReplicas: 1, StreamMaxAge: time.Hour}
	if err := natsInternal.EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	st, err := Stream(context.Background(), js, "P", "p", Options{IDs: []string{"x"}}, false)
	if err != nil || st.Scanned != 0 {
		t.Errorf("empty stream: %+v, %v", st, err)
	}
}

type fakeStore struct {
	files map[string][]byte
}

func (s *fakeStore) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	for k := range s.files {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *fakeStore) Get(_ context.Context, key string) ([]byte, error) {
	data, ok := s.files[key]
	if !ok {
		return nil, errors.New("no such key")
	}
	return data, nil
}

func (s *fakeStore) Put(_ context.Context, key string, data []byte) error {
	s.files[key] = data
	return nil
}

func (s *fakeStore) Delete(_ context.Context, key string) error {
	delete(s.files, key)
	return nil
}

func TestArchiveRewritesAndRemovesFiles(t *testing.T) {
	cfg := &config.S3Config{KeyPrefix: "events", CompressionType: "snappy", RowGroupSize: 100}
	at := func(day, hour int) time.Time { return time.Date(2026, 11, day, hour, 0, 0, 0, time.UTC) }
	store := &fakeStore{files: map[string][]byte{}}
	put := func(day int, evs ...*events.GitHubEvent) string {
		data, err := storage.EncodeParquet(cfg, evs)
		if err != nil {
			t.Fatal(err)
		}
		key := storage.DayPrefix("events", at(day, 0)) + "1.parquet"
		store.files[key] = data
		return key
	}
	mixed := put(3, pr("a", "7", 1, "1", at(3, 9)), pr("b", "7", 2, "2", at(3, 10)))
	only := put(4, pr("c", "7", 3, "1", at(4, 9)))
	other := put(5, pr("d", "8", 1, "3", at(5, 9)))
	ctx := context.Background()

	st, err := Archive(ctx, store, cfg, Options{UserID: "1"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if st.Files != 3 || st.Rows != 4 || st.Matched != 2 || st.Rewritten != 0 || st.Removed != 0 {
		t.Errorf("dry run: %+v", st)
	}
	if len(store.files) != 3 {
		t.Error("a dry run changed the archive")
	}

	st, err = Archive(ctx, store, cfg, Options{UserID: "1", From: at(3, 0), To: at(4, 0)}, false)
	if err != nil {
		t.Fatal(err)
	}
	if st.Matched != 2 || st.Rewritten != 1 || st.Removed != 1 {
		t.Errorf("wipe: %+v", st)
	}
	if _, ok := store.files[only]; ok {
		t.Error("a file left without rows must be removed")
	}
	if _, ok := store.files[other]; !ok {
		t.Error("a file outside the days must stay")
	}
	left, err := storage.DecodeParquet(store.files[mixed])
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 || left[0].ID != "b" || string(left[0].Payload) == "" {
		t.Errorf("rewritten file holds %+v, want only b with its payload", left)
	}
}
