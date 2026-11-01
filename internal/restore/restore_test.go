package restore

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/natstest"
	"github.com/zimbatm/octoflow/internal/storage"
	"github.com/zimbatm/octoflow/pkg/events"
)

// fakeStore holds archive files in memory and records what was read.
type fakeStore struct {
	files map[string][]byte
	gets  []string
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
	s.gets = append(s.gets, key)
	return data, nil
}

// archive encodes events into one file as the s3 service would at uploadedAt.
func (s *fakeStore) archive(t *testing.T, uploadedAt time.Time, evs ...*events.GitHubEvent) {
	t.Helper()
	var data []byte
	up := uploaderFunc(func(_ context.Context, key string, b []byte) error {
		data = b
		s.files[key] = b
		return nil
	})
	w := storage.NewWriter(&config.S3Config{KeyPrefix: "events", BatchSize: 1000, BatchTimeout: time.Hour,
		BatchMaxBytes: 1 << 30, CompressionType: "snappy", RowGroupSize: 100}, up)
	w.SetClock(func() time.Time { return uploadedAt })
	for _, ev := range evs {
		w.AddEvent(ev, func(err error) {
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	w.Flush()
	if data == nil {
		t.Fatal("nothing archived")
	}
}

type uploaderFunc func(ctx context.Context, key string, data []byte) error

func (f uploaderFunc) Upload(ctx context.Context, key string, data []byte) error {
	return f(ctx, key, data)
}

type recorder struct {
	ids  []string
	dups map[string]bool
}

func (r *recorder) Publish(_ context.Context, ev *events.GitHubEvent) (bool, error) {
	r.ids = append(r.ids, ev.ID)
	return r.dups[ev.ID], nil
}

func at(day int, hour int) time.Time {
	return time.Date(2026, 10, day, hour, 0, 0, 0, time.UTC)
}

func ev(id, typ, repo string, received time.Time) *events.GitHubEvent {
	return &events.GitHubEvent{ID: id, Event: typ, RepositoryID: repo, OrganizationID: "9",
		ReceivedAt: received, Payload: json.RawMessage(`{"n":1}`)}
}

func newStore(t *testing.T) *fakeStore {
	s := &fakeStore{files: map[string][]byte{}}
	// day 13: two events; day 14: one event received late on the 13th, one on the 14th; day 15: one
	s.archive(t, at(13, 10), ev("a", "push", "1", at(13, 9)), ev("b", "issues", "2", at(13, 9)))
	s.archive(t, at(14, 0).Add(2*time.Minute), ev("c", "push", "1", at(13, 23).Add(59*time.Minute)), ev("d", "push", "1", at(14, 1)))
	s.archive(t, at(15, 10), ev("e", "push", "1", at(15, 9)))
	return s
}

func TestRunSelectsByReceivedDateAcrossFileDays(t *testing.T) {
	s := newStore(t)
	rec := &recorder{}
	st, err := Run(context.Background(), s, rec, Options{KeyPrefix: "events", From: at(13, 0), To: at(13, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(rec.ids, " "); got != "a b c" {
		t.Errorf("published %q, want a b c: c was received on the 13th but uploaded on the 14th", got)
	}
	if st.Files != 2 || st.Rows != 4 || st.Matched != 3 || st.Published != 3 || st.Duplicates != 0 {
		t.Errorf("stats = %+v", st)
	}
}

func TestRunFilters(t *testing.T) {
	s := newStore(t)
	rec := &recorder{}
	_, err := Run(context.Background(), s, rec, Options{KeyPrefix: "events", From: at(13, 0), To: at(15, 0),
		Events: []string{"push"}, RepoID: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(rec.ids, " "); got != "a c d e" {
		t.Errorf("published %q, want the pushes of repo 1", got)
	}

	rec = &recorder{}
	_, _ = Run(context.Background(), s, rec, Options{KeyPrefix: "events", From: at(13, 0), To: at(15, 0), OrgID: "other"})
	if len(rec.ids) != 0 {
		t.Errorf("published %v for an organization with no events", rec.ids)
	}
}

func TestRunDryRunReadsButDoesNotPublish(t *testing.T) {
	s := newStore(t)
	st, err := Run(context.Background(), s, nil, Options{KeyPrefix: "events", From: at(13, 0), To: at(15, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if st.Matched != 5 || st.Published != 0 || len(s.gets) != 3 {
		t.Errorf("stats = %+v, files read = %d", st, len(s.gets))
	}
}

func TestRunCountsDuplicates(t *testing.T) {
	s := newStore(t)
	rec := &recorder{dups: map[string]bool{"a": true}}
	st, err := Run(context.Background(), s, rec, Options{KeyPrefix: "events", From: at(13, 0), To: at(13, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if st.Published != 2 || st.Duplicates != 1 {
		t.Errorf("stats = %+v, want 2 published and 1 duplicate", st)
	}
}

func TestRunIntoTheStream(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := &config.NATSConfig{SubjectPrefix: "p", StreamName: "P", StreamReplicas: 1, StreamMaxAge: 48 * time.Hour}
	if err := natsInternal.EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	pub := natsInternal.NewPublisher(js, cfg)
	s := newStore(t)
	opts := Options{KeyPrefix: "events", From: at(13, 0), To: at(15, 0)}

	st, err := Run(context.Background(), s, pub, opts)
	if err != nil {
		t.Fatal(err)
	}
	if st.Published != 5 || st.Duplicates != 0 {
		t.Fatalf("first run: %+v", st)
	}
	st, err = Run(context.Background(), s, pub, opts)
	if err != nil {
		t.Fatal(err)
	}
	if st.Published != 0 || st.Duplicates != 5 {
		t.Errorf("second run: %+v, want every event dropped as a duplicate", st)
	}

	info, err := js.StreamInfo("P")
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 5 {
		t.Errorf("stream holds %d messages, want 5", info.State.Msgs)
	}
	msg, err := js.GetMsg("P", 1)
	if err != nil {
		t.Fatal(err)
	}
	var restored events.GitHubEvent
	if err := json.Unmarshal(msg.Data, &restored); err != nil {
		t.Fatal(err)
	}
	if msg.Subject != "p.org.9.push" || restored.ID != "a" || !restored.ReceivedAt.Equal(at(13, 9)) {
		t.Errorf("restored message: subject %s, %+v", msg.Subject, restored)
	}
}
