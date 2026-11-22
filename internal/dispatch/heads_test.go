package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/natstest"
	"github.com/zimbatm/octoflow/pkg/events"
)

const (
	shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func stored(t *testing.T, id, eventType, payload string) []byte {
	t.Helper()
	data, err := json.Marshal(&events.GitHubEvent{
		ID:             id,
		Event:          eventType,
		RepositoryID:   "7",
		OrganizationID: "9",
		SenderID:       "1",
		Payload:        json.RawMessage(payload),
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func pullRequest(t *testing.T, id string, number int, sha string) []byte {
	return stored(t, id, "pull_request", fmt.Sprintf(`{"action":"synchronize","pull_request":{"number":%d,"head":{"sha":%q},"user":{"id":1}}}`, number, sha))
}

func checkRun(t *testing.T, id, sha, pullRequests string) []byte {
	return stored(t, id, "check_run", fmt.Sprintf(`{"check_run":{"head_sha":%q,"pull_requests":%s}}`, sha, pullRequests))
}

type countingKV struct {
	nats.KeyValue
	gets atomic.Int64
	fail error
}

func (c *countingKV) Get(key string) (nats.KeyValueEntry, error) {
	c.gets.Add(1)
	if c.fail != nil {
		return nil, c.fail
	}
	return c.KeyValue.Get(key)
}

func (c *countingKV) Create(key string, value []byte) (uint64, error) {
	if c.fail != nil {
		return 0, c.fail
	}
	return c.KeyValue.Create(key, value)
}

type syncRecorder struct {
	mu       sync.Mutex
	subjects []string
}

func (r *syncRecorder) PublishToSubject(_ context.Context, subject, _ string, _ []byte, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subjects = append(r.subjects, subject)
	return nil
}

func (r *syncRecorder) count(subject string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, s := range r.subjects {
		if s == subject {
			n++
		}
	}
	return n
}

func (r *syncRecorder) list() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.subjects, " ")
}

func openHeads(t *testing.T, js nats.JetStreamContext) (*Heads, *countingKV) {
	t.Helper()
	h, err := OpenHeads(js, 1, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	kv := &countingKV{KeyValue: h.kv}
	h.kv = kv
	return h, kv
}

func mustDispatch(t *testing.T, h *Heads, pub Publisher, data []byte, skip ...string) int {
	t.Helper()
	n, err := h.Dispatch(context.Background(), pub, "p", data, skip)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestHeadsLinksCIEventsByTheirHeadCommit(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := &config.NATSConfig{SubjectPrefix: "p", StreamName: "P", StreamReplicas: 1, StreamMaxAge: time.Hour, StreamCopyMaxAge: time.Minute}
	if err := natsInternal.EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	pub := natsInternal.NewPublisher(js, cfg)
	h, _ := openHeads(t, js)

	if n := mustDispatch(t, h, pub, pullRequest(t, "d1", 42, shaA)); n != 3 {
		t.Fatalf("pull_request: %d copies, want repo, user and pr", n)
	}
	ci := map[string][]byte{
		"check_run":    checkRun(t, "c1", shaA, `[]`),
		"check_suite":  stored(t, "c2", "check_suite", `{"check_suite":{"head_sha":"`+shaA+`","pull_requests":[]}}`),
		"workflow_run": stored(t, "c3", "workflow_run", `{"workflow_run":{"head_sha":"`+shaA+`","pull_requests":[]}}`),
		"workflow_job": stored(t, "c4", "workflow_job", `{"workflow_job":{"head_sha":"`+shaA+`"}}`),
		"status":       stored(t, "c5", "status", `{"sha":"`+shaA+`","state":"success"}`),
	}
	for eventType, data := range ci {
		if n := mustDispatch(t, h, pub, data, "check_run"); n != 2 && eventType == "check_run" {
			t.Errorf("check_run: %d copies, want repo and pr: DISPATCH_USER_SKIP_EVENTS still holds", n)
		}
	}

	subjects := func() map[string]uint64 {
		info, err := js.StreamInfo("P", &nats.StreamInfoRequest{SubjectsFilter: "p.>"})
		if err != nil {
			t.Fatal(err)
		}
		return info.State.Subjects
	}
	got := subjects()
	for eventType := range ci {
		if got["p.pr.7.42."+eventType] != 1 {
			t.Errorf("%s: %d copies on the pr subject, want 1", eventType, got["p.pr.7.42."+eventType])
		}
	}
	if got["p.user.1.check_run"] != 0 || got["p.user.1.status"] != 1 {
		t.Errorf("user copies: check_run %d, status %d, want 0 and 1", got["p.user.1.check_run"], got["p.user.1.status"])
	}

	msg, err := js.GetLastMsg("P", "p.pr.7.42.check_run")
	if err != nil {
		t.Fatal(err)
	}
	if id := msg.Header.Get(nats.MsgIdHdr); id != "c1@p.pr.7.42.check_run" {
		t.Errorf("message id = %q", id)
	}
	if ttl := msg.Header.Get("Nats-TTL"); ttl == "" {
		t.Error("the copy has no TTL: NATS_STREAM_COPY_MAX_AGE must apply to it")
	}
	if string(msg.Data) != string(ci["check_run"]) {
		t.Error("copy is not byte-identical to the original")
	}

	before, _ := js.StreamInfo("P")
	for _, data := range ci {
		mustDispatch(t, h, pub, data, "check_run")
	}
	after, _ := js.StreamInfo("P")
	if after.State.Msgs != before.State.Msgs {
		t.Errorf("a second dispatch added %d messages", after.State.Msgs-before.State.Msgs)
	}
}

func TestHeadsGivesNoCopyForAnUnknownCommit(t *testing.T) {
	_, js := natstest.Start(t)
	h, _ := openHeads(t, js)
	rec := &syncRecorder{}
	for _, data := range [][]byte{
		checkRun(t, "c1", shaB, `[]`),
		checkRun(t, "c2", "not-a-sha", `[]`),
		stored(t, "c3", "status", `{}`),
		stored(t, "c4", "check_run", `{"check_run":"unexpected"}`),
	} {
		mustDispatch(t, h, rec, data, "check_run", "status")
	}
	if got, want := rec.list(), "p.repo.7.check_run p.repo.7.check_run p.repo.7.status p.repo.7.check_run"; got != want {
		t.Errorf("subjects = %s, want %s", got, want)
	}
}

func TestHeadsPrefersThePullRequestsOfThePayload(t *testing.T) {
	_, js := natstest.Start(t)
	h, kv := openHeads(t, js)
	rec := &syncRecorder{}
	mustDispatch(t, h, rec, pullRequest(t, "d1", 42, shaA))
	gets := kv.gets.Load()

	list := `[{"number":5,"base":{"repo":{"id":7}}},{"number":6,"base":{"repo":{"id":8}}},{"number":5},{"number":9}]`
	mustDispatch(t, h, rec, checkRun(t, "c1", shaA, list), "check_run")
	if got, want := rec.list(), "p.repo.7.pull_request p.user.1.pull_request p.pr.7.42.pull_request p.repo.7.check_run p.pr.7.5.check_run p.pr.7.9.check_run"; got != want {
		t.Errorf("subjects = %s, want %s: 6 belongs to another repository, 42 is only in the map", got, want)
	}
	if kv.gets.Load() != gets {
		t.Error("the map was read although the payload lists the pull requests")
	}
}

func TestHeadsCacheBoundsTheLookups(t *testing.T) {
	_, js := natstest.Start(t)
	other, _ := openHeads(t, js)
	if err := other.Learn(context.Background(), "7", shaA, 42); err != nil {
		t.Fatal(err)
	}

	h, kv := openHeads(t, js)
	h.missExpiry = 200 * time.Millisecond
	rec := &syncRecorder{}
	burst := func(sha string) {
		var wg sync.WaitGroup
		for i := range 20 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := h.Dispatch(context.Background(), rec, "p", checkRun(t, fmt.Sprintf("%s-%d", sha[:1], i), sha, `[]`), nil); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
	}

	burst(shaA)
	if got := kv.gets.Load(); got != 1 {
		t.Errorf("%d reads of the bucket for 20 check runs on a known head, want 1", got)
	}
	if got := rec.count("p.pr.7.42.check_run"); got != 20 {
		t.Errorf("%d of 20 check runs were linked", got)
	}

	burst(shaB)
	burst(shaB)
	if got := kv.gets.Load(); got != 2 {
		t.Errorf("%d reads after 40 check runs on an unknown commit, want 2 in all: a miss is cached", got)
	}
	time.Sleep(250 * time.Millisecond)
	burst(shaB)
	if got := kv.gets.Load(); got != 3 {
		t.Errorf("%d reads, want 3: a miss is read again after its expiry", got)
	}

	h.size = 4
	for i := range 20 {
		h.Lookup(context.Background(), "7", fmt.Sprintf("%040x", i))
	}
	if len(h.entries) != 4 || h.order.Len() != 4 {
		t.Errorf("cache holds %d entries, want its size of 4", len(h.entries))
	}
}

func TestHeadsLearnReplacesACachedMiss(t *testing.T) {
	_, js := natstest.Start(t)
	h, _ := openHeads(t, js)
	rec := &syncRecorder{}

	mustDispatch(t, h, rec, checkRun(t, "c1", shaA, `[]`))
	mustDispatch(t, h, rec, pullRequest(t, "d1", 42, shaA))
	mustDispatch(t, h, rec, checkRun(t, "c2", shaA, `[]`))
	if got := rec.count("p.pr.7.42.check_run"); got != 1 {
		t.Errorf("%d check runs linked, want 1: the one before the pull request is not, the one after is at once", got)
	}
}

func TestHeadsLinksEveryPullRequestOfACommit(t *testing.T) {
	_, js := natstest.Start(t)
	h, _ := openHeads(t, js)
	rec := &syncRecorder{}

	mustDispatch(t, h, rec, pullRequest(t, "d1", 42, shaA))
	mustDispatch(t, h, rec, pullRequest(t, "d2", 43, shaA))
	mustDispatch(t, h, rec, pullRequest(t, "d3", 42, shaA))
	entry, err := h.kv.Get("7." + shaA)
	if err != nil {
		t.Fatal(err)
	}
	if string(entry.Value()) != "42,43" {
		t.Errorf("bucket holds %q, want 42,43", entry.Value())
	}

	fresh, _ := openHeads(t, js)
	if n := mustDispatch(t, fresh, rec, checkRun(t, "c1", shaA, `[]`), "check_run"); n != 3 {
		t.Errorf("%d copies, want repo and two pr copies", n)
	}
	if rec.count("p.pr.7.42.check_run") != 1 || rec.count("p.pr.7.43.check_run") != 1 {
		t.Errorf("subjects = %s, want a check_run copy on both pull requests", rec.list())
	}

	for n := 100; n < 100+2*maxPRsPerHead; n++ {
		if err := h.Learn(context.Background(), "7", shaB, n); err != nil {
			t.Fatal(err)
		}
	}
	if got := h.Lookup(context.Background(), "7", shaB); len(got) != maxPRsPerHead || !slices.Contains(got, 100) {
		t.Errorf("%d pull requests kept for one commit, want the first %d", len(got), maxPRsPerHead)
	}
}

func TestHeadsNeverFailADispatch(t *testing.T) {
	_, js := natstest.Start(t)
	h, kv := openHeads(t, js)
	kv.fail = errors.New("bucket is gone")
	rec := &syncRecorder{}

	if n := mustDispatch(t, h, rec, pullRequest(t, "d1", 42, shaA)); n != 3 {
		t.Errorf("pull_request: %d copies, want 3", n)
	}
	mustDispatch(t, h, rec, checkRun(t, "c1", shaA, `[]`), "check_run")
	mustDispatch(t, h, rec, checkRun(t, "c2", shaA, `[]`), "check_run")
	if rec.count("p.repo.7.check_run") != 2 || rec.count("p.pr.7.42.check_run") != 0 {
		t.Errorf("subjects = %s, want the repo copies and no pr copy", rec.list())
	}
	if got := kv.gets.Load(); got != 2 {
		t.Errorf("%d reads of a bucket that fails, want 2: one to learn, one lookup that is cached as a miss", got)
	}

	if _, err := h.Dispatch(context.Background(), rec, "p", []byte("not json"), nil); !errors.Is(err, ErrUndecodable) {
		t.Errorf("err = %v, want ErrUndecodable", err)
	}
	if _, err := h.Dispatch(context.Background(), &recorder{fail: errors.New("nats down")}, "p", checkRun(t, "c3", shaA, `[{"number":5}]`), nil); err == nil {
		t.Error("a publish failure must still fail the dispatch")
	}
}

func TestOpenHeadsFollowsTheMaxAge(t *testing.T) {
	_, js := natstest.Start(t)
	for _, c := range []struct{ maxAge, want time.Duration }{
		{time.Hour, time.Hour},
		{2 * time.Hour, 2 * time.Hour},
		{time.Minute, time.Minute},
		{0, defaultHeadsTTL},
	} {
		h, err := OpenHeads(js, 1, c.maxAge)
		if err != nil {
			t.Fatalf("max age %s: %v", c.maxAge, err)
		}
		status, err := h.kv.Status()
		if err != nil {
			t.Fatal(err)
		}
		if status.TTL() != c.want || h.ttl != c.want {
			t.Errorf("max age %s: bucket TTL %s, want %s", c.maxAge, status.TTL(), c.want)
		}
	}
}
