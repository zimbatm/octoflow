package dispatch

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/metrics"
	"github.com/zimbatm/octoflow/internal/names"
	"github.com/zimbatm/octoflow/pkg/events"
)

// HeadsBucket maps {repo_id}.{head_sha} to the pull request numbers that
// have this commit as their head, as a comma-separated list.
const HeadsBucket = "octoflow_heads"

const (
	// used when the stream has no max age
	defaultHeadsTTL = 30 * 24 * time.Hour
	headsCacheSize  = 10000
	// another dispatcher process can add a pull request to a known head
	headsHitExpiry = 10 * time.Minute
	// short, so that a commit that becomes a head in another process is
	// linked soon; long enough for a burst of check runs on one commit
	headsMissExpiry = 10 * time.Second
	// a value read back in full on every lookup must stay small
	maxPRsPerHead = 16
)

// Heads remembers which pull requests a commit is the head of, so that CI
// events, which name only the commit, get a copy on the pr subject. The
// bucket is the memory; the cache in front of it holds hits and misses.
type Heads struct {
	kv  nats.KeyValue
	ttl time.Duration
	// Names, when set, gets the names of each dispatched event
	Names *names.Writer

	size                  int
	hitExpiry, missExpiry time.Duration

	mu      sync.Mutex
	entries map[string]*list.Element
	// most recently used first
	order   *list.List
	loading map[string]chan struct{}
}

type headEntry struct {
	key     string
	prs     []int
	expires time.Time
}

// OpenHeads opens the bucket, creating it on first use. Its keys expire
// maxAge after their last write, the max age of the events stream: a CI
// event that the stream no longer holds needs no link.
func OpenHeads(js nats.JetStreamContext, replicas int, maxAge time.Duration) (*Heads, error) {
	ttl := maxAge
	if ttl <= 0 {
		ttl = defaultHeadsTTL
	}
	kv, err := js.KeyValue(HeadsBucket)
	switch {
	case errors.Is(err, nats.ErrBucketNotFound):
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      HeadsBucket,
			Description: "head commit to pull request numbers",
			Storage:     nats.FileStorage,
			Replicas:    replicas,
			TTL:         ttl,
		})
	case err == nil:
		err = setBucketTTL(js, kv, ttl)
	}
	if err != nil {
		return nil, err
	}
	return newHeads(kv, ttl), nil
}

func setBucketTTL(js nats.JetStreamContext, kv nats.KeyValue, ttl time.Duration) error {
	status, err := kv.Status()
	if err != nil || status.TTL() == ttl {
		return err
	}
	info, err := js.StreamInfo("KV_" + HeadsBucket)
	if err != nil {
		return err
	}
	sc := info.Config
	sc.MaxAge = ttl
	// the server rejects a duplicate window longer than the max age.
	sc.Duplicates = min(sc.Duplicates, ttl)
	_, err = js.UpdateStream(&sc)
	return err
}

func newHeads(kv nats.KeyValue, ttl time.Duration) *Heads {
	return &Heads{
		kv:         kv,
		ttl:        ttl,
		size:       headsCacheSize,
		hitExpiry:  headsHitExpiry,
		missExpiry: headsMissExpiry,
		entries:    map[string]*list.Element{},
		order:      list.New(),
		loading:    map[string]chan struct{}{},
	}
}

// Dispatch is the package's Dispatch plus the link from CI events to pull
// requests. A pull_request event teaches the map its head before any of
// its copies is written, so before it is acked. A check_run, check_suite,
// workflow_run, workflow_job or status event gets one more copy per pull
// request, on pr.{repo}.{n}.{type}. The map never fails a dispatch: when
// it cannot be read or written the event goes without that copy.
func (h *Heads) Dispatch(ctx context.Context, pub Publisher, prefix string, data []byte, skipUserEvents []string) (int, error) {
	var event events.GitHubEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return Dispatch(ctx, pub, h.Names, prefix, data, skipUserEvents)
	}
	ci := isCI(event.Event)
	var p *headPayload
	if (ci || event.Event == "pull_request") && event.RepositoryID != "" {
		p = decodeHeadPayload(event.Payload)
	}
	if p != nil && !ci {
		h.learn(ctx, &event, p)
	}

	n, err := Dispatch(ctx, pub, h.Names, prefix, data, skipUserEvents)
	if err != nil || !ci {
		return n, err
	}

	link := "none"
	var prs []int
	if p != nil {
		link, prs = h.pullRequests(ctx, &event, p)
	}
	for _, number := range prs {
		subject := fmt.Sprintf("%s.pr.%s.%d.%s", prefix, event.RepositoryID, number, event.Event)
		if err := pub.PublishToSubject(ctx, subject, event.Event, data, event.ID+"@"+subject); err != nil {
			return 0, fmt.Errorf("publish to %s: %w", subject, err)
		}
	}
	metrics.DispatchCIEvents.WithLabelValues(event.Event, link).Inc()
	if len(prs) > 0 {
		slog.Debug("Linked CI event to pull requests",
			"event_id", event.ID,
			"event_type", event.Event,
			"repo_id", event.RepositoryID,
			"pr_numbers", prs,
			"link", link)
	}
	return n + len(prs), nil
}

func isCI(eventType string) bool {
	switch eventType {
	case "check_run", "check_suite", "workflow_run", "workflow_job", "status":
		return true
	}
	return false
}

type ciObject struct {
	HeadSHA      string `json:"head_sha"`
	PullRequests []struct {
		Number int `json:"number"`
		Base   struct {
			Repo struct {
				ID json.RawMessage `json:"id"`
			} `json:"repo"`
		} `json:"base"`
	} `json:"pull_requests"`
}

type headPayload struct {
	SHA         string    `json:"sha"`
	CheckRun    *ciObject `json:"check_run"`
	CheckSuite  *ciObject `json:"check_suite"`
	WorkflowRun *ciObject `json:"workflow_run"`
	WorkflowJob *ciObject `json:"workflow_job"`
	PullRequest *struct {
		Number int `json:"number"`
		Head   struct {
			SHA string `json:"sha"`
		} `json:"head"`
	} `json:"pull_request"`
}

func decodeHeadPayload(raw json.RawMessage) *headPayload {
	var p headPayload
	err := json.Unmarshal(raw, &p)
	var typeErr *json.UnmarshalTypeError
	if err != nil && !errors.As(err, &typeErr) {
		return nil
	}
	return &p
}

func (h *Heads) learn(ctx context.Context, event *events.GitHubEvent, p *headPayload) {
	pr := p.PullRequest
	if pr == nil || pr.Number <= 0 || !isSHA(pr.Head.SHA) {
		return
	}
	if err := h.Learn(ctx, event.RepositoryID, pr.Head.SHA, pr.Number); err != nil {
		slog.Warn("Failed to record the head of a pull request; CI events of this commit get no pr copy",
			"event_id", event.ID,
			"repo_id", event.RepositoryID,
			"pr_number", pr.Number,
			"head_sha", pr.Head.SHA,
			"error", err)
	}
}

// pullRequests answers from the payload when it lists pull requests, else
// from the map. link says which: payload, map or none.
func (h *Heads) pullRequests(ctx context.Context, event *events.GitHubEvent, p *headPayload) (link string, prs []int) {
	var obj *ciObject
	sha := p.SHA
	switch event.Event {
	case "check_run":
		obj = p.CheckRun
	case "check_suite":
		obj = p.CheckSuite
	case "workflow_run":
		obj = p.WorkflowRun
	case "workflow_job":
		obj = p.WorkflowJob
	}
	if obj != nil {
		sha = obj.HeadSHA
		for _, pr := range obj.PullRequests {
			// a check on a fork lists the pull request of the upstream
			// repository, whose number means nothing in the fork.
			base := strings.Trim(string(pr.Base.Repo.ID), `"`)
			if pr.Number <= 0 || (base != "" && base != event.RepositoryID) || slices.Contains(prs, pr.Number) {
				continue
			}
			prs = append(prs, pr.Number)
		}
		if len(prs) > 0 {
			return "payload", prs
		}
	}
	if !isSHA(sha) {
		return "none", nil
	}
	if prs = h.Lookup(ctx, event.RepositoryID, sha); len(prs) > 0 {
		return "map", prs
	}
	return "none", nil
}

// isSHA keeps anything but a commit id out of the bucket's keys.
func isSHA(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Learn records that sha is the head of pull request number. It costs
// nothing when the cache already knows, one read when the bucket does,
// and a read and a write otherwise.
func (h *Heads) Learn(_ context.Context, repoID, sha string, number int) error {
	key := repoID + "." + sha
	h.mu.Lock()
	prs, ok := h.cached(key)
	h.mu.Unlock()
	if ok && slices.Contains(prs, number) {
		return nil
	}
	prs, err := h.write(key, number)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.store(key, prs)
	h.mu.Unlock()
	return nil
}

func (h *Heads) write(key string, number int) ([]int, error) {
	var err error
	for range 3 {
		var entry nats.KeyValueEntry
		var prs []int
		entry, err = h.kv.Get(key)
		switch {
		case errors.Is(err, nats.ErrKeyNotFound):
			prs = []int{number}
			_, err = h.kv.Create(key, encodePRs(prs))
		case err != nil:
			return nil, err
		default:
			prs = decodePRs(entry.Value())
			switch {
			case slices.Contains(prs, number):
				// a pull request that stays open keeps its head in the bucket
				if time.Since(entry.Created()) < h.ttl/2 {
					return prs, nil
				}
			case len(prs) >= maxPRsPerHead:
				return prs, nil
			default:
				prs = append(prs, number)
			}
			_, err = h.kv.Update(key, encodePRs(prs), entry.Revision())
		}
		if err == nil {
			return prs, nil
		}
		// another worker wrote the key in between: read it again
		if !errors.Is(err, nats.ErrKeyExists) {
			return nil, err
		}
	}
	return nil, err
}

// Lookup lists the pull requests that have sha as their head. Concurrent
// lookups of one commit share one read of the bucket, and the answer is
// cached, a miss too. A bucket that does not answer counts as a miss.
func (h *Heads) Lookup(ctx context.Context, repoID, sha string) []int {
	key := repoID + "." + sha
	done := make(chan struct{})
	for {
		h.mu.Lock()
		if prs, ok := h.cached(key); ok {
			h.mu.Unlock()
			return prs
		}
		wait, busy := h.loading[key]
		if !busy {
			h.loading[key] = done
			h.mu.Unlock()
			break
		}
		h.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil
		}
	}

	var prs []int
	entry, err := h.kv.Get(key)
	switch {
	case err == nil:
		prs = decodePRs(entry.Value())
	case !errors.Is(err, nats.ErrKeyNotFound):
		slog.Warn("Failed to look up the pull requests of a commit; its CI events get no pr copy",
			"repo_id", repoID,
			"head_sha", sha,
			"error", err)
	}

	h.mu.Lock()
	prs = h.store(key, prs)
	delete(h.loading, key)
	h.mu.Unlock()
	close(done)
	return prs
}

func (h *Heads) cached(key string) ([]int, bool) {
	el, ok := h.entries[key]
	if !ok {
		return nil, false
	}
	e := el.Value.(*headEntry)
	if !time.Now().Before(e.expires) {
		return nil, false
	}
	h.order.MoveToFront(el)
	return e.prs, true
}

// store adds to what the cache holds and never takes away: a lookup that
// read the bucket just before Learn wrote it must not hide the new head.
func (h *Heads) store(key string, prs []int) []int {
	now := time.Now()
	el, ok := h.entries[key]
	if !ok {
		el = h.order.PushFront(&headEntry{key: key})
		h.entries[key] = el
		if h.order.Len() > h.size {
			oldest := h.order.Back()
			h.order.Remove(oldest)
			delete(h.entries, oldest.Value.(*headEntry).key)
		}
	}
	e := el.Value.(*headEntry)
	if now.Before(e.expires) {
		merged := slices.Clone(e.prs)
		for _, n := range prs {
			if !slices.Contains(merged, n) {
				merged = append(merged, n)
			}
		}
		prs = merged
	}
	e.prs = prs
	e.expires = now.Add(h.missExpiry)
	if len(prs) > 0 {
		e.expires = now.Add(h.hitExpiry)
	}
	h.order.MoveToFront(el)
	return prs
}

func encodePRs(prs []int) []byte {
	parts := make([]string, len(prs))
	for i, n := range prs {
		parts[i] = strconv.Itoa(n)
	}
	return []byte(strings.Join(parts, ","))
}

func decodePRs(value []byte) []int {
	var prs []int
	for part := range strings.SplitSeq(string(value), ",") {
		if n, err := strconv.Atoi(part); err == nil && n > 0 {
			prs = append(prs, n)
		}
	}
	return prs
}
