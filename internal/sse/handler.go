package sse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/metrics"
	"github.com/zimbatm/octoflow/internal/names"
	"github.com/zimbatm/octoflow/pkg/events"
)

// maxLast bounds the replay a client can ask for with ?last=: the search for
// its start costs more the further back it goes. A variable so tests can
// lower it.
var maxLast = 1000

const heartbeatEvery = 30 * time.Second

// Handler turns stream subjects into SSE streams.
type Handler struct {
	js     nats.JetStreamContext
	stream string
	// MaxClients bounds the open streams, each of which holds a JetStream
	// consumer; 0 is unlimited. Beyond it a request gets a 503.
	MaxClients int
	// Names resolves the names in a path; nil knows none
	Names *names.Resolver
	// Site is served at /site for the landing page; nil serves null
	Site    any
	clients atomic.Int64
	// replay starts found by startSeqForLast, keyed by subjects, count and
	// stream bounds; a reconnect storm asks the same question many times
	cacheMu sync.Mutex
	cache   map[string]uint64
	// closed by Shutdown
	done     chan struct{}
	shutdown sync.Once
	// the last answer of Stored
	storedMu sync.Mutex
	stored   stored
}

func NewHandler(js nats.JetStreamContext, stream string, maxClients int) *Handler {
	return &Handler{js: js, stream: stream, MaxClients: maxClients, done: make(chan struct{})}
}

// Shutdown ends every open stream. http.Server.Shutdown waits for open
// requests, and an SSE request never ends by itself; the browser
// reconnects with Last-Event-ID.
func (h *Handler) Shutdown() {
	h.shutdown.Do(func() { close(h.done) })
}

// Clients is the number of open streams.
func (h *Handler) Clients() int64 { return h.clients.Load() }

// Stream replays the events matching subjects, then writes new ones until
// the client leaves. The replay starts after Last-Event-ID, else at
// ?since=, else at the last ?last= events. streamID names the route for
// logs and metrics, for example org-222.
func (h *Handler) Stream(w http.ResponseWriter, r *http.Request, streamID string, subjects []string, filter *events.EventFilter) {
	var since time.Time
	if s := r.URL.Query().Get("since"); s != "" {
		var err error
		if since, err = parseSince(s, time.Now()); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	slim := false
	switch p := r.URL.Query().Get("payload"); p {
	case "":
	case "none":
		slim = true
	default:
		http.Error(w, fmt.Sprintf("payload: %q is not none, the one value", p), http.StatusBadRequest)
		return
	}
	sw, err := NewWriter(w)
	if err != nil {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}
	lastCount := 50
	if n, err := strconv.Atoi(r.URL.Query().Get("last")); err == nil && n >= 0 {
		lastCount = n
	}
	kind, _, _ := strings.Cut(streamID, "-")

	if n := h.clients.Add(1); h.MaxClients > 0 && n > int64(h.MaxClients) {
		h.clients.Add(-1)
		http.Error(w, "Too many clients", http.StatusServiceUnavailable)
		return
	}
	metrics.SSEClients.Set(float64(h.clients.Load()))
	start := time.Now()
	sent := 0
	slog.Info("SSE client connected", "stream_id", streamID, "subjects", subjects)
	defer func() {
		h.clients.Add(-1)
		metrics.SSEClients.Set(float64(h.clients.Load()))
		slog.Info("SSE client disconnected", "stream_id", streamID, "duration", time.Since(start), "events", sent)
	}()

	if err := sw.WriteRetry(5000); err != nil {
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	go func() {
		select {
		case <-h.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	sub, err := h.subscribe(ctx, subjects, r.Header.Get("Last-Event-ID"), since, lastCount)
	if err != nil {
		// the client left, or the server shuts down, while the replay start was
		// looked up: nothing failed, and nobody is there to read an error
		if ctx.Err() != nil {
			return
		}
		slog.Error("Failed to create NATS subscription", "error", err, "subjects", subjects, "stream_id", streamID)
		_ = sw.WriteEvent("", "error", []byte(`{"error":"Failed to create subscription"}`))
		return
	}
	defer func() { _ = sub.Unsubscribe() }()

	connected, _ := json.Marshal(map[string]string{
		"status": "connected", "stream_id": streamID, "subjects": strings.Join(subjects, ","),
	})
	if err := sw.WriteEvent("", "connected", connected); err != nil {
		return
	}

	lastWrite := time.Now()
	for ctx.Err() == nil {
		msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
		if err != nil && !errors.Is(err, nats.ErrTimeout) {
			if ctx.Err() != nil {
				return
			}
			slog.Error("Error fetching messages", "error", err, "stream_id", streamID)
			_ = sw.WriteEvent("", "error", []byte(fmt.Sprintf(`{"error":"Stream error: %s"}`, err)))
			return
		}
		for _, msg := range msgs {
			var event events.GitHubEvent
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				slog.Error("Failed to unmarshal event", "error", err)
				continue
			}
			if !filter.Matches(&event) {
				continue
			}
			meta, err := msg.Metadata()
			if err != nil {
				slog.Error("Message without JetStream metadata", "error", err)
				continue
			}
			data := msg.Data
			if slim {
				if data, err = json.Marshal(withoutPayload{GitHubEvent: &event}); err != nil {
					slog.Error("Failed to marshal event", "error", err)
					continue
				}
			}
			// no event name: a browser's onmessage then sees every type, and
			// the type is in the data anyway. connected and error stay named.
			if err := sw.WriteEvent(strconv.FormatUint(meta.Sequence.Stream, 10), "", data); err != nil {
				return
			}
			lastWrite = time.Now()
			sent++
			metrics.SSEEventsSent.WithLabelValues(kind).Inc()
		}
		if time.Since(lastWrite) >= heartbeatEvery {
			if err := sw.WriteComment("heartbeat"); err != nil {
				return
			}
			lastWrite = time.Now()
		}
	}
}

// withoutPayload is the event of ?payload=none. Its Payload shadows the one
// of the event, which has no omitempty and would print "payload":null.
type withoutPayload struct {
	*events.GitHubEvent
	Payload *struct{} `json:"payload,omitempty"`
}

var consumerSeq atomic.Uint64

func (h *Handler) consumerName() string {
	return fmt.Sprintf("sse-%d-%d", time.Now().UnixNano(), consumerSeq.Add(1))
}

// subscribe creates a consumer that the server deletes once idle.
// Last-Event-ID is the stream sequence of the last event the client saw.
func (h *Handler) subscribe(ctx context.Context, subjects []string, lastEventID string, since time.Time, lastCount int) (*nats.Subscription, error) {
	name := h.consumerName()
	cc := &nats.ConsumerConfig{
		Durable:        name,
		AckPolicy:      nats.AckNonePolicy,
		FilterSubjects: subjects,
		DeliverPolicy:  nats.DeliverNewPolicy,
		// the server removes consumers of clients that vanish without cleanup.
		InactiveThreshold: time.Minute,
	}

	// a start sequence or time costs no search, so neither has a cap. The
	// server moves a start before the first message to the first message.
	if seq, err := strconv.ParseUint(lastEventID, 10, 64); err == nil && seq < math.MaxUint64 {
		cc.DeliverPolicy = nats.DeliverByStartSequencePolicy
		cc.OptStartSeq = seq + 1
	} else if !since.IsZero() {
		cc.DeliverPolicy = nats.DeliverByStartTimePolicy
		cc.OptStartTime = &since
	} else if lastCount > 0 {
		start, err := h.startSeqForLast(ctx, subjects, lastCount)
		if err != nil {
			return nil, err
		}
		if start > 0 {
			cc.DeliverPolicy = nats.DeliverByStartSequencePolicy
			cc.OptStartSeq = start
		}
	}

	if _, err := h.js.AddConsumer(h.stream, cc); err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	sub, err := h.js.PullSubscribe("", name, nats.Bind(h.stream, name))
	if err != nil {
		h.deleteConsumer(name)
		return nil, fmt.Errorf("failed to create subscription: %w", err)
	}
	go func() {
		<-ctx.Done()
		h.deleteConsumer(name)
	}()
	return sub, nil
}

// parseSince reads an RFC 3339 time, or a duration back from now: a Go
// duration such as 90m, or whole days as in 2d.
func parseSince(s string, now time.Time) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	d, err := time.ParseDuration(s)
	if days, ok := strings.CutSuffix(s, "d"); ok {
		var n int64
		// 16 bits of days do not overflow a Duration
		n, err = strconv.ParseInt(days, 10, 16)
		d = time.Duration(n) * 24 * time.Hour
	}
	if err != nil || d <= 0 {
		return time.Time{}, fmt.Errorf("since: %q is not an RFC 3339 time, nor a duration such as 90m or 2d", s)
	}
	return now.Add(-d), nil
}

// deleteConsumer is best effort: InactiveThreshold removes what it misses.
func (h *Handler) deleteConsumer(name string) {
	err := h.js.DeleteConsumer(h.stream, name)
	if err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
		slog.Warn("Failed to delete consumer", "consumer", name, "error", err)
	}
}

// startSeqForLast returns the sequence from which exactly the last n
// messages matching subjects remain, or 0 if the stream is empty. The
// stream holds fan-out copies of other subjects in between, so
// LastSeq-n is wrong for any filter; a binary search on NumPending is not.
func (h *Handler) startSeqForLast(ctx context.Context, subjects []string, n int) (uint64, error) {
	n = min(n, maxLast)
	si, err := h.js.StreamInfo(h.stream)
	if err != nil {
		return 0, fmt.Errorf("failed to get stream info: %w", err)
	}
	if si.State.Msgs == 0 {
		return 0, nil
	}
	key := fmt.Sprintf("%s|%d|%d|%d", strings.Join(subjects, ","), n, si.State.FirstSeq, si.State.LastSeq)
	h.cacheMu.Lock()
	start, hit := h.cache[key]
	h.cacheMu.Unlock()
	if hit {
		return start, nil
	}
	start, err = h.searchStart(ctx, subjects, n, si.State.FirstSeq, si.State.LastSeq)
	if err != nil {
		return 0, err
	}
	h.cacheMu.Lock()
	if len(h.cache) >= 1024 {
		clear(h.cache)
	}
	if h.cache == nil {
		h.cache = map[string]uint64{}
	}
	h.cache[key] = start
	h.cacheMu.Unlock()
	return start, nil
}

// searchStart is a binary search on NumPending over [first, last].
func (h *Handler) searchStart(ctx context.Context, subjects []string, n int, first, last uint64) (uint64, error) {
	lo, hi := first, last
	total, err := h.pendingFrom(subjects, lo)
	if err != nil {
		return 0, err
	}
	if total <= uint64(n) {
		return lo, nil
	}
	// invariant: pendingFrom(lo) >= n
	for lo < hi {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		mid := lo + (hi-lo+1)/2
		p, err := h.pendingFrom(subjects, mid)
		if err != nil {
			return 0, err
		}
		if p >= uint64(n) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo, nil
}

func (h *Handler) pendingFrom(subjects []string, seq uint64) (uint64, error) {
	name := h.consumerName()
	info, err := h.js.AddConsumer(h.stream, &nats.ConsumerConfig{
		Durable:           name,
		AckPolicy:         nats.AckNonePolicy,
		FilterSubjects:    subjects,
		DeliverPolicy:     nats.DeliverByStartSequencePolicy,
		OptStartSeq:       seq,
		InactiveThreshold: 10 * time.Second,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to probe stream: %w", err)
	}
	h.deleteConsumer(name)
	return info.NumPending, nil
}

// storedTTL is how long Stored reuses its answer; the dashboard asks every
// few seconds and each answer costs a consumer on the stream.
const storedTTL = 30 * time.Second

type stored struct {
	events uint64
	since  time.Time
	at     time.Time
}

// Stored counts the events in the stream that match subjects, which the
// stream's own message count cannot say: it includes the fan-out copies.
// since is the store time of the oldest message, so the retention reached.
func (h *Handler) Stored(subjects []string) (events uint64, since time.Time, err error) {
	if h.js == nil {
		return 0, time.Time{}, errors.New("no stream")
	}
	h.storedMu.Lock()
	defer h.storedMu.Unlock()
	if !h.stored.at.IsZero() && time.Since(h.stored.at) < storedTTL {
		return h.stored.events, h.stored.since, nil
	}
	si, err := h.js.StreamInfo(h.stream)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("failed to get stream info: %w", err)
	}
	s := stored{at: time.Now()}
	if si.State.Msgs > 0 {
		if s.events, err = h.pendingFrom(subjects, si.State.FirstSeq); err != nil {
			return 0, time.Time{}, err
		}
		s.since = si.State.FirstTime
	}
	h.stored = s
	return s.events, s.since, nil
}
