package sse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/metrics"
	"github.com/zimbatm/octoflow/pkg/events"
)

// maxLast bounds the replay a client can ask for, with ?last= or with an
// old Last-Event-ID. A variable so tests can lower it.
var maxLast = 1000

const heartbeatEvery = 30 * time.Second

// Handler turns stream subjects into SSE streams.
type Handler struct {
	js     nats.JetStreamContext
	stream string
	// MaxClients bounds the open streams, each of which holds a JetStream
	// consumer; 0 is unlimited. Beyond it a request gets a 503.
	MaxClients int
	clients    atomic.Int64
	// replay starts found by startSeqForLast, keyed by subjects, count and
	// stream bounds; a reconnect storm asks the same question many times
	cacheMu sync.Mutex
	cache   map[string]uint64
}

func NewHandler(js nats.JetStreamContext, stream string, maxClients int) *Handler {
	return &Handler{js: js, stream: stream, MaxClients: maxClients}
}

// Clients is the number of open streams.
func (h *Handler) Clients() int64 { return h.clients.Load() }

// Stream replays the last ?last= events matching subjects, or resumes after
// Last-Event-ID, then writes new ones until the client leaves. streamID
// names the route for logs and metrics, for example org-222.
func (h *Handler) Stream(w http.ResponseWriter, r *http.Request, streamID string, subjects []string, filter *events.EventFilter) {
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
	ctx := r.Context()
	sub, err := h.subscribe(ctx, subjects, r.Header.Get("Last-Event-ID"), lastCount)
	if err != nil {
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
			if err := sw.WriteEvent(strconv.FormatUint(meta.Sequence.Stream, 10), event.Event, msg.Data); err != nil {
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

var consumerSeq atomic.Uint64

func (h *Handler) consumerName() string {
	return fmt.Sprintf("sse-%d-%d", time.Now().UnixNano(), consumerSeq.Add(1))
}

// subscribe creates a consumer that the server deletes once idle.
// Last-Event-ID is the stream sequence of the last event the client saw.
func (h *Handler) subscribe(ctx context.Context, subjects []string, lastEventID string, lastCount int) (*nats.Subscription, error) {
	name := h.consumerName()
	cc := &nats.ConsumerConfig{
		Durable:        name,
		AckPolicy:      nats.AckNonePolicy,
		FilterSubjects: subjects,
		DeliverPolicy:  nats.DeliverNewPolicy,
		// the server removes consumers of clients that vanish without cleanup.
		InactiveThreshold: time.Minute,
	}

	if seq, err := strconv.ParseUint(lastEventID, 10, 64); err == nil {
		// a reconnect resumes where it left off, but never further back
		// than the replay cap: an old id would stream the whole stream.
		floor, err := h.startSeqForLast(ctx, subjects, maxLast)
		if err != nil {
			return nil, err
		}
		cc.DeliverPolicy = nats.DeliverByStartSequencePolicy
		cc.OptStartSeq = max(seq+1, floor)
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
