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
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/pkg/events"
)

// Handler handles SSE connections directly with NATS
type Handler struct {
	nc          *nats.Conn
	js          nats.JetStreamContext
	cfg         *config.NATSConfig
	clients     map[string]*ClientConnection
	clientMutex sync.RWMutex
}

// ClientConnection represents an active SSE connection
type ClientConnection struct {
	ID         string
	StreamID   string
	Subject    string
	StartTime  time.Time
	EventCount atomic.Int64
}

// NewHandler creates a new SSE handler
func NewHandler(nc *nats.Conn, js nats.JetStreamContext, cfg *config.NATSConfig) *Handler {
	return &Handler{
		nc:      nc,
		js:      js,
		cfg:     cfg,
		clients: make(map[string]*ClientConnection),
	}
}

// ServeHTTP handles SSE connections
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, streamID string, subjects []string, filter *events.EventFilter) {
	// Create SSE writer
	sseWriter, err := NewWriter(w)
	if err != nil {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// Get Last-Event-ID header for resumption
	lastEventID := r.Header.Get("Last-Event-ID")

	// Get last N messages parameter (default 50)
	lastCount := 50
	if lastParam := r.URL.Query().Get("last"); lastParam != "" {
		if n, err := strconv.Atoi(lastParam); err == nil && n > 0 {
			lastCount = n
		}
	}

	// Create client connection record
	clientID := fmt.Sprintf("%s-%d", streamID, time.Now().UnixNano())
	client := &ClientConnection{
		ID:        clientID,
		StreamID:  streamID,
		Subject:   strings.Join(subjects, ","),
		StartTime: time.Now(),
	}

	h.registerClient(client)
	defer h.unregisterClient(clientID)

	if err := sseWriter.WriteRetry(5000); err != nil {
		return
	}

	// Create context that cancels when client disconnects
	ctx := r.Context()

	// Create NATS subscription with JetStream
	sub, err := h.createSubscription(ctx, subjects, lastEventID, lastCount)
	if err != nil {
		slog.Error("Failed to create NATS subscription",
			"error", err,
			"subjects", subjects,
			"client_id", clientID)
		_ = sseWriter.WriteEvent("", "error", []byte(`{"error":"Failed to create subscription"}`))
		return
	}
	defer func() { _ = sub.Unsubscribe() }()

	// Send connected event
	connectedMsg := map[string]string{
		"status":    "connected",
		"stream_id": streamID,
		"subjects":  strings.Join(subjects, ","),
	}
	connectedData, _ := json.Marshal(connectedMsg)
	if err := sseWriter.WriteEvent("", "connected", connectedData); err != nil {
		return
	}

	// Start heartbeat
	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()

	// Message handling loop
	msgCh := make(chan *nats.Msg, 100)
	errCh := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msgs, err := sub.Fetch(10, nats.MaxWait(1*time.Second))
				if err != nil {
					if err == nats.ErrTimeout {
						continue
					}
					if ctx.Err() != nil {
						return
					}
					// For any other error, log and exit
					slog.Error("Error fetching messages", "error", err, "client_id", clientID)
					select {
					case errCh <- err:
					default:
					}
					return
				}
				for _, msg := range msgs {
					select {
					case msgCh <- msg:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	// Main event loop
	for {
		select {
		case <-ctx.Done():
			// Client disconnected
			return

		case err := <-errCh:
			// Fatal error in fetch loop
			slog.Error("Fatal error in SSE handler", "error", err, "client_id", clientID)
			_ = sseWriter.WriteEvent("", "error", []byte(fmt.Sprintf(`{"error":"Stream error: %s"}`, err.Error())))
			return

		case <-heartbeat.C:
			// Send heartbeat comment
			if err := sseWriter.WriteComment("heartbeat"); err != nil {
				return
			}

		case msg := <-msgCh:
			var event events.GitHubEvent
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				slog.Error("Failed to unmarshal event", "error", err)
				continue
			}

			if filter != nil && !filter.Matches(&event) {
				continue
			}

			meta, err := msg.Metadata()
			if err != nil {
				slog.Error("Message without JetStream metadata", "error", err)
				continue
			}
			id := strconv.FormatUint(meta.Sequence.Stream, 10)
			if err := sseWriter.WriteEvent(id, event.Event, msg.Data); err != nil {
				return
			}

			client.EventCount.Add(1)
		}
	}
}

// maxLast bounds the replay a client can ask for with ?last=.
const maxLast = 1000

var consumerSeq atomic.Uint64

func (h *Handler) consumerName() string {
	return fmt.Sprintf("sse-%d-%d", time.Now().UnixNano(), consumerSeq.Add(1))
}

// createSubscription creates a consumer that the server deletes once idle. Last-Event-ID is the
// stream sequence of the last event the client saw.
func (h *Handler) createSubscription(ctx context.Context, subjects []string, lastEventID string, lastCount int) (*nats.Subscription, error) {
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
		cc.DeliverPolicy = nats.DeliverByStartSequencePolicy
		cc.OptStartSeq = seq + 1
	} else if lastCount > 0 {
		start, err := h.startSeqForLast(subjects, lastCount)
		if err != nil {
			return nil, err
		}
		if start > 0 {
			cc.DeliverPolicy = nats.DeliverByStartSequencePolicy
			cc.OptStartSeq = start
		}
	}

	if _, err := h.js.AddConsumer(h.cfg.StreamName, cc); err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	sub, err := h.js.PullSubscribe("", name, nats.Bind(h.cfg.StreamName, name))
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
	err := h.js.DeleteConsumer(h.cfg.StreamName, name)
	if err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
		slog.Warn("Failed to delete consumer", "consumer", name, "error", err)
	}
}

// startSeqForLast returns the sequence from which exactly the last n
// messages matching subjects remain, or 0 if the stream is empty. The
// stream holds fan-out copies of other subjects in between, so
// LastSeq-n is wrong for any filter; a binary search on NumPending is not.
func (h *Handler) startSeqForLast(subjects []string, n int) (uint64, error) {
	if n > maxLast {
		n = maxLast
	}
	si, err := h.js.StreamInfo(h.cfg.StreamName)
	if err != nil {
		return 0, fmt.Errorf("failed to get stream info: %w", err)
	}
	if si.State.Msgs == 0 {
		return 0, nil
	}

	lo, hi := si.State.FirstSeq, si.State.LastSeq
	total, err := h.pendingFrom(subjects, lo)
	if err != nil {
		return 0, err
	}
	if total <= uint64(n) {
		return lo, nil
	}
	// invariant: pendingFrom(lo) >= n
	for lo < hi {
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
	info, err := h.js.AddConsumer(h.cfg.StreamName, &nats.ConsumerConfig{
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

// registerClient registers a new client connection
func (h *Handler) registerClient(client *ClientConnection) {
	h.clientMutex.Lock()
	defer h.clientMutex.Unlock()
	h.clients[client.ID] = client
	slog.Info("SSE client connected",
		"client_id", client.ID,
		"stream_id", client.StreamID,
		"subject", client.Subject)
}

// unregisterClient removes a client connection
func (h *Handler) unregisterClient(clientID string) {
	h.clientMutex.Lock()
	defer h.clientMutex.Unlock()

	if client, exists := h.clients[clientID]; exists {
		duration := time.Since(client.StartTime)
		slog.Info("SSE client disconnected",
			"client_id", clientID,
			"stream_id", client.StreamID,
			"duration", duration,
			"event_count", client.EventCount.Load())
		delete(h.clients, clientID)
	}
}

// GetStats returns current connection statistics
func (h *Handler) GetStats() map[string]interface{} {
	h.clientMutex.RLock()
	defer h.clientMutex.RUnlock()

	streamStats := make(map[string]int)
	totalEvents := int64(0)

	for _, client := range h.clients {
		streamStats[client.StreamID]++
		totalEvents += client.EventCount.Load()
	}

	return map[string]interface{}{
		"total_clients":     len(h.clients),
		"total_events":      totalEvents,
		"clients_by_stream": streamStats,
	}
}
