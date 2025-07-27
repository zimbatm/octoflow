package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
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
	EventCount int64
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
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, streamID, natsSubject string, filter *events.EventFilter) {
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
		Subject:   natsSubject,
		StartTime: time.Now(),
	}
	
	h.registerClient(client)
	defer h.unregisterClient(clientID)
	
	// Send initial retry suggestion
	sseWriter.WriteRetry(5000) // 5 seconds
	
	// Create context that cancels when client disconnects
	ctx := r.Context()
	
	// Create NATS subscription with JetStream
	sub, err := h.createSubscription(ctx, natsSubject, lastEventID, lastCount)
	if err != nil {
		slog.Error("Failed to create NATS subscription", 
			"error", err, 
			"subject", natsSubject,
			"client_id", clientID)
		sseWriter.WriteEvent("", "error", []byte(`{"error":"Failed to create subscription"}`))
		return
	}
	defer sub.Unsubscribe()
	
	// Send connected event
	connectedMsg := map[string]string{
		"status":    "connected",
		"stream_id": streamID,
		"subject":   natsSubject,
	}
	connectedData, _ := json.Marshal(connectedMsg)
	sseWriter.WriteEvent("", "connected", connectedData)
	
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
			sseWriter.WriteEvent("", "error", []byte(fmt.Sprintf(`{"error":"Stream error: %s"}`, err.Error())))
			return
			
		case <-heartbeat.C:
			// Send heartbeat comment
			if err := sseWriter.WriteComment("heartbeat"); err != nil {
				return
			}
			
		case msg := <-msgCh:
			// Parse event
			var event events.GitHubEvent
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				slog.Error("Failed to unmarshal event", "error", err)
				msg.Nak()
				continue
			}
			
			// Apply filter if provided
			if filter != nil && !filter.Matches(&event) {
				msg.Ack()
				continue
			}
			
			// Send event to client
			if err := sseWriter.WriteEvent(event.ID, event.Event, msg.Data); err != nil {
				// Client disconnected
				msg.Nak()
				return
			}
			
			// Update stats
			client.EventCount++
			
			// Acknowledge message
			msg.Ack()
		}
	}
}

// createSubscription creates a NATS JetStream subscription
func (h *Handler) createSubscription(ctx context.Context, subject string, lastEventID string, lastCount int) (*nats.Subscription, error) {
	// Create consumer config
	consumerName := fmt.Sprintf("sse-%s-%d", sanitizeConsumerName(subject), time.Now().UnixNano())
	
	config := &nats.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: subject,
		MaxAckPending: 100,
		AckWait:       30 * time.Second,
		MaxDeliver:    1,
	}
	
	// If we have a last event ID, try to resume from there
	if lastEventID != "" {
		// For resumption, we'd need to store sequence numbers with event IDs
		// For now, we'll start from new messages
		config.DeliverPolicy = nats.DeliverNewPolicy
		slog.Info("Event resumption requested", "last_event_id", lastEventID)
	} else {
		// Get stream info to calculate start sequence
		streamInfo, err := h.js.StreamInfo(h.cfg.StreamName)
		if err != nil {
			return nil, fmt.Errorf("failed to get stream info: %w", err)
		}
		
		// Calculate start sequence for last N messages
		if streamInfo.State.Msgs > 0 {
			var startSeq uint64
			if streamInfo.State.LastSeq >= uint64(lastCount) {
				startSeq = streamInfo.State.LastSeq - uint64(lastCount) + 1
			} else {
				// Not enough messages, start from the beginning
				startSeq = streamInfo.State.FirstSeq
			}
			
			// Ensure we don't go before the first sequence
			if startSeq < streamInfo.State.FirstSeq {
				startSeq = streamInfo.State.FirstSeq
			}
			
			config.DeliverPolicy = nats.DeliverByStartSequencePolicy
			config.OptStartSeq = startSeq
			slog.Info("Starting from sequence", 
				"start_seq", startSeq, 
				"first_seq", streamInfo.State.FirstSeq,
				"last_seq", streamInfo.State.LastSeq, 
				"total_msgs", streamInfo.State.Msgs,
				"requested_last", lastCount)
		} else {
			// No messages, start with new ones
			config.DeliverPolicy = nats.DeliverNewPolicy
		}
	}
	
	// Create consumer
	_, err := h.js.AddConsumer(h.cfg.StreamName, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	
	// Create subscription
	sub, err := h.js.PullSubscribe(subject, consumerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription: %w", err)
	}
	
	// Clean up consumer when done
	go func() {
		<-ctx.Done()
		h.js.DeleteConsumer(h.cfg.StreamName, consumerName)
	}()
	
	return sub, nil
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
			"event_count", client.EventCount)
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
		totalEvents += client.EventCount
	}
	
	return map[string]interface{}{
		"total_clients":      len(h.clients),
		"total_events":       totalEvents,
		"clients_by_stream": streamStats,
	}
}

// sanitizeConsumerName creates a valid NATS consumer name
func sanitizeConsumerName(subject string) string {
	// Replace invalid characters with underscores
	name := strings.ReplaceAll(subject, ".", "_")
	name = strings.ReplaceAll(name, "*", "star")
	name = strings.ReplaceAll(name, ">", "all")
	return name
}