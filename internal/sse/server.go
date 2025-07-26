package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/r3labs/sse/v2"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/pkg/events"
)

// Server represents the SSE server
type Server struct {
	config      *config.SSEConfig
	sseServer   *sse.Server
	httpServer  *http.Server
	clients     map[string]*Client
	clientMutex sync.RWMutex
}

// Client represents a connected SSE client
type Client struct {
	ID         string
	StreamID   string
	Filter     *events.EventFilter
	LastSeen   time.Time
	EventCount int64
}

// NewServer creates a new SSE server
func NewServer(cfg *config.SSEConfig) *Server {
	// Create SSE server
	sseServer := sse.New()
	sseServer.AutoReplay = false
	sseServer.EventTTL = 30 * time.Second
	
	// Configure CORS
	sseServer.Headers = map[string]string{
		"Access-Control-Allow-Origin":  strings.Join(cfg.AllowedOrigins, ", "),
		"Access-Control-Allow-Methods": "GET, OPTIONS",
		"Access-Control-Allow-Headers": "Content-Type, Cache-Control, X-Requested-With",
		"Cache-Control":                "no-cache",
		"Connection":                   "keep-alive",
	}

	server := &Server{
		config:    cfg,
		sseServer: sseServer,
		clients:   make(map[string]*Client),
	}

	// Start client cleanup routine
	go server.cleanupClients()

	return server
}

// ServeSSE serves SSE for a specific stream with optional filtering
func (s *Server) ServeSSE(w http.ResponseWriter, r *http.Request, streamID string, filter *events.EventFilter) {
	// Generate client ID
	clientID := fmt.Sprintf("client-%s-%d", streamID, time.Now().UnixNano())
	
	// Register client
	s.registerClient(clientID, streamID, filter)
	defer s.unregisterClient(clientID)
	
	// Create custom streams for this client based on their filter
	streams := []string{streamID}
	if filter != nil && len(filter.Events) > 0 {
		// Add event-specific streams
		for _, evt := range filter.Events {
			streams = append(streams, fmt.Sprintf("%s:%s", streamID, evt))
		}
	}
	
	// Serve SSE with the specific streams
	s.sseServer.ServeHTTP(w, r)
}

// PublishEvent publishes an event to SSE clients (legacy method for backward compatibility)
func (s *Server) PublishEvent(event *events.GitHubEvent) error {
	// Publish to the default "all" stream
	return s.PublishEventToStream(event, "all")
}

// PublishEventToStream publishes an event to a specific stream
func (s *Server) PublishEventToStream(event *events.GitHubEvent, streamID string) error {
	// Create SSE event
	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Create base streams
	streams := []string{streamID}
	
	// Add event type specific stream
	streams = append(streams, fmt.Sprintf("%s:%s", streamID, event.Event))
	
	// Add action specific stream if applicable
	if event.Action != "" {
		streams = append(streams, fmt.Sprintf("%s:%s:%s", streamID, event.Event, event.Action))
	}

	// Publish to all relevant streams
	for _, stream := range streams {
		s.sseServer.Publish(stream, &sse.Event{
			ID:    []byte(event.ID),
			Event: []byte(event.Event),
			Data:  eventData,
		})
	}

	// Update client stats
	s.updateClientStats(streamID, event)

	return nil
}

// registerClient registers a new SSE client
func (s *Server) registerClient(clientID, streamID string, filter *events.EventFilter) {
	s.clientMutex.Lock()
	defer s.clientMutex.Unlock()
	
	s.clients[clientID] = &Client{
		ID:       clientID,
		StreamID: streamID,
		Filter:   filter,
		LastSeen: time.Now(),
	}
	
	fmt.Printf("Client connected: %s (stream: %s)\n", clientID, streamID)
}

// unregisterClient removes a client registration
func (s *Server) unregisterClient(clientID string) {
	s.clientMutex.Lock()
	defer s.clientMutex.Unlock()
	
	if client, exists := s.clients[clientID]; exists {
		fmt.Printf("Client disconnected: %s (stream: %s, events: %d)\n", 
			clientID, client.StreamID, client.EventCount)
		delete(s.clients, clientID)
	}
}

// updateClientStats updates statistics for connected clients
func (s *Server) updateClientStats(streamID string, event *events.GitHubEvent) {
	s.clientMutex.Lock()
	defer s.clientMutex.Unlock()

	for _, client := range s.clients {
		if client.StreamID == streamID {
			if client.Filter == nil || client.Filter.Matches(event) {
				client.EventCount++
				client.LastSeen = time.Now()
			}
		}
	}
}

// cleanupClients removes inactive clients
func (s *Server) cleanupClients() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		s.clientMutex.Lock()
		now := time.Now()
		for id, client := range s.clients {
			if now.Sub(client.LastSeen) > s.config.ClientTimeout {
				delete(s.clients, id)
				fmt.Printf("Removed inactive client: %s\n", id)
			}
		}
		s.clientMutex.Unlock()
	}
}

// StatsHandler handles statistics requests
func (s *Server) StatsHandler(w http.ResponseWriter, r *http.Request) {
	s.clientMutex.RLock()
	defer s.clientMutex.RUnlock()

	// Group clients by stream
	streamStats := make(map[string]int)
	totalEvents := int64(0)
	
	for _, client := range s.clients {
		streamStats[client.StreamID]++
		totalEvents += client.EventCount
	}

	stats := struct {
		TotalClients    int            `json:"total_clients"`
		TotalEvents     int64          `json:"total_events"`
		ClientsByStream map[string]int `json:"clients_by_stream"`
		Uptime          string         `json:"uptime"`
	}{
		TotalClients:    len(s.clients),
		TotalEvents:     totalEvents,
		ClientsByStream: streamStats,
		Uptime:          time.Since(time.Now()).String(), // This would need proper start time tracking
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// GetConnectedClients returns the number of connected clients
func (s *Server) GetConnectedClients() int {
	s.clientMutex.RLock()
	defer s.clientMutex.RUnlock()
	return len(s.clients)
}

// Start starts the SSE server (if used standalone)
func (s *Server) Start() error {
	if s.httpServer == nil {
		// Create HTTP server if not already created
		mux := http.NewServeMux()
		mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
			s.ServeSSE(w, r, "all", extractFilter(r))
		})
		mux.HandleFunc("/health", healthHandler)
		mux.HandleFunc("/stats", s.StatsHandler)
		
		s.httpServer = &http.Server{
			Addr:         s.config.ListenAddr,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: s.config.ClientTimeout,
			IdleTimeout:  120 * time.Second,
		}
	}
	
	fmt.Printf("Starting SSE server on %s\n", s.config.ListenAddr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	fmt.Println("Shutting down SSE server...")
	s.sseServer.Close()
	
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// extractFilter extracts event filter from query parameters
func extractFilter(r *http.Request) *events.EventFilter {
	filter := &events.EventFilter{}

	// Extract event types
	if eventTypes := r.URL.Query().Get("type"); eventTypes != "" {
		filter.Events = strings.Split(eventTypes, ",")
	}

	// Extract actions
	if actions := r.URL.Query().Get("action"); actions != "" {
		filter.Actions = strings.Split(actions, ",")
	}

	return filter
}

// healthHandler handles health check requests
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"healthy","service":"sse-server"}`)
}