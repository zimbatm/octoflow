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
		"Access-Control-Allow-Headers": "Content-Type, Cache-Control, X-Requested-With, " + cfg.AuthHeader,
		"Cache-Control":                "no-cache",
	}

	// Create HTTP mux
	mux := http.NewServeMux()

	// SSE endpoint
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		// Handle CORS preflight
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Check authentication if required
		if cfg.RequireAuth {
			auth := r.Header.Get(cfg.AuthHeader)
			if auth == "" {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			// TODO: Implement proper authentication validation
		}

		// Extract filter from query params
		filter := extractFilter(r)

		// Generate client ID
		clientID := fmt.Sprintf("client-%d", time.Now().UnixNano())

		// Register client (TODO: implement client tracking)
		_ = &Client{
			ID:       clientID,
			Filter:   filter,
			LastSeen: time.Now(),
		}

		// Serve SSE
		sseServer.ServeHTTP(w, r)
	})

	// Health check endpoint
	mux.HandleFunc("/health", healthHandler)

	// Stats endpoint
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		statsHandler(w, r, &Server{clients: make(map[string]*Client), clientMutex: sync.RWMutex{}})
	})

	server := &Server{
		config:    cfg,
		sseServer: sseServer,
		clients:   make(map[string]*Client),
		httpServer: &http.Server{
			Addr:         cfg.ListenAddr,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: cfg.ClientTimeout,
			IdleTimeout:  120 * time.Second,
		},
	}

	// Start client cleanup routine
	go server.cleanupClients()

	return server
}

// PublishEvent publishes an event to SSE clients
func (s *Server) PublishEvent(event *events.GitHubEvent) error {
	// Create SSE event
	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Create streams based on event properties
	streams := s.getStreamsForEvent(event)

	// Publish to relevant streams
	for _, stream := range streams {
		s.sseServer.Publish(stream, &sse.Event{
			ID:    []byte(event.ID),
			Event: []byte(event.Event),
			Data:  eventData,
		})
	}

	// Update client stats
	s.updateClientStats(event)

	return nil
}

// getStreamsForEvent determines which streams an event should be published to
func (s *Server) getStreamsForEvent(event *events.GitHubEvent) []string {
	streams := []string{"all"} // Default stream for all events

	// Add event type stream
	streams = append(streams, "event:"+event.Event)

	// Add repository stream if present
	if event.Repository != "" {
		streams = append(streams, "repo:"+event.Repository)
	}

	// Add organization stream if present
	if event.Organization != "" {
		streams = append(streams, "org:"+event.Organization)
	}

	// Add action-specific stream for certain events
	if event.Action != "" {
		streams = append(streams, fmt.Sprintf("%s:%s", event.Event, event.Action))
	}

	return streams
}

// updateClientStats updates statistics for connected clients
func (s *Server) updateClientStats(event *events.GitHubEvent) {
	s.clientMutex.Lock()
	defer s.clientMutex.Unlock()

	for _, client := range s.clients {
		if client.Filter == nil || client.Filter.Matches(event) {
			client.EventCount++
			client.LastSeen = time.Now()
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

// Start starts the SSE server
func (s *Server) Start() error {
	fmt.Printf("Starting SSE server on %s\n", s.config.ListenAddr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	fmt.Println("Shutting down SSE server...")
	s.sseServer.Close()
	return s.httpServer.Shutdown(ctx)
}

// extractFilter extracts event filter from query parameters
func extractFilter(r *http.Request) *events.EventFilter {
	filter := &events.EventFilter{}

	// Extract event types
	if eventTypes := r.URL.Query().Get("events"); eventTypes != "" {
		filter.Events = strings.Split(eventTypes, ",")
	}

	// Extract repositories
	if repos := r.URL.Query().Get("repos"); repos != "" {
		filter.Repositories = strings.Split(repos, ",")
	}

	// Extract organizations
	if orgs := r.URL.Query().Get("orgs"); orgs != "" {
		filter.Organizations = strings.Split(orgs, ",")
	}

	// Extract senders
	if senders := r.URL.Query().Get("senders"); senders != "" {
		filter.Senders = strings.Split(senders, ",")
	}

	// Extract actions
	if actions := r.URL.Query().Get("actions"); actions != "" {
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

// statsHandler handles statistics requests
func statsHandler(w http.ResponseWriter, r *http.Request, s *Server) {
	s.clientMutex.RLock()
	defer s.clientMutex.RUnlock()

	stats := struct {
		ConnectedClients int                    `json:"connected_clients"`
		Clients          map[string]interface{} `json:"clients"`
	}{
		ConnectedClients: len(s.clients),
		Clients:          make(map[string]interface{}),
	}

	for id, client := range s.clients {
		stats.Clients[id] = map[string]interface{}{
			"event_count": client.EventCount,
			"last_seen":   client.LastSeen,
			"has_filter":  client.Filter != nil,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
