package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/sse"
	"github.com/zimbatm/octoflow/pkg/events"
)

func main() {
	// Load configuration
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Connect to NATS
	nc, err := natsInternal.Connect(&cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	// Initialize NATS schema
	schemaManager, err := natsInternal.NewSchemaManager(nc, &cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to create schema manager: %v", err)
	}
	if err := schemaManager.InitializeSchema(); err != nil {
		log.Fatalf("Failed to initialize NATS schema: %v", err)
	}

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}

	// Create SSE handler
	sseHandler := sse.NewHandler(nc, js, &cfg.NATS)

	// Create router
	router := createRouter(sseHandler, cfg)

	// Start HTTP server
	httpServer := &http.Server{
		Addr:         cfg.SSE.ListenAddr,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0, // No timeout for SSE connections
		IdleTimeout:  0, // No idle timeout for SSE connections
	}

	// Start server in background
	go func() {
		fmt.Printf("Starting SSE server on %s\n", cfg.SSE.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Graceful shutdown
	fmt.Println("\nStarting graceful shutdown...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
	}

	fmt.Println("Shutdown complete")
}

// createRouter creates the HTTP router with SSE endpoints
func createRouter(sseHandler *sse.Handler, cfg *config.Config) *mux.Router {
	router := mux.NewRouter()

	// Health check
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		if r.Method == "OPTIONS" {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"healthy","service":"sse-server"}`)
	})

	// Stats endpoint
	router.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		if r.Method == "OPTIONS" {
			return
		}

		stats := sseHandler.GetStats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	})

	// SSE endpoints

	// All events (firehose)
	router.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		handleSSE(w, r, sseHandler, "all", cfg.NATS.SubjectPrefix+".>")
	})

	// Organization events
	router.HandleFunc("/events/org/{org_id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		orgID := vars["org_id"]
		subject := fmt.Sprintf("%s.org.%s.>", cfg.NATS.SubjectPrefix, orgID)
		handleSSE(w, r, sseHandler, fmt.Sprintf("org-%s", orgID), subject)
	})

	// Repository events
	router.HandleFunc("/events/repo/{repo_id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		repoID := vars["repo_id"]
		subject := fmt.Sprintf("%s.repo.%s.>", cfg.NATS.SubjectPrefix, repoID)
		handleSSE(w, r, sseHandler, fmt.Sprintf("repo-%s", repoID), subject)
	})

	// User activity events
	router.HandleFunc("/events/user/{user_id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		userID := vars["user_id"]
		subject := fmt.Sprintf("%s.user.%s.*", cfg.NATS.SubjectPrefix, userID)
		handleSSE(w, r, sseHandler, fmt.Sprintf("user-%s", userID), subject)
	})

	// PR-specific events
	router.HandleFunc("/events/pr/{repo_id}/{pr_number}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		repoID := vars["repo_id"]
		prNumber := vars["pr_number"]
		subject := fmt.Sprintf("%s.pr.%s.%s.>", cfg.NATS.SubjectPrefix, repoID, prNumber)
		handleSSE(w, r, sseHandler, fmt.Sprintf("pr-%s-%s", repoID, prNumber), subject)
	})

	// Issue-specific events
	router.HandleFunc("/events/issue/{repo_id}/{issue_number}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		repoID := vars["repo_id"]
		issueNumber := vars["issue_number"]
		subject := fmt.Sprintf("%s.issue.%s.%s.>", cfg.NATS.SubjectPrefix, repoID, issueNumber)
		handleSSE(w, r, sseHandler, fmt.Sprintf("issue-%s-%s", repoID, issueNumber), subject)
	})

	// Event type filtering across all repos
	router.HandleFunc("/events/type/{event_type}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		eventType := vars["event_type"]
		subject := fmt.Sprintf("%s.*.*.%s", cfg.NATS.SubjectPrefix, eventType)
		handleSSE(w, r, sseHandler, fmt.Sprintf("type-%s", eventType), subject)
	})

	// API endpoints
	router.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		if r.Method == "OPTIONS" {
			return
		}
		handleEventHistory(w, r)
	})

	router.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		if r.Method == "OPTIONS" {
			return
		}
		handleEventStats(w, r)
	})

	return router
}

// setCORSHeaders sets CORS headers for cross-origin requests
func setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Cache-Control, X-Requested-With, Last-Event-ID")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Type")
}

// handleSSE handles SSE connections
func handleSSE(w http.ResponseWriter, r *http.Request, handler *sse.Handler, streamID, natsSubject string) {
	// Set CORS headers
	setCORSHeaders(w)

	// Handle OPTIONS
	if r.Method == "OPTIONS" {
		return
	}

	// Create filter from query params
	filter := createFilterFromQuery(r)

	// Let the SSE handler take over
	handler.ServeHTTP(w, r, streamID, natsSubject, filter)
}

// createFilterFromQuery creates an event filter from query parameters
func createFilterFromQuery(r *http.Request) *events.EventFilter {
	eventTypes := r.URL.Query().Get("type")
	actions := r.URL.Query().Get("action")

	if eventTypes == "" && actions == "" {
		return nil
	}

	filter := &events.EventFilter{}

	if eventTypes != "" {
		filter.Events = strings.Split(eventTypes, ",")
	}

	if actions != "" {
		filter.Actions = strings.Split(actions, ",")
	}

	return filter
}

// handleEventHistory provides historical event data via REST API
func handleEventHistory(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement event history retrieval from NATS
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"events":[],"message":"Event history API coming soon"}`)
}

// handleEventStats provides event statistics via REST API
func handleEventStats(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement event statistics
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"total_events":0,"message":"Event statistics API coming soon"}`)
}
