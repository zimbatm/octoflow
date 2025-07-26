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
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/sse"
	"github.com/zimbatm/octoflow/pkg/events"
)

// SubscriptionManager manages multiple NATS subscriptions for different SSE endpoints
type SubscriptionManager struct {
	js          nats.JetStreamContext
	cfg         *config.Config
	sseServer   *sse.Server
	consumerMu  sync.Mutex
	consumers   map[string]*nats.ConsumerInfo
	cancelFuncs map[string]context.CancelFunc
}

func main() {
	// Load configuration
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create SSE server
	sseServer := sse.NewServer(&cfg.SSE)

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

	// Create subscription manager
	subManager := &SubscriptionManager{
		js:          js,
		cfg:         cfg,
		sseServer:   sseServer,
		consumers:   make(map[string]*nats.ConsumerInfo),
		cancelFuncs: make(map[string]context.CancelFunc),
	}

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait group for goroutines
	var wg sync.WaitGroup

	// Create router with enhanced SSE endpoints
	router := createEnhancedRouter(sseServer, subManager, cfg)

	// Start HTTP server
	httpServer := &http.Server{
		Addr:         cfg.SSE.ListenAddr,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: cfg.SSE.ClientTimeout,
		IdleTimeout:  120 * time.Second,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Printf("Starting enhanced SSE server on %s\n", cfg.SSE.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
			cancel()
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived signal: %v\n", sig)
	case <-ctx.Done():
		fmt.Println("\nShutting down due to error")
	}

	// Graceful shutdown
	fmt.Println("Starting graceful shutdown...")
	cancel()

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
	}

	// Shutdown SSE server
	if err := sseServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error shutting down SSE server: %v", err)
	}

	// Cancel all subscriptions
	subManager.cancelAll()

	// Wait for all goroutines
	wg.Wait()

	fmt.Println("Shutdown complete")
}

// createEnhancedRouter creates the HTTP router with multiple SSE endpoints
func createEnhancedRouter(sseServer *sse.Server, subManager *SubscriptionManager, cfg *config.Config) *mux.Router {
	router := mux.NewRouter()

	// Health check
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"healthy","service":"sse-server"}`)
	})

	// Stats endpoint
	router.HandleFunc("/stats", sseServer.StatsHandler)

	// All events (firehose)
	router.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		handleSSE(w, r, sseServer, subManager, "all", cfg.NATS.SubjectPrefix+".>", nil)
	})

	// Organization events
	router.HandleFunc("/events/org/{org_id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		orgID := vars["org_id"]
		subject := fmt.Sprintf("%s.org.%s.>", cfg.NATS.SubjectPrefix, orgID)
		handleSSE(w, r, sseServer, subManager, fmt.Sprintf("org-%s", orgID), subject, map[string]string{"org_id": orgID})
	})

	// Repository events
	router.HandleFunc("/events/repo/{repo_id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		repoID := vars["repo_id"]
		subject := fmt.Sprintf("%s.repo.%s.>", cfg.NATS.SubjectPrefix, repoID)
		handleSSE(w, r, sseServer, subManager, fmt.Sprintf("repo-%s", repoID), subject, map[string]string{"repo_id": repoID})
	})

	// User activity events
	router.HandleFunc("/events/user/{user_id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		userID := vars["user_id"]
		subject := fmt.Sprintf("%s.user.%s.*", cfg.NATS.SubjectPrefix, userID)
		handleSSE(w, r, sseServer, subManager, fmt.Sprintf("user-%s", userID), subject, map[string]string{"user_id": userID})
	})

	// PR-specific events
	router.HandleFunc("/events/pr/{repo_id}/{pr_number}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		repoID := vars["repo_id"]
		prNumber := vars["pr_number"]
		// Subscribe to both specific and general PR events
		subject := fmt.Sprintf("%s.pr.%s.%s.>", cfg.NATS.SubjectPrefix, repoID, prNumber)
		handleSSE(w, r, sseServer, subManager, fmt.Sprintf("pr-%s-%s", repoID, prNumber), subject, map[string]string{"repo_id": repoID, "pr_number": prNumber})
	})

	// Issue-specific events
	router.HandleFunc("/events/issue/{repo_id}/{issue_number}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		repoID := vars["repo_id"]
		issueNumber := vars["issue_number"]
		// Subscribe to both specific and general issue events
		subject := fmt.Sprintf("%s.issue.%s.%s.>", cfg.NATS.SubjectPrefix, repoID, issueNumber)
		handleSSE(w, r, sseServer, subManager, fmt.Sprintf("issue-%s-%s", repoID, issueNumber), subject, map[string]string{"repo_id": repoID, "issue_number": issueNumber})
	})

	// Event type filtering across all repos
	router.HandleFunc("/events/type/{event_type}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		eventType := vars["event_type"]
		// This requires subscribing to multiple subjects
		subject := fmt.Sprintf("%s.*.*.%s", cfg.NATS.SubjectPrefix, eventType)
		handleSSE(w, r, sseServer, subManager, fmt.Sprintf("type-%s", eventType), subject, map[string]string{"event_type": eventType})
	})

	// API endpoints for event history and statistics
	router.HandleFunc("/api/events", handleEventHistory)
	router.HandleFunc("/api/stats", handleEventStats)

	// Serve static files for dashboard
	router.PathPrefix("/").Handler(http.FileServer(http.Dir("./web")))

	return router
}

// handleSSE handles SSE connections for different subscription patterns
func handleSSE(w http.ResponseWriter, r *http.Request, sseServer *sse.Server, 
	subManager *SubscriptionManager, streamID string, natsSubject string, metadata map[string]string) {
	
	// Handle CORS preflight
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Extract query parameters for filtering
	eventTypes := r.URL.Query().Get("type")
	action := r.URL.Query().Get("action")
	// limit := r.URL.Query().Get("limit") // TODO: Implement limit

	// Create a unique consumer for this subscription pattern if needed
	ctx, cancel := context.WithCancel(context.Background())
	
	// Ensure subscription exists
	if err := subManager.ensureSubscription(ctx, streamID, natsSubject, metadata); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create subscription: %v", err), http.StatusInternalServerError)
		cancel()
		return
	}

	// Clean up when client disconnects
	defer func() {
		cancel()
		// Note: We keep the consumer alive for reuse
	}()

	// Create filter from query params
	filter := &events.EventFilter{}
	if eventTypes != "" {
		filter.Events = strings.Split(eventTypes, ",")
	}
	if action != "" {
		filter.Actions = strings.Split(action, ",")
	}

	// Serve SSE with the specific stream and filter
	sseServer.ServeSSE(w, r, streamID, filter)
}

// ensureSubscription ensures a NATS subscription exists for the given pattern
func (sm *SubscriptionManager) ensureSubscription(ctx context.Context, streamID string, subject string, metadata map[string]string) error {
	sm.consumerMu.Lock()
	defer sm.consumerMu.Unlock()

	// Check if consumer already exists
	if _, exists := sm.consumers[streamID]; exists {
		return nil
	}

	// Create durable consumer for this pattern
	consumerName := fmt.Sprintf("sse-%s", streamID)
	
	consumer, err := sm.js.AddConsumer(sm.cfg.NATS.StreamName, &nats.ConsumerConfig{
		Durable:       consumerName,
		DeliverPolicy: nats.DeliverNewPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: subject,
		MaxAckPending: 1000,
	})
	if err != nil {
		// Try to use existing consumer
		consumer, err = sm.js.ConsumerInfo(sm.cfg.NATS.StreamName, consumerName)
		if err != nil {
			return fmt.Errorf("failed to create/get consumer: %w", err)
		}
	}

	sm.consumers[streamID] = consumer

	// Start subscription worker
	subCtx, subCancel := context.WithCancel(ctx)
	sm.cancelFuncs[streamID] = subCancel

	go sm.subscribeAndStream(subCtx, streamID, consumerName, subject, metadata)

	return nil
}

// subscribeAndStream handles a specific NATS subscription
func (sm *SubscriptionManager) subscribeAndStream(ctx context.Context, streamID string, consumerName string, subject string, metadata map[string]string) {
	// Create subscription
	sub, err := sm.js.PullSubscribe(subject, consumerName)
	if err != nil {
		log.Printf("Failed to create subscription for %s: %v", streamID, err)
		return
	}

	fmt.Printf("Started subscription: %s (subject: %s)\n", streamID, subject)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Stopping subscription: %s\n", streamID)
			return
		default:
			// Fetch messages
			msgs, err := sub.Fetch(10, nats.MaxWait(1*time.Second))
			if err != nil {
				if err != nats.ErrTimeout {
					log.Printf("Error fetching messages for %s: %v", streamID, err)
				}
				continue
			}

			for _, msg := range msgs {
				// Process and stream message
				if err := sm.processAndStream(msg, streamID, metadata); err != nil {
					log.Printf("Error processing message: %v", err)
					msg.Nak()
				} else {
					msg.Ack()
				}
			}
		}
	}
}

// processAndStream processes a NATS message and streams it via SSE
func (sm *SubscriptionManager) processAndStream(msg *nats.Msg, streamID string, metadata map[string]string) error {
	// Decode event
	var event events.GitHubEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Add metadata to event for client-side filtering if needed
	if metadata != nil {
		// This could be used for client-side filtering or display
		event.Metadata = metadata
	}

	// Publish to SSE clients on this stream
	if err := sm.sseServer.PublishEventToStream(&event, streamID); err != nil {
		return fmt.Errorf("failed to publish SSE event: %w", err)
	}

	// Log event for monitoring
	logMsg := fmt.Sprintf("Streamed to %s: %s (%s)", streamID, event.ID, event.Event)
	if event.Repository != "" {
		logMsg += fmt.Sprintf(" - %s", event.Repository)
	}
	fmt.Println(logMsg)

	return nil
}

// cancelAll cancels all active subscriptions
func (sm *SubscriptionManager) cancelAll() {
	sm.consumerMu.Lock()
	defer sm.consumerMu.Unlock()

	for streamID, cancel := range sm.cancelFuncs {
		fmt.Printf("Cancelling subscription: %s\n", streamID)
		cancel()
	}
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