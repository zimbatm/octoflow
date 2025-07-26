package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/retry"
	"github.com/zimbatm/octoflow/internal/webhook"
	"github.com/zimbatm/octoflow/pkg/events"
)

func main() {
	// Load configuration
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create event channel with buffer
	eventCh := make(chan *events.GitHubEvent, 1000)

	// Create NATS connection
	nc, err := nats.Connect(&cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	// Initialize NATS schema
	schemaManager, err := nats.NewSchemaManager(nc, &cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to create schema manager: %v", err)
	}
	if err := schemaManager.InitializeSchema(); err != nil {
		log.Fatalf("Failed to initialize NATS schema: %v", err)
	}

	// Create NATS publisher
	publisher, err := nats.NewPublisher(&cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to create NATS publisher: %v", err)
	}
	defer publisher.Close()

	// Create webhook server
	server := webhook.NewServer(&cfg.Webhook, eventCh)

	// Create wait group for goroutines
	var wg sync.WaitGroup

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start event processor
	wg.Add(1)
	go func() {
		defer wg.Done()
		processEvents(ctx, eventCh, publisher)
	}()

	// Start webhook server in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Start(); err != nil {
			slog.Error("Webhook server error", "error", err)
			cancel()
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("Received signal", "signal", sig)
	case <-ctx.Done():
		slog.Info("Shutting down due to error")
	}

	// Graceful shutdown
	slog.Info("Starting graceful shutdown")

	// Cancel context to stop processors
	cancel()

	// Shutdown webhook server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("Error shutting down webhook server", "error", err)
	}

	// Close event channel
	close(eventCh)

	// Wait for all goroutines to finish
	wg.Wait()

	slog.Info("Shutdown complete")
}

// processEvents processes events from the channel and publishes immediately
func processEvents(ctx context.Context, eventCh <-chan *events.GitHubEvent, publisher *nats.Publisher) {
	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-eventCh:
			if !ok {
				return
			}

			// Publish with retry logic
			err := retry.Do(ctx, func(ctx context.Context) error {
				return publisher.PublishEvent(ctx, event)
			}, retry.DefaultConfig())

			if err != nil {
				slog.Error("Failed to publish event after retries",
					"event_id", event.ID,
					"event_type", event.Event,
					"error", err)
				// Event is lost - in production, this should write to a dead letter queue
				// or persistent storage for manual recovery
			} else {
				slog.Info("Published event",
					"event_id", event.ID,
					"event_type", event.Event,
					"received_at", event.ReceivedAt.Format(time.RFC3339))
			}
		}
	}
}
