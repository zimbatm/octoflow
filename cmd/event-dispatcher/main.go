package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
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

	// Create publisher for republishing to entity-specific subjects
	publisher, err := natsInternal.NewPublisher(&cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	// Create durable consumer
	consumerName := "event-dispatcher"
	if err := natsInternal.EnsureConsumer(js, cfg.NATS.StreamName, &nats.ConsumerConfig{
		Durable:        consumerName,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubjects: natsInternal.PrimarySubjects(cfg.NATS.SubjectPrefix),
		MaxAckPending:  1000,
	}); err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait group for workers
	var wg sync.WaitGroup

	// Start subscription workers
	for i := 0; i < cfg.Processing.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			subscribeAndDispatch(ctx, js, cfg, publisher, consumerName, workerID)
		}(i)
	}

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
	cancel()

	// Wait for workers to finish
	wg.Wait()

	slog.Info("Shutdown complete")
}

// subscribeAndDispatch subscribes to events and dispatches them to entity-specific subjects
func subscribeAndDispatch(ctx context.Context, js nats.JetStreamContext, cfg *config.Config,
	publisher *natsInternal.Publisher, consumerName string, workerID int) {

	// Create subscription
	sub, err := js.PullSubscribe("", consumerName, nats.Bind(cfg.NATS.StreamName, consumerName))
	if err != nil {
		slog.Error("Failed to create subscription", "worker_id", workerID, "error", err)
		return
	}

	slog.Info("Worker started", "worker_id", workerID)

	for {
		select {
		case <-ctx.Done():
			slog.Info("Worker shutting down", "worker_id", workerID)
			return
		default:
			// Fetch messages
			msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
			if err != nil && err != nats.ErrTimeout {
				slog.Error("Error fetching messages", "worker_id", workerID, "error", err)
				// fetch fails at once while NATS reconnects; do not spin.
				time.Sleep(time.Second)
				continue
			}

			for _, msg := range msgs {
				// Process and dispatch message
				if err := processAndDispatch(msg, cfg, publisher); err != nil {
					slog.Error("Error processing message", "error", err)
					// NAK for retry
					msg.Nak()
				} else {
					// ACK the message
					msg.Ack()
				}
			}
		}
	}
}

// processAndDispatch processes an event and dispatches it to entity-specific subjects
func processAndDispatch(msg *nats.Msg, cfg *config.Config, publisher *natsInternal.Publisher) error {
	// Decode event
	var event events.GitHubEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Extract entities from the event
	entities, err := events.ExtractEventEntities(&event)
	if err != nil {
		slog.Warn("Failed to extract entities from event",
			"event_id", event.ID,
			"event_type", event.Event,
			"error", err)
		// Continue processing even if entity extraction fails
		entities = &events.EventEntities{}
	}

	// Build subjects to publish to
	subjects := buildDispatchSubjects(cfg.NATS.SubjectPrefix, &event, entities)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, subject := range subjects {
		// the stream deduplicates by message id across all subjects, so reusing
		// event.ID here would drop every fan-out copy as a duplicate of the original.
		msgID := event.ID + "@" + subject
		if err := publisher.PublishToSubject(ctx, subject, msg.Data, msgID); err != nil {
			return fmt.Errorf("publish to %s: %w", subject, err)
		}
		slog.Debug("Dispatched event",
			"subject", subject,
			"event_id", event.ID,
			"event_type", event.Event)
	}

	slog.Info("Dispatched event to entity subjects",
		"event_id", event.ID,
		"event_type", event.Event,
		"org_id", entities.OrganizationID,
		"repo_id", entities.RepositoryID,
		"user_count", len(entities.UserIDs),
		"pr_number", entities.PRNumber,
		"issue_number", entities.IssueNumber,
		"subject_count", len(subjects))

	return nil
}

// buildDispatchSubjects builds the list of subjects to publish an event to
func buildDispatchSubjects(prefix string, event *events.GitHubEvent, entities *events.EventEntities) []string {
	subjects := []string{}

	// Repository events
	if entities.RepositoryID != "" {
		subjects = append(subjects, fmt.Sprintf("%s.repo.%s.%s", prefix, entities.RepositoryID, event.Event))
	}

	// User events (for each involved user)
	for _, userID := range entities.UserIDs {
		// Specific event type
		subjects = append(subjects, fmt.Sprintf("%s.user.%s.%s", prefix, userID, event.Event))
	}

	// PR events
	if entities.PRNumber > 0 && entities.RepositoryID != "" {
		// Specific event type for PR
		subjects = append(subjects, fmt.Sprintf("%s.pr.%s.%d.%s", prefix, entities.RepositoryID, entities.PRNumber, event.Event))
		// All events for PR
		subjects = append(subjects, fmt.Sprintf("%s.pr.%s.%d", prefix, entities.RepositoryID, entities.PRNumber))
	}

	// Issue events
	if entities.IssueNumber > 0 && entities.RepositoryID != "" {
		// Specific event type for issue
		subjects = append(subjects, fmt.Sprintf("%s.issue.%s.%d.%s", prefix, entities.RepositoryID, entities.IssueNumber, event.Event))
		// All events for issue
		subjects = append(subjects, fmt.Sprintf("%s.issue.%s.%d", prefix, entities.RepositoryID, entities.IssueNumber))
	}

	return subjects
}
