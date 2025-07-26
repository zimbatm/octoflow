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
	"github.com/zimbatm/octoflow/internal/retry"
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

	// Create publisher for ordered stream
	publisher, err := natsInternal.NewPublisher(&cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	// Create reorder buffer
	reorderBuffer := events.NewEventReorderBuffer(cfg.Processing.ReorderWindow)

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait group for goroutines
	var wg sync.WaitGroup

	// Start consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeAndReorder(ctx, js, &cfg.NATS, reorderBuffer, publisher, &wg)
	}()

	// Start periodic flush
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				// Final flush
				flushToOrderedStream(ctx, reorderBuffer, publisher, &cfg.NATS)
				return
			case <-ticker.C:
				if reorderBuffer.ShouldFlush() {
					flushToOrderedStream(ctx, reorderBuffer, publisher, &cfg.NATS)
				}
			}
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
	cancel()

	// Wait for goroutines
	wg.Wait()

	slog.Info("Shutdown complete")
}

func consumeAndReorder(ctx context.Context, js nats.JetStreamContext, cfg *config.NATSConfig,
	reorderBuffer *events.EventReorderBuffer, publisher *natsInternal.Publisher, wg *sync.WaitGroup) {

	// Create durable consumer on raw stream
	consumerName := "reorder-worker"
	_, err := js.AddConsumer(cfg.StreamName, &nats.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
		FilterSubject: cfg.SubjectPrefix + ".raw.>",
	})
	if err != nil && err != nats.ErrConsumerNameAlreadyInUse {
		log.Printf("Failed to create consumer: %v", err)
		return
	}

	// Subscribe to raw events
	sub, err := js.PullSubscribe(cfg.SubjectPrefix+".raw.>", consumerName,
		nats.Bind(cfg.StreamName, consumerName))
	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
		return
	}

	slog.Info("Started consuming from raw stream")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Fetch messages
			msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
			if err != nil && err != nats.ErrTimeout {
				log.Printf("Error fetching messages: %v", err)
				continue
			}

			for _, msg := range msgs {
				// Parse event
				var event events.GitHubEvent
				if err := json.Unmarshal(msg.Data, &event); err != nil {
					log.Printf("Failed to unmarshal event: %v", err)
					msg.Nak()
					continue
				}

				// Add to reorder buffer
				reorderBuffer.Add(&event)

				// Acknowledge message
				msg.Ack()
			}
		}
	}
}

func flushToOrderedStream(ctx context.Context, reorderBuffer *events.EventReorderBuffer,
	publisher *natsInternal.Publisher, cfg *config.NATSConfig) {

	orderedEvents := reorderBuffer.Flush()
	if len(orderedEvents) == 0 {
		return
	}

	slog.Info("Flushing events to ordered stream", "count", len(orderedEvents))

	for _, event := range orderedEvents {
		// Modify the publish method to use ordered subject
		orderedSubject := buildOrderedSubject(cfg.SubjectPrefix, event)

		// Serialize event
		data, err := json.Marshal(event)
		if err != nil {
			log.Printf("Failed to marshal event %s: %v", event.ID, err)
			continue
		}

		// Publish to ordered stream with retry
		err = retry.Do(ctx, func(ctx context.Context) error {
			return publisher.PublishToSubject(ctx, orderedSubject, data, event.ID)
		}, retry.DefaultConfig())

		if err != nil {
			slog.Error("Failed to publish ordered event after retries",
				"event_id", event.ID,
				"error", err)
		} else {
			slog.Info("Published ordered event",
				"event_id", event.ID,
				"event_type", event.Event,
				"timestamp", event.GithubTimestamp.Format(time.RFC3339))
		}
	}
}

func buildOrderedSubject(prefix string, event *events.GitHubEvent) string {
	// Use "ordered" namespace to distinguish from raw events
	return fmt.Sprintf("%s.ordered.%s", prefix, event.Event)
}
