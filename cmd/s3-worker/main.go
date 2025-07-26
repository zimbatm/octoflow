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
	"github.com/zimbatm/octoflow/internal/storage"
	"github.com/zimbatm/octoflow/pkg/events"
)

func main() {
	// Load configuration
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create S3 writer
	s3Writer, err := storage.NewS3Writer(&cfg.S3)
	if err != nil {
		log.Fatalf("Failed to create S3 writer: %v", err)
	}
	defer s3Writer.Close()

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

	// Create durable consumer
	consumerName := "s3-worker"
	subject := cfg.NATS.SubjectPrefix + ".>"

	// Wait for stream to exist and create consumer
	var consumerInfo *nats.ConsumerInfo
	for attempts := 0; attempts < 30; attempts++ {
		// Check if consumer exists
		consumerInfo, err = js.ConsumerInfo(cfg.NATS.StreamName, consumerName)
		if err == nil {
			slog.Info("Using existing consumer",
				"consumer", consumerName,
				"pending", consumerInfo.NumPending)
			break
		}

		// Try to create consumer
		consumerInfo, err = js.AddConsumer(cfg.NATS.StreamName, &nats.ConsumerConfig{
			Durable:       consumerName,
			DeliverPolicy: nats.DeliverAllPolicy,
			AckPolicy:     nats.AckExplicitPolicy,
			FilterSubject: subject,
			MaxAckPending: 1000,
		})
		if err == nil {
			slog.Info("Created consumer", "consumer", consumerName)
			break
		}

		// If stream doesn't exist, wait and retry
		if err.Error() == "nats: stream not found" {
			slog.Info("Waiting for stream to be created", "stream", cfg.NATS.StreamName)
			time.Sleep(2 * time.Second)
			continue
		}

		// Other error, fail
		log.Fatalf("Failed to create consumer: %v", err)
	}

	if consumerInfo == nil {
		log.Fatalf("Failed to create consumer after 30 attempts")
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
			subscribeAndProcess(ctx, js, cfg, s3Writer, consumerName, workerID)
		}(i)
	}

	// Start periodic flush
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s3Writer.Flush(); err != nil {
					slog.Error("Error flushing batch", "error", err)
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

	// Wait for workers to finish
	wg.Wait()

	// Final flush
	if err := s3Writer.Flush(); err != nil {
		slog.Error("Error during final flush", "error", err)
	}

	slog.Info("Shutdown complete")
}

// subscribeAndProcess subscribes to NATS and processes events
func subscribeAndProcess(ctx context.Context, js nats.JetStreamContext, cfg *config.Config,
	s3Writer *storage.S3Writer, consumerName string, workerID int) {

	// Create subscription - for pull consumers, we can use any subject or wildcard
	// The consumer already has the filter configured
	sub, err := js.PullSubscribe(cfg.NATS.SubjectPrefix+".>", consumerName)
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
			msgs, err := sub.Fetch(10, nats.MaxWait(5*time.Second))
			if err != nil {
				if err != nats.ErrTimeout {
					slog.Error("Error fetching messages", "worker_id", workerID, "error", err)
				}
				continue
			}

			for _, msg := range msgs {
				// Process message
				if err := processMessage(msg, cfg, s3Writer); err != nil {
					slog.Error("Error processing message",
						"worker_id", workerID,
						"error", err)
					// NAK the message for retry
					msg.Nak()
				} else {
					// ACK the message
					msg.Ack()
				}
			}
		}
	}
}

// processMessage processes a single NATS message
func processMessage(msg *nats.Msg, cfg *config.Config, s3Writer *storage.S3Writer) error {
	// Decode event
	var event events.GitHubEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Apply S3 filter if configured
	if cfg.Processing.S3Filter != nil && !cfg.Processing.S3Filter.Matches(&event) {
		// Skip this event
		return nil
	}

	// Add to S3 writer
	if err := s3Writer.AddEvent(&event); err != nil {
		return fmt.Errorf("failed to add event to batch: %w", err)
	}

	return nil
}
