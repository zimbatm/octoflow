package main

import (
	"context"
	"encoding/json"
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
	if err := cfg.S3.Validate(); err != nil {
		log.Fatalf("Invalid S3 configuration: %v", err)
	}

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
	if err := natsInternal.EnsureConsumer(js, cfg.NATS.StreamName, &nats.ConsumerConfig{
		Durable:        consumerName,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubjects: natsInternal.PrimarySubjects(cfg.NATS.SubjectPrefix),
		// messages stay unacked until their batch is uploaded.
		AckWait:       cfg.S3.BatchTimeout + 2*time.Minute,
		MaxAckPending: 2 * cfg.S3.BatchSize,
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
				s3Writer.FlushIfDue()
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
			msgs, err := sub.Fetch(10, nats.MaxWait(5*time.Second))
			if err != nil {
				if err != nats.ErrTimeout {
					slog.Error("Error fetching messages", "worker_id", workerID, "error", err)
					// fetch fails at once while NATS reconnects; do not spin.
					time.Sleep(time.Second)
				}
				continue
			}

			for _, msg := range msgs {
				processMessage(msg, cfg, s3Writer)
			}
		}
	}
}

// processMessage leaves the ack to the S3 writer, which acks after upload.
func processMessage(msg *nats.Msg, cfg *config.Config, s3Writer *storage.S3Writer) {
	var event events.GitHubEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		slog.Error("Dropping undecodable message", "subject", msg.Subject, "error", err)
		if err := msg.Term(); err != nil {
			slog.Warn("Failed to terminate message", "subject", msg.Subject, "error", err)
		}
		return
	}

	if cfg.Processing.S3Filter != nil && !cfg.Processing.S3Filter.Matches(&event) {
		natsInternal.AckOrNak(msg, nil)
		return
	}

	s3Writer.AddEvent(&event, func(err error) {
		natsInternal.AckOrNak(msg, err)
	})
}
