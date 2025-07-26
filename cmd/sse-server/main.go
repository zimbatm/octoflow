package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
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

	// Create durable consumer for SSE
	consumerName := "sse-server"
	subject := cfg.NATS.SubjectPrefix + ".>"

	// Wait for stream to exist (webhook receiver creates it)
	var consumer *nats.ConsumerInfo
	for attempts := 0; attempts < 30; attempts++ {
		// Try to create consumer
		consumer, err = js.AddConsumer(cfg.NATS.StreamName, &nats.ConsumerConfig{
			Durable:       consumerName,
			DeliverPolicy: nats.DeliverNewPolicy, // Only new messages for real-time
			AckPolicy:     nats.AckExplicitPolicy,
			FilterSubject: subject,
			MaxAckPending: 1000,
		})
		if err == nil {
			break
		}

		// Try to update existing consumer
		consumer, err = js.UpdateConsumer(cfg.NATS.StreamName, &nats.ConsumerConfig{
			Durable:       consumerName,
			DeliverPolicy: nats.DeliverNewPolicy,
			AckPolicy:     nats.AckExplicitPolicy,
			FilterSubject: subject,
			MaxAckPending: 1000,
		})
		if err == nil {
			break
		}

		// If stream doesn't exist, wait and retry
		if err.Error() == "nats: stream not found" {
			fmt.Printf("Waiting for stream %s to be created...\n", cfg.NATS.StreamName)
			time.Sleep(2 * time.Second)
			continue
		}

		// Other error, fail
		log.Fatalf("Failed to create/update consumer: %v", err)
	}

	if consumer == nil {
		log.Fatalf("Failed to create consumer after 30 attempts")
	}

	fmt.Printf("Using consumer: %s\n", consumerName)

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait group for goroutines
	var wg sync.WaitGroup

	// Start SSE server
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sseServer.Start(); err != nil {
			log.Printf("SSE server error: %v", err)
			cancel()
		}
	}()

	// Start NATS subscription
	wg.Add(1)
	go func() {
		defer wg.Done()
		subscribeAndStream(ctx, js, cfg, sseServer, consumerName)
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

	// Shutdown SSE server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := sseServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error shutting down SSE server: %v", err)
	}

	// Wait for all goroutines
	wg.Wait()

	fmt.Println("Shutdown complete")
}

// subscribeAndStream subscribes to NATS and streams events via SSE
func subscribeAndStream(ctx context.Context, js nats.JetStreamContext, cfg *config.Config,
	sseServer *sse.Server, consumerName string) {

	// Create subscription - for pull consumers, we can use any subject or wildcard
	// The consumer already has the filter configured
	sub, err := js.PullSubscribe(cfg.NATS.SubjectPrefix+".>", consumerName)
	if err != nil {
		log.Printf("Failed to create subscription: %v", err)
		return
	}

	fmt.Println("Started streaming events from NATS to SSE")

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping NATS subscription")
			return
		default:
			// Fetch messages with short timeout for real-time streaming
			msgs, err := sub.Fetch(10, nats.MaxWait(1*time.Second))
			if err != nil {
				if err != nats.ErrTimeout {
					log.Printf("Error fetching messages: %v", err)
				}
				continue
			}

			for _, msg := range msgs {
				// Process and stream message
				if err := processAndStream(msg, cfg, sseServer); err != nil {
					log.Printf("Error processing message: %v", err)
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

// processAndStream processes a NATS message and streams it via SSE
func processAndStream(msg *nats.Msg, cfg *config.Config, sseServer *sse.Server) error {
	// Decode event
	var event events.GitHubEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Apply SSE filter if configured
	if cfg.Processing.SSEFilter != nil && !cfg.Processing.SSEFilter.Matches(&event) {
		// Skip this event
		return nil
	}

	// Publish to SSE clients
	if err := sseServer.PublishEvent(&event); err != nil {
		return fmt.Errorf("failed to publish SSE event: %w", err)
	}

	// Log event for monitoring
	fmt.Printf("Streamed event: %s (%s) - %s\n",
		event.ID, event.Event, event.Repository)

	return nil
}
