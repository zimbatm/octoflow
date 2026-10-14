package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/webhook"
)

func main() {
	// Load configuration
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

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
	server := webhook.NewServer(&cfg.Webhook, publisher)

	// Create wait group for goroutines
	var wg sync.WaitGroup

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start webhook server in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

	// Wait for all goroutines to finish
	wg.Wait()

	slog.Info("Shutdown complete")
}
