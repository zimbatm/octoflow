package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/webhook"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	if cfg.Webhook.Secret == "" {
		if !cfg.Webhook.AllowUnsigned {
			log.Fatal("WEBHOOK_SECRET is not set. Set it, or set WEBHOOK_ALLOW_UNSIGNED=true to accept unsigned deliveries")
		}
		slog.Warn("WEBHOOK_SECRET is not set; unsigned deliveries are accepted")
	}

	nc, err := nats.Connect(&cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}
	if err := nats.EnsureStream(js, &cfg.NATS); err != nil {
		log.Fatalf("Failed to initialize NATS stream: %v", err)
	}

	server := webhook.NewServer(&cfg.Webhook, nats.NewPublisher(js, &cfg.NATS))

	serverErr := make(chan error, 1)
	go func() {
		err := server.Start()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("Received signal", "signal", sig)
	case err := <-serverErr:
		if err != nil {
			slog.Error("Webhook server error", "error", err)
		}
	}

	slog.Info("Starting graceful shutdown")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("Error shutting down webhook server", "error", err)
	}
	slog.Info("Shutdown complete")
}
