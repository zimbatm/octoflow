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
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/sse"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	nc, err := natsInternal.Connect(&cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}
	if err := natsInternal.EnsureStream(js, &cfg.NATS); err != nil {
		log.Fatalf("Failed to initialize NATS stream: %v", err)
	}

	httpServer := &http.Server{
		Addr:              cfg.SSE.ListenAddr,
		Handler:           sse.Routes(sse.NewHandler(js, &cfg.NATS), cfg.NATS.SubjectPrefix),
		ReadHeaderTimeout: 5 * time.Second,
		// streams stay open: no write or idle timeout.
	}

	serverErr := make(chan error, 1)
	go func() {
		slog.Info("Starting SSE server", "address", cfg.SSE.ListenAddr)
		err := httpServer.ListenAndServe()
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
			slog.Error("HTTP server error", "error", err)
		}
	}

	slog.Info("Starting graceful shutdown")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("Error shutting down HTTP server", "error", err)
	}
	slog.Info("Shutdown complete")
}
