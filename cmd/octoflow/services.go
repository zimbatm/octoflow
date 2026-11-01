package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/dispatch"
	"github.com/zimbatm/octoflow/internal/ghevents"
	"github.com/zimbatm/octoflow/internal/metrics"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/publicingest"
	"github.com/zimbatm/octoflow/internal/sse"
	"github.com/zimbatm/octoflow/internal/storage"
	"github.com/zimbatm/octoflow/internal/webhook"
	"github.com/zimbatm/octoflow/pkg/events"
)

func webhookRoutes(cfg *config.Config, js nats.JetStreamContext, mux *http.ServeMux) error {
	if cfg.Webhook.Secret == "" {
		if !cfg.Webhook.AllowUnsigned {
			return errors.New("WEBHOOK_SECRET is not set; set it, or WEBHOOK_ALLOW_UNSIGNED=true to accept unsigned deliveries")
		}
		slog.Warn("WEBHOOK_SECRET is not set; unsigned deliveries are accepted")
	}
	webhook.Routes(mux, &cfg.Webhook, natsInternal.NewPublisher(js, &cfg.NATS))
	return nil
}

func runDispatcher(ctx context.Context, cfg *config.Config, js nats.JetStreamContext) error {
	const consumer = "event-dispatcher"
	if err := natsInternal.EnsureConsumer(js, cfg.NATS.StreamName, &nats.ConsumerConfig{
		Durable:        consumer,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubjects: natsInternal.PrimarySubjects(cfg.NATS.SubjectPrefix),
		MaxAckPending:  1000,
	}); err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	publisher := natsInternal.NewPublisher(js, &cfg.NATS)

	pullLoop(ctx, cfg, js, consumer, func(msg *nats.Msg) {
		_, err := dispatch.Dispatch(ctx, publisher, cfg.NATS.SubjectPrefix, msg.Data)
		switch {
		case errors.Is(err, dispatch.ErrUndecodable):
			metrics.DispatchedEvents.WithLabelValues("undecodable").Inc()
			terminate(msg, "Dropping undecodable message", err)
		case err != nil:
			metrics.DispatchedEvents.WithLabelValues("error").Inc()
			slog.Error("Error dispatching message", "subject", msg.Subject, "error", err)
			natsInternal.AckOrNak(msg, err)
		default:
			metrics.DispatchedEvents.WithLabelValues("ok").Inc()
			natsInternal.AckOrNak(msg, nil)
		}
	})
	return nil
}

func runS3(ctx context.Context, cfg *config.Config, js nats.JetStreamContext) error {
	if err := cfg.S3.Validate(); err != nil {
		return err
	}
	writer, err := storage.NewS3Writer(&cfg.S3)
	if err != nil {
		return err
	}

	const consumer = "s3-worker"
	if err := natsInternal.EnsureConsumer(js, cfg.NATS.StreamName, &nats.ConsumerConfig{
		Durable:        consumer,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubjects: natsInternal.PrimarySubjects(cfg.NATS.SubjectPrefix),
		// messages stay unacked until their batch is uploaded.
		AckWait:       cfg.S3.BatchTimeout + 2*time.Minute,
		MaxAckPending: 2 * cfg.S3.BatchSize,
	}); err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				writer.FlushIfDue()
			}
		}
	}()

	pullLoop(ctx, cfg, js, consumer, func(msg *nats.Msg) {
		var event events.GitHubEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			terminate(msg, "Dropping undecodable message", err)
			return
		}
		writer.AddEvent(&event, func(err error) {
			natsInternal.AckOrNak(msg, err)
		})
	})

	// acks through the connection, which main closes only after this returns.
	writer.Flush()
	return nil
}

func sseRoutes(cfg *config.Config, js nats.JetStreamContext, mux *http.ServeMux) error {
	sse.Routes(mux, sse.NewHandler(js, cfg.NATS.StreamName, cfg.SSE.MaxClients), cfg.NATS.SubjectPrefix)
	return nil
}

// dashboardRoutes serves WEB_DIR at /, so it goes last on a shared mux:
// the mux prefers the more specific patterns of the other services.
func dashboardRoutes(cfg *config.Config, _ nats.JetStreamContext, mux *http.ServeMux) error {
	webDir, err := filepath.Abs(cfg.Dashboard.WebDir)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(webDir, "index.html")); err != nil {
		return fmt.Errorf("WEB_DIR must hold index.html: %w", err)
	}
	slog.Info("Dashboard served", "web_dir", webDir)
	mux.Handle("/", http.FileServer(http.Dir(webDir)))
	return nil
}

func runPublicIngest(ctx context.Context, cfg *config.Config, js nats.JetStreamContext) error {
	pc := &cfg.PublicIngest
	if !publicIngestEnabled(cfg) {
		return errors.New("PUBLIC_REPOS is required, for example PUBLIC_REPOS=NixOS/nixpkgs")
	}
	state, err := publicingest.OpenState(js, cfg.NATS.StreamReplicas)
	if err != nil {
		return fmt.Errorf("failed to open state bucket: %w", err)
	}

	in := &publicingest.Ingest{
		GH:        ghevents.NewClient(pc.GitHubToken),
		Publisher: natsInternal.NewPublisher(js, &cfg.NATS),
		State:     state,
	}
	for _, name := range pc.Repos {
		info, err := in.GH.Repo(ctx, name)
		if err != nil {
			return fmt.Errorf("failed to look up %s: %w", name, err)
		}
		slog.Info("Watching repository", "repo", info.FullName, "repo_id", info.ID, "owner_id", info.Owner.ID)
		in.Repos = append(in.Repos, info)
	}

	var wg sync.WaitGroup
	if pc.EventsAPI {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in.RunEventsAPI(ctx, pc.EventsAPIInterval)
		}()
	}
	if pc.GHArchive {
		start := pc.GHArchiveStart
		if start.IsZero() {
			// the stream deduplicates for 24h only, so a first start does not
			// replay hours that the Events API may already have delivered long ago.
			start = time.Now().UTC().Add(-3 * time.Hour)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			in.RunGHArchive(ctx, pc.GHArchiveURL, start)
		}()
	}
	wg.Wait()
	return nil
}
