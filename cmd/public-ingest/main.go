// public-ingest publishes events of repositories that we cannot attach a
// webhook to, from the GitHub Events API and from GH Archive, into the same
// NATS subjects as the webhook receiver. See internal/publicingest.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/ghevents"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/publicingest"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	names := splitList(os.Getenv("PUBLIC_REPOS"))
	if len(names) == 0 {
		log.Fatalf("PUBLIC_REPOS is required, for example PUBLIC_REPOS=NixOS/nixpkgs")
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
	state, err := publicingest.OpenState(js, cfg.NATS.StreamReplicas)
	if err != nil {
		log.Fatalf("Failed to open state bucket: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	in := &publicingest.Ingest{
		GH:        ghevents.NewClient(os.Getenv("GITHUB_TOKEN")),
		Publisher: natsInternal.NewPublisher(js, &cfg.NATS),
		State:     state,
	}
	for _, name := range names {
		info, err := in.GH.Repo(ctx, name)
		if err != nil {
			log.Fatalf("Failed to look up %s: %v", name, err)
		}
		slog.Info("Watching repository", "repo", info.FullName, "repo_id", info.ID, "owner_id", info.Owner.ID)
		in.Repos = append(in.Repos, info)
	}

	var wg sync.WaitGroup
	if envBool("EVENTS_API_ENABLED", true) {
		interval := envDuration("EVENTS_API_INTERVAL", time.Minute)
		wg.Add(1)
		go func() {
			defer wg.Done()
			in.RunEventsAPI(ctx, interval)
		}()
	}
	if envBool("GHARCHIVE_ENABLED", true) {
		base := getenv("GHARCHIVE_URL", "https://data.gharchive.org")
		// the stream deduplicates for 24h only, so a first start does not
		// replay hours that the Events API may already have delivered long ago.
		start := time.Now().UTC().Add(-3 * time.Hour)
		if s := os.Getenv("GHARCHIVE_START"); s != "" {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				log.Fatalf("GHARCHIVE_START: %v", err)
			}
			start = t
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			in.RunGHArchive(ctx, base, start)
		}()
	}
	wg.Wait()
	slog.Info("Shutdown complete")
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if b, err := strconv.ParseBool(os.Getenv(key)); err == nil {
		return b
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return d
	}
	return def
}
