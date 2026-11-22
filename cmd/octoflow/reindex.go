package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/reindex"
)

// runReindex is the reindex subcommand. It returns the exit status.
func runReindex(args []string) int {
	fs := flag.NewFlagSet("octoflow reindex", flag.ContinueOnError)
	dryRun := fs.Bool("dry-run", false, "read the stream and count, write nothing")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		fs.Usage()
		return 2
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		fatal("Failed to load configuration", err)
	}
	setupLogging(cfg.Log)
	nc, js, err := natsInternal.ConnectJetStream(&cfg.NATS)
	if err != nil {
		fatal("Failed to connect to NATS", err)
	}
	defer nc.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("Reading the stream", "stream", cfg.NATS.StreamName, "dry_run", *dryRun)
	st, err := reindex.Run(ctx, js, cfg.NATS.StreamName, cfg.NATS.SubjectPrefix, cfg.NATS.StreamReplicas, cfg.NATS.StreamMaxAge, *dryRun)
	fmt.Printf("reindex: events=%d heads=%d minutes=%d raised=%d\n", st.Events, st.Heads, st.Minutes, st.Raised)
	if err != nil {
		slog.Error("Reindex stopped", "error", err)
		return 1
	}
	return 0
}
