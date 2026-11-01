package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/restore"
	"github.com/zimbatm/octoflow/internal/storage"
	"github.com/zimbatm/octoflow/pkg/events"
)

// runRestore is the restore subcommand. It returns the exit status.
func runRestore(args []string) int {
	fs := flag.NewFlagSet("octoflow restore", flag.ContinueOnError)
	from := fs.String("from", "", "first day to restore, YYYY-MM-DD in UTC, by received time (required)")
	to := fs.String("to", "", "last day to restore, inclusive (default: today)")
	eventTypes := fs.String("event", "", "comma-separated event types to restore (default: all)")
	repo := fs.String("repo", "", "restore only this repository id")
	org := fs.String("org", "", "restore only this organization id")
	dryRun := fs.Bool("dry-run", false, "read the archive and count, publish nothing")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *from == "" {
		fmt.Fprintln(os.Stderr, "octoflow restore: --from is required")
		fs.Usage()
		return 2
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to load configuration:", err)
		return 1
	}
	setupLogging(cfg.Log)

	opts := restore.Options{KeyPrefix: cfg.S3.KeyPrefix, RepoID: *repo, OrgID: *org, DryRun: *dryRun}
	if opts.From, err = time.Parse("2006-01-02", *from); err != nil {
		fmt.Fprintln(os.Stderr, "octoflow restore: --from:", err)
		return 2
	}
	opts.To = time.Now().UTC()
	if *to != "" {
		if opts.To, err = time.Parse("2006-01-02", *to); err != nil {
			fmt.Fprintln(os.Stderr, "octoflow restore: --to:", err)
			return 2
		}
	}
	if opts.To.Before(opts.From) {
		fmt.Fprintln(os.Stderr, "octoflow restore: --to is before --from")
		return 2
	}
	if *eventTypes != "" {
		opts.Events = strings.Split(*eventTypes, ",")
	}
	if err := cfg.S3.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	store, err := storage.NewObjectStore(&cfg.S3)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var pub restore.Publisher = noPublisher{}
	if !*dryRun {
		nc, err := natsInternal.Connect(&cfg.NATS)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to connect to NATS:", err)
			return 1
		}
		defer nc.Close()
		js, err := nc.JetStream()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to create JetStream context:", err)
			return 1
		}
		if err := natsInternal.EnsureStream(js, &cfg.NATS); err != nil {
			fmt.Fprintln(os.Stderr, "Failed to initialize NATS stream:", err)
			return 1
		}
		pub = natsInternal.NewPublisher(js, &cfg.NATS)
	}

	slog.Info("Restoring from the archive", "bucket", cfg.S3.Bucket, "prefix", opts.KeyPrefix,
		"from", opts.From.Format("2006-01-02"), "to", opts.To.Format("2006-01-02"), "dry_run", *dryRun)
	st, err := restore.Run(ctx, store, pub, opts)
	fmt.Printf("restore: files=%d rows=%d matched=%d published=%d duplicates=%d\n",
		st.Files, st.Rows, st.Matched, st.Published, st.Duplicates)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Restore stopped:", err)
		return 1
	}
	return 0
}

// noPublisher stands in during a dry run, which never publishes.
type noPublisher struct{}

func (noPublisher) Publish(context.Context, *events.GitHubEvent) (bool, error) {
	return false, errors.New("dry run")
}
