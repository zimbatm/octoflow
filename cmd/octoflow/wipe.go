package main

import (
	"context"
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
	"github.com/zimbatm/octoflow/internal/storage"
	"github.com/zimbatm/octoflow/internal/wipe"
)

// runWipe is the wipe subcommand. It returns the exit status.
func runWipe(args []string) int {
	fs := flag.NewFlagSet("octoflow wipe", flag.ContinueOnError)
	ids := fs.String("id", "", "comma-separated delivery ids")
	eventTypes := fs.String("event", "", "comma-separated event types")
	repo := fs.String("repo", "", "repository id")
	org := fs.String("org", "", "organization id")
	user := fs.String("user", "", "user id, as sender, author, assignee or reviewer")
	pr := fs.Int("pr", 0, "pull request number, with --repo")
	issue := fs.Int("issue", 0, "issue number, with --repo")
	from := fs.String("from", "", "first day, YYYY-MM-DD in UTC, by received time")
	to := fs.String("to", "", "last day, inclusive")
	dryRun := fs.Bool("dry-run", false, "count the matching events, delete nothing")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	usageErr := func(msg string, err error) int {
		fmt.Fprintln(os.Stderr, "octoflow wipe:", msg, err)
		fs.Usage()
		return 2
	}

	opts := wipe.Options{RepoID: *repo, OrgID: *org, UserID: *user, PR: *pr, Issue: *issue}
	var err error
	if *from != "" {
		if opts.From, err = time.Parse(time.DateOnly, *from); err != nil {
			return usageErr("--from:", err)
		}
	}
	if *to != "" {
		if opts.To, err = time.Parse(time.DateOnly, *to); err != nil {
			return usageErr("--to:", err)
		}
		if opts.From.IsZero() {
			return usageErr("--to needs --from", nil)
		}
		if opts.To.Before(opts.From) {
			return usageErr("--to is before --from", nil)
		}
	}
	if *ids != "" {
		opts.IDs = strings.Split(*ids, ",")
	}
	if *eventTypes != "" {
		opts.Events = strings.Split(*eventTypes, ",")
	}
	if opts.Empty() {
		return usageErr("at least one selector is required; a wipe of everything is not supported", nil)
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		fatal("Failed to load configuration", err)
	}
	setupLogging(cfg.Log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nc, js, err := natsInternal.ConnectJetStream(&cfg.NATS)
	if err != nil {
		fatal("Failed to connect to NATS", err)
	}
	defer nc.Close()

	slog.Info("Wiping events", "dry_run", *dryRun, "ids", opts.IDs, "events", opts.Events, "repo", opts.RepoID,
		"org", opts.OrgID, "user", opts.UserID, "pr", opts.PR, "issue", opts.Issue, "from", *from, "to", *to)
	ss, err := wipe.Stream(ctx, js, cfg.NATS.StreamName, cfg.NATS.SubjectPrefix, opts, *dryRun)
	fmt.Printf("wipe stream: scanned=%d matched=%d events=%d deleted=%d\n", ss.Scanned, ss.Matched, ss.Events, ss.Deleted)
	if err != nil {
		slog.Error("Stream wipe stopped", "error", err)
		return 1
	}

	// the archive is wiped when it is configured, so one command cleans
	// everything; without S3_BUCKET the event was never archived
	if cfg.S3.Bucket == "" {
		fmt.Println("wipe archive: skipped, S3_BUCKET is not set")
		return 0
	}
	if err := cfg.S3.Validate(); err != nil {
		fatal("Invalid S3 configuration", err)
	}
	store, err := storage.NewObjectStore(&cfg.S3)
	if err != nil {
		fatal("Failed to open the archive", err)
	}
	as, err := wipe.Archive(ctx, store, &cfg.S3, opts, *dryRun)
	fmt.Printf("wipe archive: files=%d rows=%d matched=%d rewritten=%d removed=%d\n",
		as.Files, as.Rows, as.Matched, as.Rewritten, as.Removed)
	if err != nil {
		slog.Error("Archive wipe stopped", "error", err)
		return 1
	}
	return 0
}
