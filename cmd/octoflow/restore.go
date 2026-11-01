package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
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
	force := fs.Bool("force", false, "also publish events the stream still holds; past the 24 hour duplicate window they are stored twice")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	usageErr := func(msg string, err error) int {
		fmt.Fprintln(os.Stderr, "octoflow restore:", msg, err)
		fs.Usage()
		return 2
	}

	opts := restore.Options{RepoID: *repo, OrgID: *org, To: time.Now().UTC()}
	var err error
	if *from == "" {
		return usageErr("--from is required", nil)
	}
	if opts.From, err = time.Parse(time.DateOnly, *from); err != nil {
		return usageErr("--from:", err)
	}
	if *to != "" {
		if opts.To, err = time.Parse(time.DateOnly, *to); err != nil {
			return usageErr("--to:", err)
		}
	}
	if opts.To.Before(opts.From) {
		return usageErr("--to is before --from", nil)
	}
	if *eventTypes != "" {
		opts.Events = strings.Split(*eventTypes, ",")
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		fatal("Failed to load configuration", err)
	}
	setupLogging(cfg.Log)
	opts.KeyPrefix = cfg.S3.KeyPrefix
	if err := cfg.S3.Validate(); err != nil {
		fatal("Invalid S3 configuration", err)
	}
	store, err := storage.NewObjectStore(&cfg.S3)
	if err != nil {
		fatal("Failed to open the archive", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var pub restore.Publisher
	if !*dryRun {
		nc, js, err := natsInternal.ConnectJetStream(&cfg.NATS)
		if err != nil {
			fatal("Failed to connect to NATS", err)
		}
		defer nc.Close()
		pub = natsInternal.NewPublisher(js, &cfg.NATS)
		if !*force {
			if opts.Before, err = oldestInStream(js, cfg.NATS.StreamName); err != nil {
				fatal("Failed to read the stream", err)
			}
		}
	}

	slog.Info("Restoring from the archive", "bucket", cfg.S3.Bucket, "prefix", opts.KeyPrefix,
		"from", opts.From.Format(time.DateOnly), "to", opts.To.Format(time.DateOnly), "dry_run", *dryRun, "before", opts.Before)
	st, err := restore.Run(ctx, store, pub, opts)
	fmt.Printf("restore: files=%d rows=%d matched=%d in_stream=%d published=%d duplicates=%d\n",
		st.Files, st.Rows, st.Matched, st.InStream, st.Published, st.Duplicates)
	if err != nil {
		slog.Error("Restore stopped", "error", err)
		return 1
	}
	return 0
}

// oldestInStream returns the received_at of the oldest event the stream
// still holds, or zero for an empty stream. Everything received since
// then is in the stream, and after the duplicate window the stream would
// store it a second time.
func oldestInStream(js nats.JetStreamContext, stream string) (time.Time, error) {
	info, err := js.StreamInfo(stream)
	if err != nil {
		return time.Time{}, err
	}
	if info.State.Msgs == 0 {
		return time.Time{}, nil
	}
	msg, err := js.GetMsg(stream, info.State.FirstSeq)
	if err != nil {
		return time.Time{}, err
	}
	var ev events.GitHubEvent
	if json.Unmarshal(msg.Data, &ev) != nil || ev.ReceivedAt.IsZero() {
		return msg.Time, nil
	}
	return ev.ReceivedAt, nil
}
