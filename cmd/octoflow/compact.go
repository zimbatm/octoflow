package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zimbatm/octoflow/internal/compact"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/storage"
)

// runCompact is the compact subcommand. It returns the exit status.
func runCompact(args []string) int {
	fs := flag.NewFlagSet("octoflow compact", flag.ContinueOnError)
	day := fs.String("day", "", "the day to compact, YYYY-MM-DD in UTC, by received time (default: yesterday)")
	from := fs.String("from", "", "first day of a range, in place of --day")
	to := fs.String("to", "", "last day of the range, inclusive (default: yesterday)")
	dryRun := fs.Bool("dry-run", false, "read the batch files and count, write nothing")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	usageErr := func(msg string, err error) int {
		fmt.Fprintln(os.Stderr, "octoflow compact:", msg, err)
		fs.Usage()
		return 2
	}

	// today is not over: its file would be rewritten by every later run
	yesterday := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
	first, last := yesterday, yesterday
	var err error
	switch {
	case *day != "" && (*from != "" || *to != ""):
		return usageErr("--day excludes --from and --to", nil)
	case *day != "":
		if first, err = time.Parse(time.DateOnly, *day); err != nil {
			return usageErr("--day:", err)
		}
		last = first
	case *from != "":
		if first, err = time.Parse(time.DateOnly, *from); err != nil {
			return usageErr("--from:", err)
		}
		if *to != "" {
			if last, err = time.Parse(time.DateOnly, *to); err != nil {
				return usageErr("--to:", err)
			}
		}
		if last.Before(first) {
			return usageErr("--to is before --from", nil)
		}
	case *to != "":
		return usageErr("--to needs --from", nil)
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		fatal("Failed to load configuration", err)
	}
	setupLogging(cfg.Log)
	if err := cfg.S3.Validate(); err != nil {
		fatal("Invalid S3 configuration", err)
	}
	if err := cfg.S3.ValidateDaily(); err != nil {
		fatal("Invalid S3 configuration", err)
	}
	store, err := storage.NewStreamStore(&cfg.S3)
	if err != nil {
		fatal("Failed to open the archive", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("Compacting the archive", "bucket", cfg.S3.Bucket, "prefix", cfg.S3.KeyPrefix, "daily_prefix", cfg.S3.DailyPrefix,
		"from", first.Format(time.DateOnly), "to", last.Format(time.DateOnly), "dry_run", *dryRun)
	for d := first; !d.After(last); d = d.AddDate(0, 0, 1) {
		st, err := compact.Day(ctx, store, &cfg.S3, d, *dryRun)
		result := "none"
		if *dryRun {
			result = "dry-run"
		}
		switch {
		case err != nil:
			result = "failed"
		case st.Written:
			result = "written"
		case st.Removed:
			result = "removed"
		}
		fmt.Printf("compact %s: files=%d rows=%d events=%d duplicates=%d daily=%s key=%s\n", d.Format(time.DateOnly),
			st.Files, st.Rows, st.Events, st.Duplicates, result, storage.DailyKey(cfg.S3.DailyPrefix, d))
		if err != nil {
			slog.Error("Compaction stopped", "day", d.Format(time.DateOnly), "error", err)
			return 1
		}
	}
	return 0
}
