// Package compact writes one archive file per day from the batch files
// of that day.
package compact

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/storage"
	"github.com/zimbatm/octoflow/pkg/events"
)

type Stats struct {
	// batch files read, their rows received on the day, rows written,
	// rows left out because an earlier row has the same id
	Files, Rows, Events, Duplicates int
	// the daily file was written, or removed because the day has no rows
	Written, Removed bool
}

// cacheBudget bounds the payload bytes of decoded batch files held while
// the daily file is written. A batch file is at most S3_BATCH_MAX_BYTES,
// so a few fit.
var cacheBudget int64 = 256 << 20

// row locates one event of the day in the batch files. The day is sorted
// and deduplicated on these, without the payloads.
type row struct {
	at   int64
	id   string
	file int
	n    int
}

// Day writes the daily file of one UTC day: every event received on that
// day, sorted by received_at, one row per id. It reads the batch files
// uploaded on the day and on the next one, as restore does, because a
// file holds events received before its upload. The output depends only
// on the batch files, so a second run writes the same bytes. A day
// without events writes nothing and removes a daily file left from before.
// With dryRun it reads and counts.
func Day(ctx context.Context, store storage.StreamStore, cfg *config.S3Config, day time.Time, dryRun bool) (Stats, error) {
	var st Stats
	from := day.UTC().Truncate(24 * time.Hour)
	end := from.Add(24 * time.Hour)

	var keys []string
	for _, d := range []time.Time{from, end} {
		dayKeys, err := store.List(ctx, storage.DayPrefix(cfg.KeyPrefix, d))
		if err != nil {
			return st, err
		}
		keys = append(keys, dayKeys...)
	}
	slices.Sort(keys)

	var rows []row
	for f, key := range keys {
		evs, err := load(ctx, store, key, from, end)
		if err != nil {
			return st, err
		}
		st.Files++
		for n, ev := range evs {
			if ev != nil {
				rows = append(rows, row{at: ev.ReceivedAt.UnixMilli(), id: ev.ID, file: f, n: n})
			}
		}
	}
	st.Rows = len(rows)

	slices.SortFunc(rows, func(a, b row) int {
		return cmp.Or(cmp.Compare(a.at, b.at), cmp.Compare(a.id, b.id), cmp.Compare(a.file, b.file), cmp.Compare(a.n, b.n))
	})
	seen := make(map[string]struct{}, len(rows))
	rows = slices.DeleteFunc(rows, func(r row) bool {
		_, dup := seen[r.id]
		seen[r.id] = struct{}{}
		return dup
	})
	st.Events = len(rows)
	st.Duplicates = st.Rows - st.Events

	key := storage.DailyKey(cfg.DailyPrefix, from)
	if dryRun {
		return st, nil
	}
	if len(rows) == 0 {
		existing, err := store.List(ctx, key)
		if err != nil {
			return st, err
		}
		if slices.Contains(existing, key) {
			if err := store.Delete(ctx, key); err != nil {
				return st, err
			}
			st.Removed = true
			slog.Info("Removed daily file, the day has no events", "key", key)
		}
		return st, nil
	}

	err := storage.PutParquet(ctx, store, cfg, key, func(out *storage.ParquetWriter) error {
		return write(ctx, store, out, keys, rows, from, end)
	})
	if err != nil {
		return st, err
	}
	st.Written = true
	slog.Info("Wrote daily file", "key", key, "events", st.Events, "batch_files", st.Files, "duplicates", st.Duplicates)
	return st, nil
}

// load returns the rows of one batch file, with nil for those received
// outside from..end.
func load(ctx context.Context, store storage.ObjectStore, key string, from, end time.Time) ([]*events.GitHubEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	evs, err := storage.DecodeParquet(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	for i, ev := range evs {
		if ev.ReceivedAt.Before(from) || !ev.ReceivedAt.Before(end) {
			evs[i] = nil
		}
	}
	return evs, nil
}

type cached struct {
	evs   []*events.GitHubEvent
	bytes int64
	// position in the output of the last row taken from this file
	used int
}

// write encodes rows in order to out. The sorted order walks the batch
// files almost one after the other, so a file is read again here, kept
// until its last row is written, and dropped. When files overlap so much
// that they pass cacheBudget, the one not used for longest goes and is
// read again if a later row needs it.
func write(ctx context.Context, store storage.ObjectStore, out *storage.ParquetWriter, keys []string, rows []row, from, end time.Time) error {
	left := make([]int, len(keys))
	for _, r := range rows {
		left[r.file]++
	}
	cache := map[int]*cached{}
	var held int64

	for pos, r := range rows {
		c := cache[r.file]
		if c == nil {
			evs, err := load(ctx, store, keys[r.file], from, end)
			if err != nil {
				return err
			}
			c = &cached{evs: evs}
			for _, ev := range evs {
				if ev != nil {
					c.bytes += int64(len(ev.Payload))
				}
			}
			for len(cache) > 0 && held+c.bytes > cacheBudget {
				oldest := -1
				for f, o := range cache {
					if oldest < 0 || o.used < cache[oldest].used {
						oldest = f
					}
				}
				held -= cache[oldest].bytes
				delete(cache, oldest)
			}
			cache[r.file] = c
			held += c.bytes
		}
		c.used = pos

		var ev *events.GitHubEvent
		if r.n < len(c.evs) {
			ev = c.evs[r.n]
		}
		// the rows were located in a first read; a wipe or a rewrite in
		// between moves them
		if ev == nil || ev.ID != r.id || ev.ReceivedAt.UnixMilli() != r.at {
			return fmt.Errorf("%s changed during the compaction, run it again", keys[r.file])
		}
		if err := out.Write(ev); err != nil {
			return err
		}
		if left[r.file]--; left[r.file] == 0 {
			held -= c.bytes
			delete(cache, r.file)
		}
	}
	return nil
}
