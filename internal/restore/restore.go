// Package restore publishes archived events from S3 back into the stream.
package restore

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/zimbatm/octoflow/internal/storage"
	"github.com/zimbatm/octoflow/pkg/events"
)

type Publisher interface {
	Publish(ctx context.Context, event *events.GitHubEvent) (duplicate bool, err error)
}

// Options select what to restore. From and To are UTC dates, inclusive,
// compared with each event's received_at.
type Options struct {
	KeyPrefix string
	From, To  time.Time
	// empty means every event type
	Events []string
	// empty means every repository or organization
	RepoID, OrgID string
}

type Stats struct {
	Files, Rows, Matched, Published, Duplicates int
}

// Run reads the archive days From to To plus one, because a file holds
// events received before its upload time and can spill into the next day,
// and publishes every event whose received_at is in the range. Events are
// published in archive order; the stream drops those it still holds. A nil
// pub is a dry run: everything is read and counted, nothing is published.
func Run(ctx context.Context, store storage.ObjectStore, pub Publisher, opts Options) (Stats, error) {
	var st Stats
	from := opts.From.UTC().Truncate(24 * time.Hour)
	end := opts.To.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)

	var keys []string
	for d := from; !d.After(end); d = d.Add(24 * time.Hour) {
		dayKeys, err := store.List(ctx, storage.DayPrefix(opts.KeyPrefix, d))
		if err != nil {
			return st, err
		}
		keys = append(keys, dayKeys...)
	}
	slices.Sort(keys)

	for _, key := range keys {
		if ctx.Err() != nil {
			return st, ctx.Err()
		}
		data, err := store.Get(ctx, key)
		if err != nil {
			return st, err
		}
		evs, err := storage.DecodeParquet(data)
		if err != nil {
			return st, fmt.Errorf("%s: %w", key, err)
		}
		st.Files++
		st.Rows += len(evs)

		fileMatched, filePublished := 0, 0
		for _, ev := range evs {
			if !matches(ev, &opts, from, end) {
				continue
			}
			st.Matched++
			fileMatched++
			if pub == nil {
				continue
			}
			dup, err := pub.Publish(ctx, ev)
			if err != nil {
				return st, fmt.Errorf("%s: event %s: %w", key, ev.ID, err)
			}
			if dup {
				st.Duplicates++
			} else {
				st.Published++
				filePublished++
			}
		}
		slog.Info("Restored archive file", "key", key, "rows", len(evs), "matched", fileMatched, "published", filePublished)
	}
	return st, nil
}

func matches(ev *events.GitHubEvent, opts *Options, from, end time.Time) bool {
	if ev.ReceivedAt.Before(from) || !ev.ReceivedAt.Before(end) {
		return false
	}
	if len(opts.Events) > 0 && !slices.Contains(opts.Events, ev.Event) {
		return false
	}
	if opts.RepoID != "" && ev.RepositoryID != opts.RepoID {
		return false
	}
	if opts.OrgID != "" && ev.OrganizationID != opts.OrgID {
		return false
	}
	return true
}
