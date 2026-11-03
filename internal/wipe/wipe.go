// Package wipe removes events from the stream and from the archive.
package wipe

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/storage"
	"github.com/zimbatm/octoflow/pkg/events"
)

// Options select the events to wipe. Every set field must match; an
// event that matches is removed with all its fan-out copies. From and To
// are UTC dates, inclusive, compared with received_at; zero is unbounded.
type Options struct {
	// delivery ids
	IDs    []string
	Events []string
	RepoID string
	OrgID  string
	// a user involved in the event: sender, author, assignee, reviewer
	UserID string
	// a pull request or issue number; combine with RepoID
	PR, Issue int
	From, To  time.Time
}

// Empty reports whether no field selects anything, which would match
// every event.
func (o *Options) Empty() bool {
	return len(o.IDs) == 0 && len(o.Events) == 0 && o.RepoID == "" && o.OrgID == "" &&
		o.UserID == "" && o.PR == 0 && o.Issue == 0 && o.From.IsZero() && o.To.IsZero()
}

// Matches reports whether ev is selected.
func (o *Options) Matches(ev *events.GitHubEvent) bool {
	if !o.From.IsZero() && ev.ReceivedAt.Before(o.From.UTC().Truncate(24*time.Hour)) {
		return false
	}
	if !o.To.IsZero() && !ev.ReceivedAt.Before(o.To.UTC().Truncate(24*time.Hour).Add(24*time.Hour)) {
		return false
	}
	if len(o.IDs) > 0 && !slices.Contains(o.IDs, ev.ID) {
		return false
	}
	if len(o.Events) > 0 && !slices.Contains(o.Events, ev.Event) {
		return false
	}
	if o.RepoID != "" && ev.RepositoryID != o.RepoID {
		return false
	}
	if o.OrgID != "" && ev.OrganizationID != o.OrgID {
		return false
	}
	if o.UserID != "" || o.PR != 0 || o.Issue != 0 {
		ent, err := events.ExtractEventEntities(ev)
		if err != nil {
			ent = &events.EventEntities{}
		}
		if o.UserID != "" && ev.SenderID != o.UserID && !slices.Contains(ent.UserIDs, o.UserID) {
			return false
		}
		if o.PR != 0 && ent.PRNumber != o.PR {
			return false
		}
		if o.Issue != 0 && ent.IssueNumber != o.Issue {
			return false
		}
	}
	return true
}

type StreamStats struct {
	// messages read, messages selected (primary and copies), messages
	// deleted, distinct events among the selected
	Scanned, Matched, Deleted, Events int
}

// Stream reads every message of the stream and securely deletes the
// selected ones, so the server overwrites them on disk. With dryRun it
// only counts. Consumers that already received a message are not
// affected; SSE clients cannot be recalled.
func Stream(ctx context.Context, js nats.JetStreamContext, stream, prefix string, opts Options, dryRun bool) (StreamStats, error) {
	var st StreamStats
	info, err := js.StreamInfo(stream)
	if err != nil {
		return st, fmt.Errorf("failed to read the stream: %w", err)
	}
	if info.State.Msgs == 0 {
		return st, nil
	}

	sub, err := js.SubscribeSync(prefix+".>", nats.OrderedConsumer(), nats.DeliverAll(), nats.BindStream(stream))
	if err != nil {
		return st, fmt.Errorf("failed to read the stream: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	var seqs []uint64
	ids := map[string]bool{}
	for {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			return st, fmt.Errorf("failed to read the stream: %w", err)
		}
		meta, err := msg.Metadata()
		if err != nil {
			return st, fmt.Errorf("message without metadata: %w", err)
		}
		st.Scanned++
		var ev events.GitHubEvent
		if json.Unmarshal(msg.Data, &ev) == nil && opts.Matches(&ev) {
			st.Matched++
			ids[ev.ID] = true
			seqs = append(seqs, meta.Sequence.Stream)
		}
		if meta.NumPending == 0 {
			break
		}
	}
	st.Events = len(ids)
	if dryRun {
		return st, nil
	}

	for _, seq := range seqs {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		if err := js.SecureDeleteMsg(stream, seq); err != nil {
			return st, fmt.Errorf("failed to delete message %d: %w", seq, err)
		}
		st.Deleted++
	}
	return st, nil
}

type ArchiveStats struct {
	// files read, rows read, rows selected, files written back without
	// them, files removed because nothing was left
	Files, Rows, Matched, Rewritten, Removed int
}

// Archive rewrites every archive file that holds a selected event. A file
// is listed by the day it was uploaded, and holds events received up to a
// day before, so the days From to To plus one are read; without From the
// whole archive is. With dryRun it only counts.
func Archive(ctx context.Context, store storage.ObjectStore, cfg *config.S3Config, opts Options, dryRun bool) (ArchiveStats, error) {
	var st ArchiveStats
	var keys []string
	if opts.From.IsZero() {
		var err error
		if keys, err = store.List(ctx, cfg.KeyPrefix+"/"); err != nil {
			return st, err
		}
	} else {
		to := opts.To
		if to.IsZero() {
			to = time.Now()
		}
		from := opts.From.UTC().Truncate(24 * time.Hour)
		end := to.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
		for d := from; !d.After(end); d = d.Add(24 * time.Hour) {
			dayKeys, err := store.List(ctx, storage.DayPrefix(cfg.KeyPrefix, d))
			if err != nil {
				return st, err
			}
			keys = append(keys, dayKeys...)
		}
	}
	slices.Sort(keys)

	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return st, err
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
		total := len(evs)
		kept := slices.DeleteFunc(evs, func(ev *events.GitHubEvent) bool { return opts.Matches(ev) })
		matched := total - len(kept)
		st.Matched += matched
		if matched == 0 || dryRun {
			continue
		}
		if len(kept) == 0 {
			if err := store.Delete(ctx, key); err != nil {
				return st, err
			}
			st.Removed++
			slog.Info("Removed archive file", "key", key, "rows", matched)
			continue
		}
		out, err := storage.EncodeParquet(cfg, kept)
		if err != nil {
			return st, fmt.Errorf("%s: %w", key, err)
		}
		if err := store.Put(ctx, key, out); err != nil {
			return st, err
		}
		st.Rewritten++
		slog.Info("Rewrote archive file", "key", key, "kept", len(kept), "removed", matched)
	}
	return st, nil
}
