// Package publicingest reads the GitHub Events API and GH Archive for
// repositories that cannot get a webhook, and publishes their events in the
// webhook shape. Both sources are best-effort; see the README.
package publicingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/ghevents"
	"github.com/zimbatm/octoflow/internal/retry"
	"github.com/zimbatm/octoflow/pkg/events"
)

// StateBucket is the KV bucket that holds the progress of both sources.
const StateBucket = "octoflow_public_ingest"

type Publisher interface {
	PublishEvent(ctx context.Context, event *events.GitHubEvent) error
}

type Ingest struct {
	GH        *ghevents.Client
	Publisher Publisher
	State     nats.KeyValue
	Repos     []*ghevents.RepoInfo
	// HTTP fetches GH Archive files. nil means a client with a 10 minute timeout.
	HTTP *http.Client
	// Now is time.Now unless a test says otherwise.
	Now func() time.Time
}

// OpenState opens the progress bucket, creating it on first use.
func OpenState(js nats.JetStreamContext, replicas int) (nats.KeyValue, error) {
	kv, err := js.KeyValue(StateBucket)
	if errors.Is(err, nats.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      StateBucket,
			Description: "public-ingest progress",
			Storage:     nats.FileStorage,
			Replicas:    replicas,
		})
	}
	return kv, err
}

func (in *Ingest) now() time.Time {
	if in.Now != nil {
		return in.Now()
	}
	return time.Now()
}

func (in *Ingest) publish(ctx context.Context, e *ghevents.Event, owner ghevents.Account, source string) error {
	ev, err := ghevents.ToGitHubEvent(e, owner, source)
	if err != nil {
		slog.Warn("Skipping unreadable event", "source", source, "github_event_id", e.ID, "error", err)
		return nil
	}
	return retry.Do(ctx, func(ctx context.Context) error {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return in.Publisher.PublishEvent(pctx, ev)
	}, retry.DefaultConfig())
}

// RunEventsAPI polls each repository until ctx ends. GitHub's X-Poll-Interval
// wins over interval when it is longer.
func (in *Ingest) RunEventsAPI(ctx context.Context, interval time.Duration) {
	etags := map[int64]string{}
	for {
		wait := interval
		for _, repo := range in.Repos {
			pi, err := in.PollRepo(ctx, repo, etags)
			if err != nil && ctx.Err() == nil {
				slog.Error("Events API poll failed", "repo", repo.FullName, "error", err)
			}
			if pi > wait {
				wait = pi
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// PollRepo publishes the events newer than the last one seen and returns
// the poll interval GitHub asks for. The API keeps the newest 300 events
// (3 pages); paging stops at events already seen. etags persists the
// conditional-request state between polls; a 304 costs no rate limit.
func (in *Ingest) PollRepo(ctx context.Context, repo *ghevents.RepoInfo, etags map[int64]string) (time.Duration, error) {
	key := fmt.Sprintf("events-api.%d", repo.ID)
	var lastID int64
	if entry, err := in.State.Get(key); err == nil {
		lastID, _ = strconv.ParseInt(string(entry.Value()), 10, 64)
	}

	first, err := in.GH.RepoEvents(ctx, repo.FullName, 1, etags[repo.ID])
	if err != nil {
		return 0, err
	}
	if first.NotModified {
		return first.PollInterval, nil
	}

	all := first.Events
	page := first.Events
	for p := 2; p <= 3 && len(page) > 0 && minID(page) > lastID; p++ {
		next, err := in.GH.RepoEvents(ctx, repo.FullName, p, "")
		if err != nil {
			return first.PollInterval, err
		}
		page = next.Events
		all = append(all, page...)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].NumericID() < all[j].NumericID() })
	if lastID > 0 && len(all) >= 300 && minID(all) > lastID {
		slog.Warn("Events API window overflowed; GH Archive has to fill the gap",
			"repo", repo.FullName, "last_id", lastID, "oldest_id", minID(all))
	}

	published := 0
	for i := range all {
		e := &all[i]
		if e.NumericID() <= lastID {
			continue
		}
		if err := in.publish(ctx, e, repo.Owner, ghevents.SourceEventsAPI); err != nil {
			return first.PollInterval, err
		}
		lastID = e.NumericID()
		published++
	}
	if _, err := in.State.PutString(key, strconv.FormatInt(lastID, 10)); err != nil {
		return first.PollInterval, err
	}
	etags[repo.ID] = first.ETag
	slog.Info("Events API poll", "repo", repo.FullName, "fetched", len(all), "published", published)
	return first.PollInterval, nil
}

func minID(evs []ghevents.Event) int64 {
	m := int64(-1)
	for i := range evs {
		if id := evs[i].NumericID(); m < 0 || id < m {
			m = id
		}
	}
	return m
}

// RunGHArchive imports each hourly file from base once it is out, about ten
// minutes after the hour ends, until ctx ends. start is the first hour to
// import when no progress is saved yet.
func (in *Ingest) RunGHArchive(ctx context.Context, base string, start time.Time) {
	const key = "gharchive.next_hour"
	hc := in.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Minute}
	}

	repoIDs := map[int64]bool{}
	owners := map[int64]ghevents.Account{}
	for _, r := range in.Repos {
		repoIDs[r.ID] = true
		owners[r.ID] = r.Owner
	}

	next := start.UTC().Truncate(time.Hour)
	if entry, err := in.State.Get(key); err == nil {
		if t, err := time.Parse(time.RFC3339, string(entry.Value())); err == nil {
			next = t
		}
	}

	sleep := func(d time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}

	for ctx.Err() == nil {
		if due := next.Add(time.Hour + 10*time.Minute); in.now().Before(due) {
			if !sleep(due.Sub(in.now())) {
				return
			}
		}

		url := ghevents.HourURL(base, next)
		n, err := ghevents.ReadHour(ctx, hc, url, repoIDs, func(e *ghevents.Event) error {
			return in.publish(ctx, e, owners[e.Repo.ID], ghevents.SourceGHArchive)
		})
		switch {
		case errors.Is(err, ghevents.ErrHourNotFound) && in.now().Sub(next) > 24*time.Hour:
			slog.Warn("GH Archive hour missing; skipping", "hour", next)
		case errors.Is(err, ghevents.ErrHourNotFound):
			slog.Info("GH Archive hour not published yet", "hour", next)
			if !sleep(10 * time.Minute) {
				return
			}
			continue
		case err != nil:
			// a partial hour is re-read in full; JetStream drops the repeats.
			slog.Error("GH Archive import failed", "hour", next, "error", err)
			if !sleep(time.Minute) {
				return
			}
			continue
		default:
			slog.Info("GH Archive hour imported", "hour", next, "published", n)
		}

		next = next.Add(time.Hour)
		if _, err := in.State.PutString(key, next.Format(time.RFC3339)); err != nil {
			slog.Error("Failed to save GH Archive progress", "error", err)
		}
	}
}
