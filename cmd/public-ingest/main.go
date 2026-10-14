// public-ingest publishes events of repositories that we cannot attach a
// webhook to, from the GitHub Events API and from GH Archive, into the same
// NATS subjects as the webhook receiver. Both sources are best-effort; see
// the README.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/ghevents"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/retry"
)

const stateBucket = "octoflow_public_ingest"

type ingest struct {
	gh        *ghevents.Client
	publisher *natsInternal.Publisher
	state     nats.KeyValue
	repos     []*ghevents.RepoInfo
}

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

	schemaManager, err := natsInternal.NewSchemaManager(nc, &cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to create schema manager: %v", err)
	}
	if err := schemaManager.InitializeSchema(); err != nil {
		log.Fatalf("Failed to initialize NATS schema: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}
	state, err := js.KeyValue(stateBucket)
	if errors.Is(err, nats.ErrBucketNotFound) {
		state, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      stateBucket,
			Description: "public-ingest progress",
			Storage:     nats.FileStorage,
			Replicas:    cfg.NATS.StreamReplicas,
		})
	}
	if err != nil {
		log.Fatalf("Failed to open state bucket: %v", err)
	}

	publisher, err := natsInternal.NewPublisher(&cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	in := &ingest{
		gh:        ghevents.NewClient(os.Getenv("GITHUB_TOKEN")),
		publisher: publisher,
		state:     state,
	}
	for _, name := range names {
		info, err := in.gh.Repo(ctx, name)
		if err != nil {
			log.Fatalf("Failed to look up %s: %v", name, err)
		}
		slog.Info("Watching repository", "repo", info.FullName, "repo_id", info.ID, "owner_id", info.Owner.ID)
		in.repos = append(in.repos, info)
	}

	var wg sync.WaitGroup
	if envBool("EVENTS_API_ENABLED", true) {
		interval := envDuration("EVENTS_API_INTERVAL", time.Minute)
		wg.Add(1)
		go func() {
			defer wg.Done()
			in.runEventsAPI(ctx, interval)
		}()
	}
	if envBool("GHARCHIVE_ENABLED", true) {
		base := getenv("GHARCHIVE_URL", "https://data.gharchive.org")
		wg.Add(1)
		go func() {
			defer wg.Done()
			in.runGHArchive(ctx, base)
		}()
	}
	wg.Wait()
	slog.Info("Shutdown complete")
}

func (in *ingest) publish(ctx context.Context, e *ghevents.Event, owner ghevents.Account, source string) error {
	ev, err := ghevents.ToGitHubEvent(e, owner, source)
	if err != nil {
		slog.Warn("Skipping unreadable event", "source", source, "github_event_id", e.ID, "error", err)
		return nil
	}
	return retry.Do(ctx, func(ctx context.Context) error {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return in.publisher.PublishEvent(pctx, ev)
	}, retry.DefaultConfig())
}

// runEventsAPI polls each repository. The API keeps the newest 300 events
// (3 pages), so a poll stops paging once it reaches events it already saw.
func (in *ingest) runEventsAPI(ctx context.Context, interval time.Duration) {
	etags := map[int64]string{}
	for {
		wait := interval
		for _, repo := range in.repos {
			pi, err := in.pollRepo(ctx, repo, etags)
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

func (in *ingest) pollRepo(ctx context.Context, repo *ghevents.RepoInfo, etags map[int64]string) (time.Duration, error) {
	key := fmt.Sprintf("events-api.%d", repo.ID)
	var lastID int64
	if entry, err := in.state.Get(key); err == nil {
		lastID, _ = strconv.ParseInt(string(entry.Value()), 10, 64)
	}

	first, err := in.gh.RepoEvents(ctx, repo.FullName, 1, etags[repo.ID])
	if err != nil {
		return 0, err
	}
	if first.NotModified {
		return first.PollInterval, nil
	}

	all := first.Events
	page := first.Events
	for p := 2; p <= 3 && len(page) > 0 && minID(page) > lastID; p++ {
		next, err := in.gh.RepoEvents(ctx, repo.FullName, p, "")
		if err != nil {
			return first.PollInterval, err
		}
		page = next.Events
		all = append(all, page...)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].NumericID() < all[j].NumericID() })
	gap := lastID > 0 && len(all) > 0 && minID(all) > lastID && len(all) >= 300
	if gap {
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
	if _, err := in.state.PutString(key, strconv.FormatInt(lastID, 10)); err != nil {
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

// runGHArchive imports each hour once its file is out, about five minutes
// after the hour ends.
func (in *ingest) runGHArchive(ctx context.Context, base string) {
	const key = "gharchive.next_hour"
	hc := &http.Client{Timeout: 10 * time.Minute}

	repoIDs := map[int64]bool{}
	owners := map[int64]ghevents.Account{}
	for _, r := range in.repos {
		repoIDs[r.ID] = true
		owners[r.ID] = r.Owner
	}

	// JetStream deduplicates for 24h only, so a first start does not
	// replay hours that the Events API may already have delivered long ago.
	next := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Hour)
	if s := os.Getenv("GHARCHIVE_START"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			next = t.UTC().Truncate(time.Hour)
		}
	}
	if entry, err := in.state.Get(key); err == nil {
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
		if due := next.Add(time.Hour + 10*time.Minute); time.Now().Before(due) {
			if !sleep(time.Until(due)) {
				return
			}
		}

		url := ghevents.HourURL(base, next)
		n, err := ghevents.ReadHour(ctx, hc, url, repoIDs, func(e *ghevents.Event) error {
			return in.publish(ctx, e, owners[e.Repo.ID], ghevents.SourceGHArchive)
		})
		switch {
		case errors.Is(err, ghevents.ErrHourNotFound) && time.Since(next) > 24*time.Hour:
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
		if _, err := in.state.PutString(key, next.Format(time.RFC3339)); err != nil {
			slog.Error("Failed to save GH Archive progress", "error", err)
		}
	}
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
