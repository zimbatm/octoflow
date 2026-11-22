// Package reindex fills the dispatcher's KV buckets again from the events
// in the stream: after a bucket was lost, or for the events that were
// stored before the dispatcher kept that bucket.
package reindex

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/dispatch"
	"github.com/zimbatm/octoflow/internal/names"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/status"
	"github.com/zimbatm/octoflow/pkg/events"
)

// settle is how old a minute must be before its count is written. The
// dispatcher holds its counts for some seconds before it writes them, and
// would add them on top of a count that already has them. A variable so
// tests can lower it.
var settle = 2 * time.Minute

type Stats struct {
	// primary events read
	Events int
	// pull_request events with a head commit
	Heads int
	// minutes of the last 24 hours with an event
	Minutes int
	// minutes whose count in the bucket was lower
	Raised int
}

// Run reads every primary event of the stream once and gives it to the
// name index, the map of pull request heads and the status counts. It
// publishes nothing and is safe beside a running dispatcher; a second run
// writes nothing new. With dryRun it only reads and counts.
func Run(ctx context.Context, js nats.JetStreamContext, stream, prefix string, replicas int, maxAge time.Duration, dryRun bool) (Stats, error) {
	var st Stats

	var index *names.Writer
	var heads *dispatch.Heads
	if !dryRun {
		kv, err := names.Open(js, replicas)
		if err != nil {
			return st, fmt.Errorf("failed to open the name index: %w", err)
		}
		index = names.NewWriter(kv)
		if heads, err = dispatch.OpenHeads(js, replicas, maxAge); err != nil {
			return st, fmt.Errorf("failed to open the heads bucket: %w", err)
		}
	}

	counts := map[time.Time]int64{}
	start := time.Now()
	subjects := natsInternal.PrimarySubjects(prefix)
	sub, err := js.SubscribeSync("", nats.OrderedConsumer(), nats.DeliverAll(), nats.BindStream(stream),
		nats.ConsumerFilterSubjects(subjects...))
	if err != nil {
		return st, fmt.Errorf("failed to read the stream: %w", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	// a read of an empty selection would wait for the first event
	more := false
	for _, subject := range subjects {
		info, err := js.StreamInfo(stream, &nats.StreamInfoRequest{SubjectsFilter: subject})
		if err != nil {
			return st, fmt.Errorf("failed to read the stream: %w", err)
		}
		more = more || len(info.State.Subjects) > 0
	}
	for more {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			return st, fmt.Errorf("failed to read the stream: %w", err)
		}
		meta, err := msg.Metadata()
		if err != nil {
			return st, fmt.Errorf("message without metadata: %w", err)
		}
		// what arrives from here on, the dispatcher handles
		more = meta.NumPending > 0 && meta.Timestamp.Before(start)
		st.Events++
		if age := start.Sub(meta.Timestamp); age >= settle && age < status.Minutes*time.Minute {
			counts[meta.Timestamp.UTC().Truncate(time.Minute)]++
		}
		var ev events.GitHubEvent
		if json.Unmarshal(msg.Data, &ev) != nil {
			continue
		}
		if dryRun {
			if ev.Event == "pull_request" {
				st.Heads++
			}
			continue
		}
		index.Record(&ev)
		learned, err := heads.LearnFrom(ctx, &ev)
		if err != nil {
			return st, fmt.Errorf("failed to record a head: %w", err)
		}
		if learned {
			st.Heads++
		}
	}
	st.Minutes = len(counts)
	if dryRun {
		return st, nil
	}
	if st.Raised, err = status.Raise(js, replicas, status.Events, counts); err != nil {
		return st, fmt.Errorf("failed to write the status counts: %w", err)
	}
	return st, nil
}
