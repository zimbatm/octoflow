package reindex

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/dispatch"
	"github.com/zimbatm/octoflow/internal/names"
	"github.com/zimbatm/octoflow/internal/natstest"
	"github.com/zimbatm/octoflow/internal/status"
	"github.com/zimbatm/octoflow/pkg/events"
)

const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func publish(t *testing.T, js nats.JetStreamContext, subject, id, event, payload string) {
	t.Helper()
	data, err := json.Marshal(events.GitHubEvent{
		ID: id, Event: event, Payload: json.RawMessage(payload),
		Repository: "NixOS/nixpkgs", RepositoryID: "7",
		Organization: "NixOS", OrganizationID: "1",
		Sender: "alice", SenderID: "5",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(subject, data); err != nil {
		t.Fatal(err)
	}
}

func setup(t *testing.T) nats.JetStreamContext {
	t.Helper()
	_, js := natstest.Start(t)
	if _, err := js.AddStream(&nats.StreamConfig{Name: "S", Subjects: []string{"p.>"}}); err != nil {
		t.Fatal(err)
	}
	old := settle
	settle = 0
	t.Cleanup(func() { settle = old })
	return js
}

func run(t *testing.T, js nats.JetStreamContext, dryRun bool) Stats {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := Run(ctx, js, "S", "p", 1, time.Hour, dryRun)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestRunFillsTheBucketsFromTheStream(t *testing.T) {
	js := setup(t)
	publish(t, js, "p.org.1.pull_request", "d1", "pull_request", fmt.Sprintf(`{"pull_request":{"number":42,"head":{"sha":%q}}}`, sha))
	publish(t, js, "p.org.1.check_run", "c1", "check_run", fmt.Sprintf(`{"check_run":{"head_sha":%q,"pull_requests":[]}}`, sha))
	publish(t, js, "p.org.1.push", "p1", "push", `{}`)
	// a fan-out copy is not an event of its own
	publish(t, js, "p.repo.7.push", "p1", "push", `{}`)

	if st := run(t, js, true); st.Events != 3 || st.Heads != 1 || st.Raised != 0 {
		t.Errorf("dry run: %+v", st)
	}
	if _, err := js.KeyValue(names.Bucket); err == nil {
		t.Error("a dry run created the name index")
	}

	st := run(t, js, false)
	if st.Events != 3 || st.Heads != 1 || st.Minutes == 0 || st.Raised != st.Minutes {
		t.Errorf("first run: %+v", st)
	}

	kv, err := js.KeyValue(names.Bucket)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"org.nixos": "1", "repo.nixos.nixpkgs": "7", "user.alice": "5"} {
		if e, err := kv.Get(key); err != nil || string(e.Value()) != want {
			t.Errorf("%s: %v, %v; want %s", key, e, err, want)
		}
	}
	heads, err := dispatch.OpenHeads(js, 1, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if prs := heads.Lookup(context.Background(), "7", sha); len(prs) != 1 || prs[0] != 42 {
		t.Errorf("head: %v, want [42]", prs)
	}
	series, err := status.NewReader(js).Read(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, n := range series.Events {
		total += n
	}
	if total != 3 {
		t.Errorf("status counts %d events, want 3", total)
	}

	if st := run(t, js, false); st.Events != 3 || st.Raised != 0 {
		t.Errorf("second run wrote again: %+v", st)
	}
	if series, _ := status.NewReader(js).Read(time.Now()); series != nil {
		total = 0
		for _, n := range series.Events {
			total += n
		}
		if total != 3 {
			t.Errorf("after a second run the status counts %d events, want 3", total)
		}
	}
}

func TestRunLeavesTheNewestMinutesToTheDispatcher(t *testing.T) {
	js := setup(t)
	settle = 2 * time.Minute
	publish(t, js, "p.org.1.push", "p1", "push", `{}`)
	if st := run(t, js, false); st.Events != 1 || st.Minutes != 0 || st.Raised != 0 {
		t.Errorf("%+v", st)
	}
}

func TestRunOnAnEmptyStream(t *testing.T) {
	js := setup(t)
	if st := run(t, js, false); st != (Stats{}) {
		t.Errorf("%+v", st)
	}
}
