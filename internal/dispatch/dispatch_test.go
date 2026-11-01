package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/internal/natstest"
	"github.com/zimbatm/octoflow/pkg/events"
)

func TestSubjects(t *testing.T) {
	tests := []struct {
		name     string
		event    *events.GitHubEvent
		entities *events.EventEntities
		want     []string
	}{
		{
			name:     "repository only",
			event:    &events.GitHubEvent{Event: "push"},
			entities: &events.EventEntities{RepositoryID: "7"},
			want:     []string{"p.repo.7.push"},
		},
		{
			name:     "pull request with users",
			event:    &events.GitHubEvent{Event: "pull_request"},
			entities: &events.EventEntities{RepositoryID: "7", UserIDs: []string{"1", "2"}, PRNumber: 42},
			want: []string{
				"p.repo.7.pull_request",
				"p.user.1.pull_request",
				"p.user.2.pull_request",
				"p.pr.7.42.pull_request",
				"p.pr.7.42",
			},
		},
		{
			name:     "issue",
			event:    &events.GitHubEvent{Event: "issues"},
			entities: &events.EventEntities{RepositoryID: "7", IssueNumber: 3},
			want:     []string{"p.repo.7.issues", "p.issue.7.3.issues", "p.issue.7.3"},
		},
		{
			name:     "pr number without repository is not routable",
			event:    &events.GitHubEvent{Event: "pull_request"},
			entities: &events.EventEntities{PRNumber: 42},
			want:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Subjects("p", tt.event, tt.entities)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Subjects() = %v, want %v", got, tt.want)
			}
		})
	}
}

type call struct {
	subject, eventType, msgID string
	data                      []byte
}

type recorder struct {
	calls []call
	fail  error
}

func (r *recorder) PublishToSubject(_ context.Context, subject, eventType string, data []byte, msgID string) error {
	if r.fail != nil {
		return r.fail
	}
	r.calls = append(r.calls, call{subject, eventType, msgID, data})
	return nil
}

func storedEvent(t *testing.T) []byte {
	t.Helper()
	ev := &events.GitHubEvent{
		ID:           "d1",
		Event:        "pull_request",
		RepositoryID: "7",
		SenderID:     "1",
		Payload:      json.RawMessage(`{"pull_request":{"number":42,"user":{"id":1}}}`),
	}
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDispatchGivesEachCopyItsOwnID(t *testing.T) {
	rec := &recorder{}
	data := storedEvent(t)

	n, err := Dispatch(context.Background(), rec, "p", data)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 || len(rec.calls) != 4 {
		t.Fatalf("published %d/%d subjects, want 4", n, len(rec.calls))
	}
	seen := map[string]bool{}
	for _, c := range rec.calls {
		if c.eventType != "pull_request" {
			t.Errorf("event type label = %q", c.eventType)
		}
		if c.msgID != "d1@"+c.subject {
			t.Errorf("msg id = %q, want d1@%s", c.msgID, c.subject)
		}
		if seen[c.msgID] {
			t.Errorf("duplicate msg id %q", c.msgID)
		}
		seen[c.msgID] = true
		if string(c.data) != string(data) {
			t.Error("copy is not byte-identical to the original")
		}
	}
}

func TestDispatchReportsPublishFailure(t *testing.T) {
	rec := &recorder{fail: errors.New("nats down")}
	if _, err := Dispatch(context.Background(), rec, "p", storedEvent(t)); err == nil {
		t.Fatal("expected an error")
	}
}

func TestDispatchRejectsUndecodableMessage(t *testing.T) {
	_, err := Dispatch(context.Background(), &recorder{}, "p", []byte("not json"))
	if !errors.Is(err, ErrUndecodable) {
		t.Errorf("err = %v, want ErrUndecodable", err)
	}
}

func TestDispatchStoresCopiesOnce(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := &config.NATSConfig{SubjectPrefix: "p", StreamName: "P", StreamReplicas: 1, StreamMaxAge: time.Hour}
	if err := natsInternal.EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	pub := natsInternal.NewPublisher(js, cfg)
	ctx := context.Background()

	var ev events.GitHubEvent
	data := storedEvent(t)
	if err := json.Unmarshal(data, &ev); err != nil {
		t.Fatal(err)
	}
	if err := pub.PublishEvent(ctx, &ev); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if _, err := Dispatch(ctx, pub, "p", data); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}

	info, err := js.StreamInfo("P")
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 5 {
		t.Errorf("stream holds %d messages, want 1 primary + 4 copies; a second dispatch must add none", info.State.Msgs)
	}
}
