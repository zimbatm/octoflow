package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/names"
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
			},
		},
		{
			name:     "issue",
			event:    &events.GitHubEvent{Event: "issues"},
			entities: &events.EventEntities{RepositoryID: "7", IssueNumber: 3},
			want:     []string{"p.repo.7.issues", "p.issue.7.3.issues"},
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

	n, err := Dispatch(context.Background(), rec, nil, "p", data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || len(rec.calls) != 3 {
		t.Fatalf("published %d/%d subjects, want 3", n, len(rec.calls))
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
	if _, err := Dispatch(context.Background(), rec, nil, "p", storedEvent(t), nil); err == nil {
		t.Fatal("expected an error")
	}
}

func TestDispatchRejectsUndecodableMessage(t *testing.T) {
	_, err := Dispatch(context.Background(), &recorder{}, nil, "p", []byte("not json"), nil)
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
	if _, err := pub.Publish(ctx, &ev); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if _, err := Dispatch(ctx, pub, nil, "p", data, nil); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}

	info, err := js.StreamInfo("P")
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 4 {
		t.Errorf("stream holds %d messages, want 1 primary + 3 copies; a second dispatch must add none", info.State.Msgs)
	}
}

func TestDispatchSkipsUserCopiesForListedTypes(t *testing.T) {
	rec := &recorder{}
	if _, err := Dispatch(context.Background(), rec, nil, "p", storedEvent(t), []string{"check_run", "pull_request"}); err != nil {
		t.Fatal(err)
	}
	var subjects []string
	for _, c := range rec.calls {
		subjects = append(subjects, c.subject)
	}
	if got, want := strings.Join(subjects, " "), "p.repo.7.pull_request p.pr.7.42.pull_request"; got != want {
		t.Errorf("subjects = %s, want %s: the repo and pr copies stay", got, want)
	}

	rec = &recorder{}
	if _, err := Dispatch(context.Background(), rec, nil, "p", storedEvent(t), []string{"check_run"}); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 3 {
		t.Errorf("%d copies of a type that is not listed, want 3", len(rec.calls))
	}
}

func namedEvent(t *testing.T, id, repo, sender, senderID string) []byte {
	t.Helper()
	data, err := json.Marshal(&events.GitHubEvent{
		ID: id, Event: "push",
		Organization: "NixOS", OrganizationID: "487568",
		Repository: repo, RepositoryID: "4542716",
		Sender: sender, SenderID: senderID,
		Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDispatchFillsTheNameIndex(t *testing.T) {
	_, js := natstest.Start(t)
	kv, err := names.Open(js, 1)
	if err != nil {
		t.Fatal(err)
	}
	index := names.NewWriter(kv)
	ctx := context.Background()
	writes := func() uint64 {
		t.Helper()
		info, err := js.StreamInfo("KV_" + names.Bucket)
		if err != nil {
			t.Fatal(err)
		}
		return info.State.LastSeq
	}

	if _, err := Dispatch(ctx, &recorder{}, index, "p", namedEvent(t, "d1", "NixOS/nixpkgs", "zimbatm", "3248"), nil); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"org.nixos": "487568", "repo.nixos.nixpkgs": "4542716", "user.zimbatm": "3248"} {
		e, err := kv.Get(key)
		if err != nil || string(e.Value()) != want {
			t.Errorf("%s = %v, %v; want %s", key, e, err, want)
		}
	}
	if n := writes(); n != 3 {
		t.Fatalf("%d writes for the first event, want 3", n)
	}

	for _, id := range []string{"d2", "d3", "d4"} {
		if _, err := Dispatch(ctx, &recorder{}, index, "p", namedEvent(t, id, "NixOS/nixpkgs", "zimbatm", "3248"), nil); err != nil {
			t.Fatal(err)
		}
	}
	if n := writes(); n != 3 {
		t.Errorf("%d writes after three more events with the same names, want 3", n)
	}

	if _, err := Dispatch(ctx, &recorder{}, index, "p", namedEvent(t, "d5", "NixOS/nixpkgs", "someone", "5"), nil); err != nil {
		t.Fatal(err)
	}
	if n := writes(); n != 4 {
		t.Errorf("%d writes after an event with one new name, want 4", n)
	}
}

func TestDispatchSurvivesAFailedIndexWrite(t *testing.T) {
	_, js := natstest.Start(t)
	kv, err := names.Open(js, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := js.DeleteKeyValue(names.Bucket); err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	n, err := Dispatch(context.Background(), rec, names.NewWriter(kv), "p", namedEvent(t, "d1", "NixOS/nixpkgs", "zimbatm", "3248"), nil)
	if err != nil || n != 2 || len(rec.calls) != 2 {
		t.Errorf("Dispatch = %d, %v with %d copies; want the repo and user copies and no error", n, err, len(rec.calls))
	}
}
