package events

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseCommonFields(t *testing.T) {
	tests := []struct {
		name       string
		payload    string
		wantOrg    string
		wantRepo   string
		wantActor  string
		wantAction string
	}{
		{
			name: "pull request event",
			payload: `{
				"action": "opened",
				"repository": {
					"full_name": "test-org/test-repo",
					"owner": {"login": "test-org"}
				},
				"sender": {"login": "test-user"},
				"pull_request": {"created_at": "2025-01-26T12:00:00Z"}
			}`,
			wantOrg:    "test-org",
			wantRepo:   "test-org/test-repo",
			wantActor:  "test-user",
			wantAction: "opened",
		},
		{
			name: "push event",
			payload: `{
				"repository": {
					"full_name": "another-org/another-repo",
					"owner": {"login": "another-org"}
				},
				"sender": {"login": "pusher"},
				"pushed_at": "2025-01-26T13:00:00Z"
			}`,
			wantOrg:    "another-org",
			wantRepo:   "another-org/another-repo",
			wantActor:  "pusher",
			wantAction: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &GitHubEvent{
				Payload: json.RawMessage(tt.payload),
			}

			err := ParseCommonFields(event)
			if err != nil {
				t.Fatalf("ParseCommonFields() error = %v", err)
			}

			if event.Organization != tt.wantOrg {
				t.Errorf("Organization = %v, want %v", event.Organization, tt.wantOrg)
			}
			if event.Repository != tt.wantRepo {
				t.Errorf("Repository = %v, want %v", event.Repository, tt.wantRepo)
			}
			if event.Sender != tt.wantActor {
				t.Errorf("Sender = %v, want %v", event.Sender, tt.wantActor)
			}
			if event.Action != tt.wantAction {
				t.Errorf("Action = %v, want %v", event.Action, tt.wantAction)
			}
		})
	}
}

func TestEventFilterMatches(t *testing.T) {
	push := &GitHubEvent{Event: "push"}
	opened := &GitHubEvent{Event: "pull_request", Action: "opened"}
	closed := &GitHubEvent{Event: "pull_request", Action: "closed"}

	var nilFilter *EventFilter
	if !nilFilter.Matches(push) || !(&EventFilter{}).Matches(push) {
		t.Error("a nil or empty filter must accept every event")
	}
	byType := &EventFilter{Events: []string{"pull_request", "issues"}}
	if byType.Matches(push) || !byType.Matches(opened) {
		t.Error("type filter")
	}
	byAction := &EventFilter{Actions: []string{"opened"}}
	if byAction.Matches(closed) || !byAction.Matches(opened) {
		t.Error("action filter")
	}
	both := &EventFilter{Events: []string{"pull_request"}, Actions: []string{"closed"}}
	if both.Matches(opened) || !both.Matches(closed) || both.Matches(push) {
		t.Error("type and action must both match")
	}
}

func TestParseCommonFieldsKeepsWhatItCanOnOddShapes(t *testing.T) {
	e := &GitHubEvent{Payload: json.RawMessage(`{"repository":"not an object","sender":{"id":"42","login":"x"},"organization":{"id":7,"login":"o"}}`)}
	if err := ParseCommonFields(e); err != nil {
		t.Fatalf("a type mismatch must not be an error: %v", err)
	}
	if e.Sender != "x" || e.SenderID != "42" || e.OrganizationID != "7" || e.Repository != "" {
		t.Errorf("parsed %+v", e)
	}
	if err := ParseCommonFields(&GitHubEvent{Payload: json.RawMessage(`{`)}); err == nil {
		t.Error("invalid JSON must be an error")
	}
}

func TestExtractEventEntitiesUsers(t *testing.T) {
	e := &GitHubEvent{Event: "pull_request_review", SenderID: "1", RepositoryID: "7", Payload: json.RawMessage(`{
		"pull_request": {"number": 5, "user": {"id": 2}, "assignees": [{"id": 3}, {"id": 1}], "requested_reviewers": [{"id": 4}]},
		"review": {"user": {"id": 1}},
		"comment": {"user": {"id": 5}}
	}`)}
	got, err := ExtractEventEntities(e)
	if err != nil {
		t.Fatal(err)
	}
	if got.RepositoryID != "7" || got.PRNumber != 5 {
		t.Errorf("entities = %+v", got)
	}
	if want := "1 2 3 4 5"; strings.Join(got.UserIDs, " ") != want {
		t.Errorf("users = %v, want %s: sender first, then author, assignees, reviewers, comment author, each once", got.UserIDs, want)
	}
}

func TestExtractEventEntitiesNumbers(t *testing.T) {
	tests := []struct {
		event   string
		payload string
		pr      int
		issue   int
	}{
		{"pull_request", `{"pull_request":{"number":42,"user":{"id":7}}}`, 42, 0},
		{"issue_comment", `{"issue":{"number":1234567},"comment":{"user":{"id":8}}}`, 0, 1234567},
	}
	for _, tt := range tests {
		t.Run(tt.event, func(t *testing.T) {
			e := &GitHubEvent{Event: tt.event, Payload: []byte(tt.payload)}
			got, err := ExtractEventEntities(e)
			if err != nil {
				t.Fatal(err)
			}
			if got.PRNumber != tt.pr || got.IssueNumber != tt.issue {
				t.Errorf("got pr=%d issue=%d, want pr=%d issue=%d", got.PRNumber, got.IssueNumber, tt.pr, tt.issue)
			}
		})
	}
}
