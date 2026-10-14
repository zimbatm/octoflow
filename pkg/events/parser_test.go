package events

import (
	"encoding/json"
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

func TestValidateEvent(t *testing.T) {
	tests := []struct {
		name    string
		event   *GitHubEvent
		wantErr bool
	}{
		{
			name: "valid event",
			event: &GitHubEvent{
				ID:      "123",
				Event:   "push",
				Payload: json.RawMessage(`{"test": "data"}`),
			},
			wantErr: false,
		},
		{
			name: "missing ID",
			event: &GitHubEvent{
				Event:   "push",
				Payload: json.RawMessage(`{"test": "data"}`),
			},
			wantErr: true,
		},
		{
			name: "missing event type",
			event: &GitHubEvent{
				ID:      "123",
				Payload: json.RawMessage(`{"test": "data"}`),
			},
			wantErr: true,
		},
		{
			name: "missing payload",
			event: &GitHubEvent{
				ID:    "123",
				Event: "push",
			},
			wantErr: true,
		},
		{
			name: "invalid JSON payload",
			event: &GitHubEvent{
				ID:      "123",
				Event:   "push",
				Payload: json.RawMessage(`{invalid json`),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateEvent(tt.event)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEventFilterMatches(t *testing.T) {
	filter := &EventFilter{
		Events:        []string{"push", "pull_request"},
		Organizations: []string{"test-org", "another-org"},
		Repositories:  []string{"test-org/repo1", "another-org/repo2"},
	}

	tests := []struct {
		name  string
		event *GitHubEvent
		want  bool
	}{
		{
			name: "matches all criteria",
			event: &GitHubEvent{
				Event:        "push",
				Organization: "test-org",
				Repository:   "test-org/repo1",
			},
			want: true,
		},
		{
			name: "matches event and org but not repo",
			event: &GitHubEvent{
				Event:        "push",
				Organization: "test-org",
				Repository:   "test-org/different-repo",
			},
			want: false,
		},
		{
			name: "matches nothing",
			event: &GitHubEvent{
				Event:        "issues",
				Organization: "different-org",
				Repository:   "different-repo",
			},
			want: false,
		},
		{
			name: "empty filter matches all",
			event: &GitHubEvent{
				Event:        "any-event",
				Organization: "any-org",
				Repository:   "any-repo",
			},
			want: true,
		},
	}

	// Test with populated filter
	for _, tt := range tests {
		if tt.name == "empty filter matches all" {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			if got := filter.Matches(tt.event); got != tt.want {
				t.Errorf("EventFilter.Matches() = %v, want %v", got, tt.want)
			}
		})
	}

	// Test with empty filter
	emptyFilter := &EventFilter{}
	event := &GitHubEvent{
		Event:        "any-event",
		Organization: "any-org",
		Repository:   "any-repo",
	}
	if !emptyFilter.Matches(event) {
		t.Error("Empty filter should match all events")
	}
}

func TestEventFilterMatchesPartial(t *testing.T) {
	// Filter with only event types
	eventFilter := &EventFilter{
		Events: []string{"push", "pull_request"},
	}

	pushEvent := &GitHubEvent{
		Event:        "push",
		Organization: "any-org",
		Repository:   "any-repo",
	}

	issuesEvent := &GitHubEvent{
		Event:        "issues",
		Organization: "any-org",
		Repository:   "any-repo",
	}

	if !eventFilter.Matches(pushEvent) {
		t.Error("Filter should match push event")
	}

	if eventFilter.Matches(issuesEvent) {
		t.Error("Filter should not match issues event")
	}

	// Filter with only organizations
	orgFilter := &EventFilter{
		Organizations: []string{"allowed-org"},
	}

	allowedEvent := &GitHubEvent{
		Event:        "any-event",
		Organization: "allowed-org",
		Repository:   "any-repo",
	}

	blockedEvent := &GitHubEvent{
		Event:        "any-event",
		Organization: "blocked-org",
		Repository:   "any-repo",
	}

	if !orgFilter.Matches(allowedEvent) {
		t.Error("Filter should match allowed organization")
	}

	if orgFilter.Matches(blockedEvent) {
		t.Error("Filter should not match blocked organization")
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
