package ghevents

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/zimbatm/octoflow/pkg/events"
)

func TestWebhookName(t *testing.T) {
	for in, want := range map[string]string{
		"PullRequestEvent":              "pull_request",
		"PullRequestReviewCommentEvent": "pull_request_review_comment",
		"IssuesEvent":                   "issues",
		"PushEvent":                     "push",
		"GollumEvent":                   "gollum",
	} {
		if got := WebhookName(in); got != want {
			t.Errorf("WebhookName(%q) = %q, want %q", in, got, want)
		}
	}
}

// A trimmed PullRequestEvent as the Events API returns it since 2025-10-07.
const apiEvent = `{
  "id": "53000000001",
  "type": "PullRequestEvent",
  "actor": {"id": 2, "login": "zimbatm"},
  "repo": {"id": 4542716, "name": "NixOS/nixpkgs"},
  "org": {"id": 487568, "login": "NixOS"},
  "payload": {"action": "labeled", "number": 450000,
              "pull_request": {"id": 1, "number": 450000, "url": "u", "base": {}, "head": {}}},
  "created_at": "2026-10-14T13:07:00Z"
}`

func TestToGitHubEvent(t *testing.T) {
	var e Event
	if err := json.Unmarshal([]byte(apiEvent), &e); err != nil {
		t.Fatal(err)
	}
	got, err := ToGitHubEvent(&e, Account{ID: 487568, Login: "NixOS"}, SourceEventsAPI)
	if err != nil {
		t.Fatal(err)
	}

	if got.ID != "gh-event-53000000001" || got.Event != "pull_request" || got.Action != "labeled" {
		t.Errorf("id/event/action = %q/%q/%q", got.ID, got.Event, got.Action)
	}
	if got.RepositoryID != "4542716" || got.Repository != "NixOS/nixpkgs" {
		t.Errorf("repository = %q %q", got.RepositoryID, got.Repository)
	}
	if got.OrganizationID != "487568" || got.SenderID != "2" {
		t.Errorf("org/sender = %q/%q", got.OrganizationID, got.SenderID)
	}
	if got.Metadata["source"] != SourceEventsAPI || got.Metadata["created_at"] != "2026-10-14T13:07:00Z" {
		t.Errorf("metadata = %v", got.Metadata)
	}
	if time.Since(got.ReceivedAt) > time.Minute {
		t.Errorf("received_at = %v", got.ReceivedAt)
	}

	entities, err := events.ExtractEventEntities(got)
	if err != nil {
		t.Fatal(err)
	}
	if entities.PRNumber != 450000 {
		t.Errorf("pr number = %d, want 450000", entities.PRNumber)
	}
}

func TestToGitHubEventUserRepo(t *testing.T) {
	e := Event{ID: "1", Type: "WatchEvent", Actor: Account{ID: 9, Login: "x"},
		Repo: Repo{ID: 5, Name: "zimbatm/octoflow"}, Payload: json.RawMessage(`{"action":"started"}`)}
	got, err := ToGitHubEvent(&e, Account{ID: 3, Login: "zimbatm"}, SourceGHArchive)
	if err != nil {
		t.Fatal(err)
	}
	// webhooks on user repos also take the organization id from repository.owner.
	if got.OrganizationID != "3" || got.Organization != "zimbatm" {
		t.Errorf("org = %q %q", got.OrganizationID, got.Organization)
	}
}
