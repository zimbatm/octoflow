package events

import (
	"encoding/json"
	"slices"
	"time"
)

// GitHubEvent is the message stored in JetStream. The parquet schema lives
// in internal/storage.
type GitHubEvent struct {
	// Event metadata from GitHub headers
	ID               string `json:"id"`                          // X-GitHub-Delivery
	Event            string `json:"event"`                       // X-GitHub-Event
	Signature        string `json:"signature,omitempty"`         // X-Hub-Signature-256
	HookID           string `json:"hook_id,omitempty"`           // X-GitHub-Hook-ID
	HookInstallation string `json:"hook_installation,omitempty"` // X-GitHub-Hook-Installation-Target-ID

	// Timestamp when we received the event
	ReceivedAt time.Time `json:"received_at"`

	// Event payload
	Payload json.RawMessage `json:"payload"`

	// Parsed common fields for filtering/routing
	Repository   string `json:"repository,omitempty"`
	Organization string `json:"organization,omitempty"`
	Sender       string `json:"sender,omitempty"`
	Action       string `json:"action,omitempty"`

	// Numeric IDs for rename-proof identification
	OrganizationID string `json:"organization_id,omitempty"`
	RepositoryID   string `json:"repository_id,omitempty"`
	SenderID       string `json:"sender_id,omitempty"`

	// Metadata for SSE streaming context
	Metadata map[string]string `json:"metadata,omitempty"`
}

// EventFilter keeps the events whose type and action are listed; an empty
// list accepts every value, and a nil filter accepts every event.
type EventFilter struct {
	Events  []string
	Actions []string
}

func (f *EventFilter) Matches(event *GitHubEvent) bool {
	if f == nil {
		return true
	}
	return (len(f.Events) == 0 || slices.Contains(f.Events, event.Event)) &&
		(len(f.Actions) == 0 || slices.Contains(f.Actions, event.Action))
}

// EventEntities is what the dispatcher routes an event to.
type EventEntities struct {
	RepositoryID string
	// every involved user: sender, author, assignees, requested reviewers,
	// comment and review authors, team members
	UserIDs     []string
	PRNumber    int
	IssueNumber int
}
