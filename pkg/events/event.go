package events

import (
	"encoding/json"
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

// EventFilter defines criteria for filtering events
type EventFilter struct {
	Events        []string `json:"events,omitempty"`        // Filter by event types
	Repositories  []string `json:"repositories,omitempty"`  // Filter by repository names
	Organizations []string `json:"organizations,omitempty"` // Filter by organization names
	Senders       []string `json:"senders,omitempty"`       // Filter by sender usernames
	Actions       []string `json:"actions,omitempty"`       // Filter by action types
}

// Matches returns true if the event matches the filter criteria
func (f *EventFilter) Matches(event *GitHubEvent) bool {
	// If no filters are specified, match all events
	if f == nil || (len(f.Events) == 0 && len(f.Repositories) == 0 &&
		len(f.Organizations) == 0 && len(f.Senders) == 0 && len(f.Actions) == 0) {
		return true
	}

	// Check event type filter
	if len(f.Events) > 0 && !contains(f.Events, event.Event) {
		return false
	}

	// Check repository filter
	if len(f.Repositories) > 0 && !contains(f.Repositories, event.Repository) {
		return false
	}

	// Check organization filter
	if len(f.Organizations) > 0 && !contains(f.Organizations, event.Organization) {
		return false
	}

	// Check sender filter
	if len(f.Senders) > 0 && !contains(f.Senders, event.Sender) {
		return false
	}

	// Check action filter
	if len(f.Actions) > 0 && !contains(f.Actions, event.Action) {
		return false
	}

	return true
}

// Helper function to check if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// EventEntities represents all entities involved in a GitHub event
// Used by the event dispatcher to route events to entity-specific subjects
type EventEntities struct {
	OrganizationID string   `json:"organization_id,omitempty"`
	RepositoryID   string   `json:"repository_id,omitempty"`
	SenderID       string   `json:"sender_id,omitempty"`
	UserIDs        []string `json:"user_ids,omitempty"`     // All users involved (sender, assignees, reviewers, etc.)
	PRNumber       int      `json:"pr_number,omitempty"`    // Pull request number if applicable
	IssueNumber    int      `json:"issue_number,omitempty"` // Issue number if applicable
}
