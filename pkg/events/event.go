package events

import (
	"encoding/json"
	"time"
)

// GitHubEvent represents a GitHub webhook event with metadata for processing
type GitHubEvent struct {
	// Event metadata from GitHub headers
	ID               string `json:"id" parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8"`                                         // X-GitHub-Delivery
	Event            string `json:"event" parquet:"name=event, type=BYTE_ARRAY, convertedtype=UTF8"`                                   // X-GitHub-Event
	Signature        string `json:"signature,omitempty" parquet:"name=signature, type=BYTE_ARRAY, convertedtype=UTF8"`                 // X-Hub-Signature-256
	HookID           string `json:"hook_id,omitempty" parquet:"name=hook_id, type=BYTE_ARRAY, convertedtype=UTF8"`                     // X-GitHub-Hook-ID
	HookInstallation string `json:"hook_installation,omitempty" parquet:"name=hook_installation, type=BYTE_ARRAY, convertedtype=UTF8"` // X-GitHub-Hook-Installation-Target-ID

	// Timestamp when we received the event
	ReceivedAt time.Time `json:"received_at" parquet:"name=received_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`

	// Event payload
	Payload json.RawMessage `json:"payload" parquet:"name=payload, type=BYTE_ARRAY, convertedtype=UTF8"` // Raw JSON payload

	// Parsed common fields for filtering/routing
	Repository   string `json:"repository,omitempty" parquet:"name=repository, type=BYTE_ARRAY, convertedtype=UTF8"`
	Organization string `json:"organization,omitempty" parquet:"name=organization, type=BYTE_ARRAY, convertedtype=UTF8"`
	Sender       string `json:"sender,omitempty" parquet:"name=sender, type=BYTE_ARRAY, convertedtype=UTF8"`
	Action       string `json:"action,omitempty" parquet:"name=action, type=BYTE_ARRAY, convertedtype=UTF8"`

	// Numeric IDs for rename-proof identification
	OrganizationID string `json:"organization_id,omitempty" parquet:"name=organization_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	RepositoryID   string `json:"repository_id,omitempty" parquet:"name=repository_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	SenderID       string `json:"sender_id,omitempty" parquet:"name=sender_id, type=BYTE_ARRAY, convertedtype=UTF8"`

	// Processing metadata
	ProcessingVersion int `json:"processing_version" parquet:"name=processing_version, type=INT32"`
	Retries           int `json:"retries" parquet:"name=retries, type=INT32"`
	
	// Metadata for SSE streaming context
	Metadata map[string]string `json:"metadata,omitempty"` // Additional metadata for SSE clients
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

// EventBatch represents a batch of events for processing
type EventBatch struct {
	Events    []*GitHubEvent `json:"events"`
	BatchID   string         `json:"batch_id"`
	CreatedAt time.Time      `json:"created_at"`
}

// EventEntities represents all entities involved in a GitHub event
// Used by the event dispatcher to route events to entity-specific subjects
type EventEntities struct {
	OrganizationID string   `json:"organization_id,omitempty"`
	RepositoryID   string   `json:"repository_id,omitempty"`
	SenderID       string   `json:"sender_id,omitempty"`
	UserIDs        []string `json:"user_ids,omitempty"`      // All users involved (sender, assignees, reviewers, etc.)
	PRNumber       int      `json:"pr_number,omitempty"`      // Pull request number if applicable
	IssueNumber    int      `json:"issue_number,omitempty"`   // Issue number if applicable
}

