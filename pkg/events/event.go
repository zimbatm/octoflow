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

	// Timestamps
	ReceivedAt      time.Time `json:"received_at" parquet:"name=received_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	GithubTimestamp time.Time `json:"github_timestamp,omitempty" parquet:"name=github_timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`

	// Event payload
	Payload json.RawMessage `json:"payload" parquet:"name=payload, type=BYTE_ARRAY, convertedtype=UTF8"` // Raw JSON payload

	// Parsed common fields for filtering/routing
	Repository   string `json:"repository,omitempty" parquet:"name=repository, type=BYTE_ARRAY, convertedtype=UTF8"`
	Organization string `json:"organization,omitempty" parquet:"name=organization, type=BYTE_ARRAY, convertedtype=UTF8"`
	Sender       string `json:"sender,omitempty" parquet:"name=sender, type=BYTE_ARRAY, convertedtype=UTF8"`
	Action       string `json:"action,omitempty" parquet:"name=action, type=BYTE_ARRAY, convertedtype=UTF8"`

	// Processing metadata
	ProcessingVersion int `json:"processing_version" parquet:"name=processing_version, type=INT32"`
	Retries           int `json:"retries" parquet:"name=retries, type=INT32"`
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

// EventReorderBuffer helps reorder events by their GitHub timestamp
type EventReorderBuffer struct {
	events     []*GitHubEvent
	windowSize time.Duration
	lastFlush  time.Time
}

// NewEventReorderBuffer creates a new reorder buffer with the specified window size
func NewEventReorderBuffer(windowSize time.Duration) *EventReorderBuffer {
	return &EventReorderBuffer{
		events:     make([]*GitHubEvent, 0),
		windowSize: windowSize,
		lastFlush:  time.Now(),
	}
}

// Add adds an event to the buffer
func (b *EventReorderBuffer) Add(event *GitHubEvent) {
	b.events = append(b.events, event)
}

// ShouldFlush returns true if the buffer should be flushed based on time window
func (b *EventReorderBuffer) ShouldFlush() bool {
	return time.Since(b.lastFlush) > b.windowSize
}

// Flush returns ordered events and clears the buffer
func (b *EventReorderBuffer) Flush() []*GitHubEvent {
	// Sort events by GitHub timestamp, then by ID for stable ordering
	sortEventsByTimestamp(b.events)

	result := make([]*GitHubEvent, len(b.events))
	copy(result, b.events)

	b.events = b.events[:0]
	b.lastFlush = time.Now()

	return result
}

// sortEventsByTimestamp sorts events by GitHub timestamp, then by ID
func sortEventsByTimestamp(events []*GitHubEvent) {
	// Using a simple bubble sort for now, can optimize later if needed
	n := len(events)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if shouldSwap(events[j], events[j+1]) {
				events[j], events[j+1] = events[j+1], events[j]
			}
		}
	}
}

// shouldSwap returns true if event a should come after event b
func shouldSwap(a, b *GitHubEvent) bool {
	// First compare by GitHub timestamp
	if !a.GithubTimestamp.Equal(b.GithubTimestamp) {
		return a.GithubTimestamp.After(b.GithubTimestamp)
	}
	// If timestamps are equal, compare by ID for stable ordering
	return a.ID > b.ID
}
