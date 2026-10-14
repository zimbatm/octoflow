// Package ghevents turns GitHub Events API and GH Archive records into the
// webhook-shaped events that the rest of octoflow handles.
package ghevents

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/zimbatm/octoflow/pkg/events"
)

// Event is one record of the Events API. GH Archive stores the same shape.
type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Actor     Account         `json:"actor"`
	Repo      Repo            `json:"repo"`
	Org       *Account        `json:"org,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

type Account struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
}

type Repo struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// MsgIDPrefix makes ids from both public sources collide on purpose: GH
// Archive is built from the Events API and keeps its ids, so JetStream drops
// an event that both sources deliver.
const MsgIDPrefix = "gh-event-"

// Sources, recorded in GitHubEvent.Metadata["source"].
const (
	SourceEventsAPI = "events-api"
	SourceGHArchive = "gharchive"
)

// WebhookName maps an Events API type to its webhook event name,
// for example PullRequestReviewCommentEvent to pull_request_review_comment.
func WebhookName(eventType string) string {
	name := strings.TrimSuffix(eventType, "Event")
	var b strings.Builder
	for i, r := range name {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}

// ToGitHubEvent rewrites e into the webhook shape. The payload gains the
// repository, sender and organization objects that webhooks carry, so
// ParseCommonFields and the dispatcher route it like a webhook delivery.
// owner is the repository owner; the Events API only names it.
func ToGitHubEvent(e *Event, owner Account, source string) (*events.GitHubEvent, error) {
	payload := map[string]json.RawMessage{}
	if len(e.Payload) > 0 && string(e.Payload) != "null" {
		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			return nil, fmt.Errorf("event %s: decode payload: %w", e.ID, err)
		}
	}

	short := e.Repo.Name
	if i := strings.IndexByte(short, '/'); i >= 0 {
		short = short[i+1:]
	}
	setIfMissing(payload, "repository", map[string]any{
		"id":        e.Repo.ID,
		"name":      short,
		"full_name": e.Repo.Name,
		"owner":     owner,
	})
	setIfMissing(payload, "sender", e.Actor)
	if e.Org != nil {
		setIfMissing(payload, "organization", e.Org)
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	ev := &events.GitHubEvent{
		ID:                MsgIDPrefix + e.ID,
		Event:             WebhookName(e.Type),
		ReceivedAt:        time.Now().UTC(),
		Payload:           raw,
		ProcessingVersion: 1,
		Metadata: map[string]string{
			"source":          source,
			"github_event_id": e.ID,
			"created_at":      e.CreatedAt.UTC().Format(time.RFC3339),
		},
	}
	if err := events.ParseCommonFields(ev); err != nil {
		return nil, err
	}
	return ev, nil
}

func setIfMissing(m map[string]json.RawMessage, key string, v any) {
	if _, ok := m[key]; ok {
		return
	}
	raw, _ := json.Marshal(v)
	m[key] = raw
}

// NumericID returns the event id as a number, for ordering.
func (e *Event) NumericID() int64 {
	n, _ := strconv.ParseInt(e.ID, 10, 64)
	return n
}
