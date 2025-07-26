package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// ParseCommonFields extracts common fields from a GitHub webhook payload
func ParseCommonFields(event *GitHubEvent) error {
	var payload map[string]interface{}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	// Extract repository information
	if repo, ok := payload["repository"].(map[string]interface{}); ok {
		if fullName, ok := repo["full_name"].(string); ok {
			event.Repository = fullName
		}

		// Extract organization from owner
		if owner, ok := repo["owner"].(map[string]interface{}); ok {
			if login, ok := owner["login"].(string); ok {
				event.Organization = login
			}
		}
	}

	// Extract sender information
	if sender, ok := payload["sender"].(map[string]interface{}); ok {
		if login, ok := sender["login"].(string); ok {
			event.Sender = login
		}
	}

	// Extract action (for events that have it)
	if action, ok := payload["action"].(string); ok {
		event.Action = action
	}

	// Try to extract timestamp from various common fields
	event.GithubTimestamp = extractTimestamp(payload)

	return nil
}

// extractTimestamp tries to extract a timestamp from common GitHub event fields
func extractTimestamp(payload map[string]interface{}) time.Time {
	// Common timestamp fields in GitHub events
	timestampFields := []string{
		"created_at",
		"updated_at",
		"pushed_at",
		"timestamp",
		"occurred_at",
	}

	for _, field := range timestampFields {
		if ts, ok := payload[field].(string); ok {
			if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
				return parsed
			}
		}
	}

	// Check nested objects for timestamps
	nestedObjects := []string{"pull_request", "issue", "comment", "commit", "release"}
	for _, obj := range nestedObjects {
		if nested, ok := payload[obj].(map[string]interface{}); ok {
			for _, field := range timestampFields {
				if ts, ok := nested[field].(string); ok {
					if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
						return parsed
					}
				}
			}
		}
	}

	// If no timestamp found, use zero time (will use ReceivedAt for ordering)
	return time.Time{}
}

// ValidateEvent performs basic validation on a GitHub event
func ValidateEvent(event *GitHubEvent) error {
	if event.ID == "" {
		return fmt.Errorf("event ID is required")
	}

	if event.Event == "" {
		return fmt.Errorf("event type is required")
	}

	if len(event.Payload) == 0 {
		return fmt.Errorf("event payload is required")
	}

	// Validate that payload is valid JSON
	var test interface{}
	if err := json.Unmarshal(event.Payload, &test); err != nil {
		return fmt.Errorf("invalid JSON payload: %w", err)
	}

	return nil
}
