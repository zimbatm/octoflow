package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// ParseCommonFields extracts common fields from a GitHub webhook payload
func ParseCommonFields(event *GitHubEvent) error {
	var payload map[string]interface{}

	// Use json.Decoder with UseNumber to preserve integer precision
	decoder := json.NewDecoder(bytes.NewReader(event.Payload))
	decoder.UseNumber()

	if err := decoder.Decode(&payload); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	// Extract repository information
	if repo, ok := payload["repository"].(map[string]interface{}); ok {
		if fullName, ok := repo["full_name"].(string); ok {
			event.Repository = fullName
		}

		// Extract repository ID
		if id, ok := repo["id"]; ok {
			event.RepositoryID = formatID(id)
		}

		// Extract organization from owner
		if owner, ok := repo["owner"].(map[string]interface{}); ok {
			if login, ok := owner["login"].(string); ok {
				event.Organization = login
			}
			// Extract organization ID from owner
			if id, ok := owner["id"]; ok {
				event.OrganizationID = formatID(id)
			}
		}
	}

	// Extract sender information
	if sender, ok := payload["sender"].(map[string]interface{}); ok {
		if login, ok := sender["login"].(string); ok {
			event.Sender = login
		}
		// Extract sender ID
		if id, ok := sender["id"]; ok {
			event.SenderID = formatID(id)
		}
	}

	// Extract action (for events that have it)
	if action, ok := payload["action"].(string); ok {
		event.Action = action
	}

	// Extract organization ID directly if present (for org-level events)
	if org, ok := payload["organization"].(map[string]interface{}); ok {
		if id, ok := org["id"]; ok {
			event.OrganizationID = formatID(id)
		}
		if login, ok := org["login"].(string); ok {
			event.Organization = login
		}
	}

	return nil
}

// formatID converts numeric IDs to string format without scientific notation
func formatID(id interface{}) string {
	switch v := id.(type) {
	case json.Number:
		// json.Number preserves the exact numeric representation
		return v.String()
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case string:
		// Already a string, return as-is
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ExtractEventEntities extracts all entities involved in a GitHub event
// This is used by the event dispatcher to route events to entity-specific subjects
func ExtractEventEntities(event *GitHubEvent) (*EventEntities, error) {
	var payload map[string]interface{}

	// Use json.Decoder with UseNumber to preserve integer precision
	decoder := json.NewDecoder(bytes.NewReader(event.Payload))
	decoder.UseNumber()

	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	entities := &EventEntities{
		OrganizationID: event.OrganizationID,
		RepositoryID:   event.RepositoryID,
		SenderID:       event.SenderID,
		UserIDs:        []string{},
	}

	// Add sender to user IDs if present
	if event.SenderID != "" {
		entities.UserIDs = append(entities.UserIDs, event.SenderID)
	}

	// Extract PR/Issue numbers and additional user IDs based on event type
	switch event.Event {
	case "pull_request", "pull_request_review", "pull_request_review_comment":
		if pr, ok := payload["pull_request"].(map[string]interface{}); ok {
			// Extract PR number
			if number, ok := toInt(pr["number"]); ok {
				entities.PRNumber = number
			}

			// Extract PR author
			if user, ok := pr["user"].(map[string]interface{}); ok {
				if id, ok := user["id"]; ok {
					userID := formatID(id)
					if !containsString(entities.UserIDs, userID) {
						entities.UserIDs = append(entities.UserIDs, userID)
					}
				}
			}

			// Extract assignees
			if assignees, ok := pr["assignees"].([]interface{}); ok {
				for _, assignee := range assignees {
					if user, ok := assignee.(map[string]interface{}); ok {
						if id, ok := user["id"]; ok {
							userID := formatID(id)
							if !containsString(entities.UserIDs, userID) {
								entities.UserIDs = append(entities.UserIDs, userID)
							}
						}
					}
				}
			}

			// Extract requested reviewers
			if reviewers, ok := pr["requested_reviewers"].([]interface{}); ok {
				for _, reviewer := range reviewers {
					if user, ok := reviewer.(map[string]interface{}); ok {
						if id, ok := user["id"]; ok {
							userID := formatID(id)
							if !containsString(entities.UserIDs, userID) {
								entities.UserIDs = append(entities.UserIDs, userID)
							}
						}
					}
				}
			}
		}

	case "issues", "issue_comment":
		if issue, ok := payload["issue"].(map[string]interface{}); ok {
			// Extract issue number
			if number, ok := toInt(issue["number"]); ok {
				entities.IssueNumber = number
			}

			// Extract issue author
			if user, ok := issue["user"].(map[string]interface{}); ok {
				if id, ok := user["id"]; ok {
					userID := formatID(id)
					if !containsString(entities.UserIDs, userID) {
						entities.UserIDs = append(entities.UserIDs, userID)
					}
				}
			}

			// Extract assignees
			if assignees, ok := issue["assignees"].([]interface{}); ok {
				for _, assignee := range assignees {
					if user, ok := assignee.(map[string]interface{}); ok {
						if id, ok := user["id"]; ok {
							userID := formatID(id)
							if !containsString(entities.UserIDs, userID) {
								entities.UserIDs = append(entities.UserIDs, userID)
							}
						}
					}
				}
			}
		}

	case "push", "create", "delete":
		// For push events, the pusher is already captured as sender
		// These events typically don't have other users involved

	case "team_add", "team":
		// Extract team members if available
		if team, ok := payload["team"].(map[string]interface{}); ok {
			if members, ok := team["members"].([]interface{}); ok {
				for _, member := range members {
					if user, ok := member.(map[string]interface{}); ok {
						if id, ok := user["id"]; ok {
							userID := formatID(id)
							if !containsString(entities.UserIDs, userID) {
								entities.UserIDs = append(entities.UserIDs, userID)
							}
						}
					}
				}
			}
		}
	}

	// Extract comment author for comment events
	if comment, ok := payload["comment"].(map[string]interface{}); ok {
		if user, ok := comment["user"].(map[string]interface{}); ok {
			if id, ok := user["id"]; ok {
				userID := formatID(id)
				if !containsString(entities.UserIDs, userID) {
					entities.UserIDs = append(entities.UserIDs, userID)
				}
			}
		}
	}

	// Extract review author for review events
	if review, ok := payload["review"].(map[string]interface{}); ok {
		if user, ok := review["user"].(map[string]interface{}); ok {
			if id, ok := user["id"]; ok {
				userID := formatID(id)
				if !containsString(entities.UserIDs, userID) {
					entities.UserIDs = append(entities.UserIDs, userID)
				}
			}
		}
	}

	return entities, nil
}

// containsString checks if a string slice contains a specific string
func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
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

func toInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	case float64:
		return int(n), true
	}
	return 0, false
}
