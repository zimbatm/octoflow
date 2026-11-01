// Package dispatch republishes stored events to their entity subjects.
package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/zimbatm/octoflow/pkg/events"
)

// ErrUndecodable marks a message that is not a GitHubEvent. Retrying it
// cannot help; the caller should terminate it.
var ErrUndecodable = errors.New("message is not a GitHubEvent")

// Publisher is the part of nats.Publisher the dispatcher uses.
type Publisher interface {
	PublishToSubject(ctx context.Context, subject, eventType string, data []byte, msgID string) error
}

// Subjects lists where an event is republished: its repository, every
// involved user, and its pull request or issue. Every copy ends in the
// event type, so one pull request is pr.{repo}.{n}.* and all pull requests
// of a repository pr.{repo}.*.*; each event is stored once per subject.
func Subjects(prefix string, event *events.GitHubEvent, entities *events.EventEntities) []string {
	var subjects []string

	if entities.RepositoryID != "" {
		subjects = append(subjects, fmt.Sprintf("%s.repo.%s.%s", prefix, entities.RepositoryID, event.Event))
	}
	for _, userID := range entities.UserIDs {
		subjects = append(subjects, fmt.Sprintf("%s.user.%s.%s", prefix, userID, event.Event))
	}
	if entities.PRNumber > 0 && entities.RepositoryID != "" {
		subjects = append(subjects, fmt.Sprintf("%s.pr.%s.%d.%s", prefix, entities.RepositoryID, entities.PRNumber, event.Event))
	}
	if entities.IssueNumber > 0 && entities.RepositoryID != "" {
		subjects = append(subjects, fmt.Sprintf("%s.issue.%s.%d.%s", prefix, entities.RepositoryID, entities.IssueNumber, event.Event))
	}
	return subjects
}

// Dispatch republishes one stored event. data is republished as is, so
// every copy is byte-identical to the original. It returns the number of
// subjects written.
func Dispatch(ctx context.Context, pub Publisher, prefix string, data []byte) (int, error) {
	var event events.GitHubEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrUndecodable, err)
	}

	entities, err := events.ExtractEventEntities(&event)
	if err != nil {
		slog.Warn("Failed to extract entities from event",
			"event_id", event.ID,
			"event_type", event.Event,
			"error", err)
		entities = &events.EventEntities{}
	}

	subjects := Subjects(prefix, &event, entities)

	for _, subject := range subjects {
		// the stream deduplicates by message id across all subjects, so reusing
		// event.ID here would drop every copy as a duplicate of the original.
		msgID := event.ID + "@" + subject
		if err := pub.PublishToSubject(ctx, subject, event.Event, data, msgID); err != nil {
			return 0, fmt.Errorf("publish to %s: %w", subject, err)
		}
	}

	slog.Info("Dispatched event",
		"event_id", event.ID,
		"event_type", event.Event,
		"repo_id", entities.RepositoryID,
		"user_count", len(entities.UserIDs),
		"pr_number", entities.PRNumber,
		"issue_number", entities.IssueNumber,
		"subject_count", len(subjects))
	return len(subjects), nil
}
