package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/metrics"
	"github.com/zimbatm/octoflow/pkg/events"
)

// streamLabel is the fixed value of the metrics' stream label.
const streamLabel = "events"

// Publisher publishes events to the JetStream stream over a connection the
// caller owns.
type Publisher struct {
	prefix string
	js     nats.JetStreamContext
}

func NewPublisher(js nats.JetStreamContext, cfg *config.NATSConfig) *Publisher {
	return &Publisher{prefix: cfg.SubjectPrefix, js: js}
}

// PublishEvent publishes a GitHub event under its org or global subject,
// with event.ID as the deduplication id.
func (p *Publisher) PublishEvent(ctx context.Context, event *events.GitHubEvent) error {
	_, err := p.Publish(ctx, event)
	return err
}

// Publish is PublishEvent that also reports whether the stream already
// held a message with this id and dropped the new one.
func (p *Publisher) Publish(ctx context.Context, event *events.GitHubEvent) (duplicate bool, err error) {
	data, err := json.Marshal(event)
	if err != nil {
		metrics.RecordNATSPublish(streamLabel, event.Event, "marshal_error", 0)
		return false, fmt.Errorf("failed to marshal event: %w", err)
	}
	ack, err := p.publish(ctx, p.Subject(event), event.Event, data, event.ID)
	if err != nil {
		return false, err
	}
	return ack.Duplicate, nil
}

// Subject is the primary subject of an event: org.{org_id}.{type}, or
// global.{type} when the event has no organization.
func (p *Publisher) Subject(event *events.GitHubEvent) string {
	if event.OrganizationID != "" {
		return fmt.Sprintf("%s.org.%s.%s", p.prefix, event.OrganizationID, event.Event)
	}
	return fmt.Sprintf("%s.global.%s", p.prefix, event.Event)
}

// PublishToSubject publishes data and waits for the stream's ack. msgID is
// the deduplication id; eventType only labels the metrics.
func (p *Publisher) PublishToSubject(ctx context.Context, subject, eventType string, data []byte, msgID string) error {
	_, err := p.publish(ctx, subject, eventType, data, msgID)
	return err
}

func (p *Publisher) publish(ctx context.Context, subject, eventType string, data []byte, msgID string) (*nats.PubAck, error) {
	start := time.Now()
	status := "success"
	defer func() {
		metrics.RecordNATSPublish(streamLabel, eventType, status, time.Since(start).Seconds())
	}()

	future, err := p.js.PublishAsync(subject, data, nats.MsgId(msgID))
	if err != nil {
		status = "publish_error"
		return nil, fmt.Errorf("failed to publish to JetStream: %w", err)
	}
	select {
	case ack := <-future.Ok():
		return ack, nil
	case err := <-future.Err():
		status = "ack_error"
		return nil, fmt.Errorf("JetStream publish failed: %w", err)
	case <-ctx.Done():
		status = "timeout"
		return nil, ctx.Err()
	}
}
