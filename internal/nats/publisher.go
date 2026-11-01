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

// Publisher publishes events to the JetStream stream.
type Publisher struct {
	config *config.NATSConfig
	nc     *nats.Conn
	js     nats.JetStreamContext
}

// NewPublisher opens its own NATS connection.
func NewPublisher(cfg *config.NATSConfig) (*Publisher, error) {
	nc, err := Connect(cfg)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}
	return &Publisher{config: cfg, nc: nc, js: js}, nil
}

// PublishEvent publishes a GitHub event under its org or global subject,
// with event.ID as the deduplication id.
func (p *Publisher) PublishEvent(ctx context.Context, event *events.GitHubEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		metrics.RecordNATSPublish(streamLabel, event.Event, "marshal_error", 0)
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	return p.PublishToSubject(ctx, p.buildSubject(event), event.Event, data, event.ID)
}

func (p *Publisher) buildSubject(event *events.GitHubEvent) string {
	if event.OrganizationID != "" {
		return fmt.Sprintf("%s.org.%s.%s", p.config.SubjectPrefix, event.OrganizationID, event.Event)
	}
	return fmt.Sprintf("%s.global.%s", p.config.SubjectPrefix, event.Event)
}

// PublishToSubject publishes data and waits for the stream's ack. msgID is
// the deduplication id; eventType only labels the metrics.
func (p *Publisher) PublishToSubject(ctx context.Context, subject, eventType string, data []byte, msgID string) error {
	start := time.Now()
	status := "success"
	defer func() {
		metrics.RecordNATSPublish(streamLabel, eventType, status, time.Since(start).Seconds())
	}()

	ack, err := p.js.PublishAsync(subject, data, nats.MsgId(msgID))
	if err != nil {
		status = "publish_error"
		return fmt.Errorf("failed to publish to JetStream: %w", err)
	}
	select {
	case <-ack.Ok():
		return nil
	case err := <-ack.Err():
		status = "ack_error"
		return fmt.Errorf("JetStream publish failed: %w", err)
	case <-ctx.Done():
		status = "timeout"
		return ctx.Err()
	}
}

// Close closes the NATS connection
func (p *Publisher) Close() {
	if p.nc != nil {
		p.nc.Close()
	}
}
