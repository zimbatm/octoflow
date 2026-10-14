package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/metrics"
	"github.com/zimbatm/octoflow/pkg/events"
)

// Publisher publishes events to NATS
type Publisher struct {
	config *config.NATSConfig
	nc     *nats.Conn
	js     nats.JetStreamContext
}

// NewPublisher creates a new NATS publisher
func NewPublisher(cfg *config.NATSConfig) (*Publisher, error) {
	// Connect to NATS
	nc, err := Connect(cfg)
	if err != nil {
		return nil, err
	}

	publisher := &Publisher{
		config: cfg,
		nc:     nc,
	}

	// Set up JetStream if enabled
	if cfg.JetStream {
		js, err := nc.JetStream()
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("failed to create JetStream context: %w", err)
		}
		publisher.js = js
	}

	return publisher, nil
}

// PublishEvent publishes a GitHub event to NATS
func (p *Publisher) PublishEvent(ctx context.Context, event *events.GitHubEvent) error {
	start := time.Now()
	streamName := "events"
	status := "success"

	defer func() {
		// Record metrics
		metrics.RecordNATSPublish(streamName, event.Event, status, time.Since(start).Seconds())

	}()

	// Serialize event to JSON
	data, err := json.Marshal(event)
	if err != nil {
		status = "marshal_error"
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Build subject based on event type
	subject := p.buildSubject(event)

	// Publish with JetStream if enabled
	if p.js != nil {
		// Use event ID for deduplication
		pubOpts := []nats.PubOpt{
			nats.MsgId(event.ID),
		}

		// Publish with acknowledgment
		ack, err := p.js.PublishAsync(subject, data, pubOpts...)
		if err != nil {
			status = "publish_error"
			return fmt.Errorf("failed to publish to JetStream: %w", err)
		}

		// Wait for acknowledgment
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
	} else {
		// Regular NATS publish
		if err := p.nc.Publish(subject, data); err != nil {
			status = "publish_error"
			return fmt.Errorf("failed to publish to NATS: %w", err)
		}

		// Flush to ensure delivery
		if err := p.nc.FlushTimeout(5 * time.Second); err != nil {
			status = "flush_error"
			return fmt.Errorf("failed to flush NATS connection: %w", err)
		}
	}

	return nil
}

// PublishBatch publishes multiple events to NATS
func (p *Publisher) PublishBatch(ctx context.Context, events []*events.GitHubEvent) error {
	for _, event := range events {
		if err := p.PublishEvent(ctx, event); err != nil {
			return fmt.Errorf("failed to publish event %s: %w", event.ID, err)
		}
	}
	return nil
}

// buildSubject constructs the NATS subject for an event
func (p *Publisher) buildSubject(event *events.GitHubEvent) string {
	// Use simplified pattern: org.{org_id}.{event_type}
	// or global.{event_type} for non-org events

	if event.OrganizationID != "" {
		return fmt.Sprintf("%s.org.%s.%s", p.config.SubjectPrefix, event.OrganizationID, event.Event)
	}

	// For events without organization context (e.g., user events, global events)
	return fmt.Sprintf("%s.global.%s", p.config.SubjectPrefix, event.Event)
}

// PublishToSubject publishes data to a specific subject
func (p *Publisher) PublishToSubject(ctx context.Context, subject string, data []byte, msgID string) error {
	start := time.Now()
	streamName := "events"
	status := "success"
	eventType := "unknown"

	// Extract event type from subject
	parts := strings.Split(subject, ".")
	if len(parts) >= 4 {
		eventType = parts[3]
	}

	defer func() {
		// Record metrics
		metrics.RecordNATSPublish(streamName, eventType, status, time.Since(start).Seconds())
	}()

	if p.js != nil {
		// Use JetStream with deduplication
		pubOpts := []nats.PubOpt{
			nats.MsgId(msgID),
		}

		// Publish with acknowledgment
		ack, err := p.js.PublishAsync(subject, data, pubOpts...)
		if err != nil {
			status = "publish_error"
			return fmt.Errorf("failed to publish to JetStream: %w", err)
		}

		// Wait for acknowledgment
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

	// Regular NATS publish
	if err := p.nc.Publish(subject, data); err != nil {
		status = "publish_error"
		return fmt.Errorf("failed to publish to NATS: %w", err)
	}

	// Flush to ensure delivery
	if err := p.nc.FlushTimeout(5 * time.Second); err != nil {
		status = "flush_error"
		return fmt.Errorf("failed to flush NATS connection: %w", err)
	}

	return nil
}

// Close closes the NATS connection
func (p *Publisher) Close() error {
	if p.nc != nil {
		p.nc.Close()
	}
	return nil
}

// IsConnected returns true if connected to NATS
func (p *Publisher) IsConnected() bool {
	return p.nc != nil && p.nc.IsConnected()
}

// Stats returns NATS connection statistics
func (p *Publisher) Stats() nats.Statistics {
	if p.nc != nil {
		return p.nc.Stats()
	}
	return nats.Statistics{}
}
