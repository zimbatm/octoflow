package nats

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
)

// SchemaManager handles NATS JetStream schema initialization and migration
type SchemaManager struct {
	js     nats.JetStreamContext
	config *config.NATSConfig
}

// NewSchemaManager creates a new schema manager
func NewSchemaManager(nc *nats.Conn, cfg *config.NATSConfig) (*SchemaManager, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &SchemaManager{
		js:     js,
		config: cfg,
	}, nil
}

// InitializeSchema ensures all required streams exist with proper configuration
func (m *SchemaManager) InitializeSchema() error {
	slog.Info("Initializing NATS JetStream schema")

	// Initialize single deduplicated events stream
	// All events go here, deduplication handled by NATS JetStream
	if err := m.ensureStream(StreamConfig{
		Name:         m.config.StreamName,
		Description:  "Deduplicated GitHub webhook events",
		Subjects:     []string{m.config.SubjectPrefix + ".>"}, // Flexible depth for org.{id}.{event} or global.{event}
		MaxConsumers: -1,                                      // Unlimited consumers
	}); err != nil {
		return fmt.Errorf("failed to ensure events stream: %w", err)
	}

	slog.Info("NATS JetStream schema initialization complete")
	return nil
}

// StreamConfig holds stream configuration
type StreamConfig struct {
	Name         string
	Description  string
	Subjects     []string
	MaxConsumers int // -1 for unlimited, 0 uses default
}

// ensureStream creates or updates a stream
func (m *SchemaManager) ensureStream(sc StreamConfig) error {
	streamConfig := &nats.StreamConfig{
		Name:        sc.Name,
		Description: sc.Description,
		Subjects:    sc.Subjects,
		Storage:     nats.FileStorage,
		Replicas:    m.config.StreamReplicas,
		MaxAge:      m.config.StreamMaxAge,
		Discard:     nats.DiscardOld,
		Retention:   nats.LimitsPolicy,
		Duplicates:  24 * time.Hour, // Deduplication window
	}

	// Set max consumers if specified
	if sc.MaxConsumers != 0 {
		streamConfig.MaxConsumers = sc.MaxConsumers
	}

	// Check if stream exists
	info, err := m.js.StreamInfo(sc.Name)
	if err != nil {
		if err == nats.ErrStreamNotFound {
			// Create new stream
			_, err = m.js.AddStream(streamConfig)
			if err != nil {
				return fmt.Errorf("failed to create stream %s: %w", sc.Name, err)
			}
			slog.Info("Created stream", "name", sc.Name)
			return nil
		}
		return fmt.Errorf("failed to get stream info for %s: %w", sc.Name, err)
	}

	// Check if subjects match
	if !subjectsEqual(info.Config.Subjects, sc.Subjects) {
		slog.Info("Stream exists with different subjects",
			"stream", sc.Name,
			"current", info.Config.Subjects,
			"expected", sc.Subjects)

		// Update the stream with new subjects
		slog.Info("Updating stream with new subjects", "stream", sc.Name)
		streamConfig.Subjects = sc.Subjects

		_, err = m.js.UpdateStream(streamConfig)
		if err != nil {
			// If we can't update due to overlap, we need to delete and recreate
			if err.Error() == "nats: subjects overlap with an existing stream" {
				slog.Error("Cannot update subjects due to overlap",
					"stream", sc.Name,
					"action", fmt.Sprintf("Please delete the stream and let it be recreated: nats stream rm %s", sc.Name))
				return fmt.Errorf("subject overlap prevents automatic migration")
			}
			return fmt.Errorf("failed to update stream %s subjects: %w", sc.Name, err)
		}
		slog.Info("Successfully updated stream subjects", "stream", sc.Name)
		return nil
	}

	// Update existing stream with same subjects
	_, err = m.js.UpdateStream(streamConfig)
	if err != nil {
		return fmt.Errorf("failed to update stream %s: %w", sc.Name, err)
	}
	slog.Info("Updated stream", "name", sc.Name)
	return nil
}

// subjectsEqual checks if two subject slices contain the same subjects
func subjectsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	// Create maps for comparison
	aMap := make(map[string]bool)
	for _, s := range a {
		aMap[s] = true
	}

	for _, s := range b {
		if !aMap[s] {
			return false
		}
	}

	return true
}

// CreateConsumer creates a consumer if it doesn't exist
func (m *SchemaManager) CreateConsumer(streamName, consumerName string, config *nats.ConsumerConfig) (*nats.ConsumerInfo, error) {
	// Check if consumer exists
	info, err := m.js.ConsumerInfo(streamName, consumerName)
	if err == nil {
		slog.Info("Using existing consumer",
			"consumer", consumerName,
			"stream", streamName,
			"pending", info.NumPending)
		return info, nil
	}

	// Create consumer
	info, err = m.js.AddConsumer(streamName, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer %s on stream %s: %w",
			consumerName, streamName, err)
	}

	slog.Info("Created consumer", "consumer", consumerName, "stream", streamName)
	return info, nil
}

// WaitForStream waits for a stream to exist
func (m *SchemaManager) WaitForStream(streamName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		_, err := m.js.StreamInfo(streamName)
		if err == nil {
			return nil
		}

		if err != nats.ErrStreamNotFound {
			return fmt.Errorf("error checking stream %s: %w", streamName, err)
		}

		slog.Info("Waiting for stream to be created", "stream", streamName)
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("timeout waiting for stream %s", streamName)
}
