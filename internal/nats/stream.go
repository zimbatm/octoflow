package nats

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
)

// DuplicateWindow is how long the stream remembers message ids. A
// redelivery inside it is dropped; one after it is stored again.
const DuplicateWindow = 24 * time.Hour

// EnsureStream creates the events stream or updates its limits. Primary
// events and the dispatcher's fan-out copies share it.
func EnsureStream(js nats.JetStreamContext, cfg *config.NATSConfig) error {
	// the server rejects a duplicate window longer than the retention.
	duplicates := DuplicateWindow
	if cfg.StreamMaxAge > 0 && cfg.StreamMaxAge < duplicates {
		duplicates = cfg.StreamMaxAge
	}
	sc := &nats.StreamConfig{
		Name:         cfg.StreamName,
		Description:  "GitHub events, deduplicated by message id",
		Subjects:     []string{cfg.SubjectPrefix + ".>"},
		Storage:      nats.FileStorage,
		Replicas:     cfg.StreamReplicas,
		MaxAge:       cfg.StreamMaxAge,
		MaxBytes:     cfg.StreamMaxBytes,
		MaxConsumers: -1,
		Discard:      nats.DiscardOld,
		Retention:    nats.LimitsPolicy,
		Duplicates:   duplicates,
	}

	_, err := js.StreamInfo(sc.Name)
	switch {
	case err == nil:
		_, err = js.UpdateStream(sc)
	case errors.Is(err, nats.ErrStreamNotFound):
		_, err = js.AddStream(sc)
		if err == nil {
			slog.Info("Created stream", "name", sc.Name, "subjects", sc.Subjects)
		}
	}
	if err != nil {
		return fmt.Errorf("failed to ensure stream %s: %w", sc.Name, err)
	}
	return nil
}
