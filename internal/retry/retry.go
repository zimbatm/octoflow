package retry

import (
	"context"
	"log/slog"
	"time"
)

// Config bounds the attempts. The delay grows by Multiplier from
// InitialDelay up to MaxDelay.
type Config struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

func DefaultConfig() Config {
	return Config{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     5 * time.Second,
		Multiplier:   2.0,
	}
}

type Operation func(ctx context.Context) error

// Do runs op until it succeeds, the attempts run out, or ctx ends. It
// returns the last error and leaves reporting it to the caller.
func Do(ctx context.Context, op Operation, cfg Config) error {
	delay := cfg.InitialDelay
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := op(ctx)
		if err == nil {
			return nil
		}
		if attempt >= cfg.MaxAttempts {
			return err
		}
		slog.Warn("Operation failed, will retry",
			"attempt", attempt,
			"max_attempts", cfg.MaxAttempts,
			"next_delay", delay,
			"error", err)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
		delay = min(time.Duration(float64(delay)*cfg.Multiplier), cfg.MaxDelay)
	}
}
