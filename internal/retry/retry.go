package retry

import (
	"context"
	"log/slog"
	"time"
)

// RetryConfig holds retry configuration
type RetryConfig struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

// DefaultConfig returns default retry configuration
func DefaultConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     5 * time.Second,
		Multiplier:   2.0,
	}
}

// Operation is a function that can be retried
type Operation func(ctx context.Context) error

// Do executes an operation with exponential backoff retry
func Do(ctx context.Context, op Operation, config RetryConfig) error {
	var lastErr error
	delay := config.InitialDelay

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Try the operation
		if err := op(ctx); err == nil {
			// Success
			if attempt > 1 {
				slog.Debug("Operation succeeded after retry",
					"attempt", attempt,
					"total_attempts", config.MaxAttempts)
			}
			return nil
		} else {
			lastErr = err

			// Log retry attempt
			if attempt < config.MaxAttempts {
				slog.Warn("Operation failed, will retry",
					"attempt", attempt,
					"max_attempts", config.MaxAttempts,
					"next_delay", delay,
					"error", err)

				// Wait before next attempt
				select {
				case <-time.After(delay):
					// Continue to next attempt
				case <-ctx.Done():
					return ctx.Err()
				}

				// Calculate next delay with exponential backoff
				delay = time.Duration(float64(delay) * config.Multiplier)
				if delay > config.MaxDelay {
					delay = config.MaxDelay
				}
			} else {
				slog.Error("Operation failed after all retries",
					"attempts", attempt,
					"error", err)
			}
		}
	}

	return lastErr
}
