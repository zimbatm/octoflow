package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func fast() Config {
	return Config{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond, Multiplier: 2}
}

func TestDoSucceedsAfterFailures(t *testing.T) {
	calls := 0
	err := Do(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		return nil
	}, fast())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestDoReturnsLastErrorWhenExhausted(t *testing.T) {
	calls := 0
	last := errors.New("still failing")
	err := Do(context.Background(), func(context.Context) error {
		calls++
		if calls == 3 {
			return last
		}
		return errors.New("earlier")
	}, fast())
	if !errors.Is(err, last) {
		t.Errorf("err = %v, want the last error", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want MaxAttempts", calls)
	}
}

func TestDoStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	cfg := fast()
	cfg.InitialDelay = time.Hour
	err := Do(ctx, func(context.Context) error {
		calls++
		cancel()
		return errors.New("fail")
	}, cfg)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1: no retry after cancel", calls)
	}
}

func TestDoDoesNotRunWithCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := Do(ctx, func(context.Context) error { calls++; return nil }, fast())
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Errorf("err = %v, calls = %d; want Canceled and 0", err, calls)
	}
}
