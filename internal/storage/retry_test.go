package storage

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func TestRetryGet(t *testing.T) {
	backoff := []time.Duration{time.Millisecond, time.Millisecond}

	t.Run("a broken download is read again", func(t *testing.T) {
		calls := 0
		data, err := retryGet(context.Background(), backoff, func() ([]byte, error) {
			calls++
			if calls < 3 {
				return nil, io.ErrUnexpectedEOF
			}
			return []byte("ok"), nil
		})
		if err != nil || string(data) != "ok" || calls != 3 {
			t.Fatalf("data=%q err=%v calls=%d", data, err, calls)
		}
	})

	t.Run("gives up after the last pause", func(t *testing.T) {
		calls := 0
		_, err := retryGet(context.Background(), backoff, func() ([]byte, error) {
			calls++
			return nil, io.ErrUnexpectedEOF
		})
		if err != io.ErrUnexpectedEOF || calls != 3 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
	})

	t.Run("a missing object is not read again", func(t *testing.T) {
		calls := 0
		_, err := retryGet(context.Background(), backoff, func() ([]byte, error) {
			calls++
			return nil, fmt.Errorf("failed to read k: %w", minio.ErrorResponse{StatusCode: 404, Code: "NoSuchKey"})
		})
		if err == nil || calls != 1 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
	})
}
