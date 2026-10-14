package sse

import (
	"fmt"
	"net/http"
	"strings"
)

// Writer provides a clean abstraction for writing SSE messages
type Writer struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

// NewWriter creates a new SSE writer
func NewWriter(w http.ResponseWriter) (*Writer, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("response writer does not support flushing")
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable Nginx buffering

	return &Writer{
		w:       w,
		flusher: flusher,
	}, nil
}

// WriteEvent writes a complete SSE event
func (w *Writer) WriteEvent(id, eventType string, data []byte) error {
	// Write event ID if provided
	if id != "" {
		if _, err := fmt.Fprintf(w.w, "id: %s\n", id); err != nil {
			return err
		}
	}

	// Write event type if provided
	if eventType != "" {
		if _, err := fmt.Fprintf(w.w, "event: %s\n", eventType); err != nil {
			return err
		}
	}

	// Write data field(s) - handle multi-line data
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if _, err := fmt.Fprintf(w.w, "data: %s\n", line); err != nil {
			return err
		}
	}

	// End of event
	if _, err := fmt.Fprintln(w.w); err != nil {
		return err
	}

	// Flush to client
	w.flusher.Flush()

	return nil
}

// WriteComment writes a comment (for keep-alive)
func (w *Writer) WriteComment(comment string) error {
	if _, err := fmt.Fprintf(w.w, ": %s\n\n", comment); err != nil {
		return err
	}
	w.flusher.Flush()
	return nil
}

// WriteRetry writes a retry directive
func (w *Writer) WriteRetry(milliseconds int) error {
	if _, err := fmt.Fprintf(w.w, "retry: %d\n\n", milliseconds); err != nil {
		return err
	}
	w.flusher.Flush()
	return nil
}
