package sse

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
)

// writeTimeout is how long one write may block. A client that stops
// reading is dropped after it instead of holding its slot and consumer.
const writeTimeout = 30 * time.Second

// Writer frames server-sent events and flushes after each one.
type Writer struct {
	w       http.ResponseWriter
	flusher http.Flusher
	rc      *http.ResponseController
}

func NewWriter(w http.ResponseWriter) (*Writer, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("response writer does not support flushing")
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	return &Writer{w: w, flusher: flusher, rc: http.NewResponseController(w)}, nil
}

// WriteEvent writes one event; data may span several lines.
func (w *Writer) WriteEvent(id, eventType string, data []byte) error {
	w.deadline()
	if id != "" {
		if _, err := fmt.Fprintf(w.w, "id: %s\n", id); err != nil {
			return err
		}
	}
	if eventType != "" {
		if _, err := fmt.Fprintf(w.w, "event: %s\n", eventType); err != nil {
			return err
		}
	}
	for _, line := range bytes.Split(data, []byte("\n")) {
		if _, err := fmt.Fprintf(w.w, "data: %s\n", line); err != nil {
			return err
		}
	}
	return w.end()
}

// WriteComment writes a comment line, which clients ignore; the heartbeat.
func (w *Writer) WriteComment(comment string) error {
	w.deadline()
	if _, err := fmt.Fprintf(w.w, ": %s\n", comment); err != nil {
		return err
	}
	return w.end()
}

// WriteRetry tells the client how long to wait before reconnecting.
func (w *Writer) WriteRetry(milliseconds int) error {
	w.deadline()
	if _, err := fmt.Fprintf(w.w, "retry: %d\n", milliseconds); err != nil {
		return err
	}
	return w.end()
}

func (w *Writer) deadline() {
	// not every ResponseWriter supports it; a recorder in tests does not.
	_ = w.rc.SetWriteDeadline(time.Now().Add(writeTimeout))
}

func (w *Writer) end() error {
	if _, err := io.WriteString(w.w, "\n"); err != nil {
		return err
	}
	w.flusher.Flush()
	return nil
}
