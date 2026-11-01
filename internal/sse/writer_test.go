package sse

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriterFramesEvents(t *testing.T) {
	rec := httptest.NewRecorder()
	w, err := NewWriter(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRetry(5000); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteEvent("12", "push", []byte("line1\nline2")); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteEvent("", "", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteComment("heartbeat"); err != nil {
		t.Fatal(err)
	}

	want := "retry: 5000\n\n" +
		"id: 12\nevent: push\ndata: line1\ndata: line2\n\n" +
		"data: {\"a\":1}\n\n" +
		": heartbeat\n\n"
	if got := rec.Body.String(); got != want {
		t.Errorf("stream =\n%q\nwant\n%q", got, want)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content type = %q", ct)
	}
}

func TestWriterNeedsAFlusher(t *testing.T) {
	if _, err := NewWriter(noFlush{}); err == nil {
		t.Error("expected an error for a writer that cannot flush")
	}
}

type noFlush struct{}

func (noFlush) Header() http.Header         { return http.Header{} }
func (noFlush) Write(b []byte) (int, error) { return len(b), nil }
func (noFlush) WriteHeader(int)             {}
