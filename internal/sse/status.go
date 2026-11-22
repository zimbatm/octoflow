package sse

import (
	"encoding/json"
	"errors"
	"log/slog"
	"time"
)

// statusTTL is how long Status reuses its answer. Every open landing page
// asks once a minute, and an answer costs some fifty reads of the bucket.
const statusTTL = 10 * time.Second

// Status is the body of GET /status.
type Status struct {
	// Now is when the answer was computed, so a reader with a wrong clock
	// can still tell the age of LastEvent.
	Now   time.Time `json:"now"`
	Start time.Time `json:"start"`
	// Step is the seconds per entry of Events and Refused.
	Step       int        `json:"step"`
	Events     []int64    `json:"events"`
	Refused    []int64    `json:"refused"`
	LastEvent  *time.Time `json:"last_event,omitempty"`
	Events1h   int64      `json:"events_1h"`
	Events24h  int64      `json:"events_24h"`
	Refused1h  int64      `json:"refused_1h"`
	Refused24h int64      `json:"refused_24h"`
}

type statusAnswer struct {
	body []byte
	err  error
	at   time.Time
}

// Status returns the encoded Status, at most statusTTL old. A failure is
// kept as long as an answer: a stream that does not answer must not get
// one request per visitor.
func (h *Handler) Status() ([]byte, error) {
	if h.js == nil {
		return nil, errors.New("no stream")
	}
	h.statusMu.Lock()
	defer h.statusMu.Unlock()
	if !h.lastStatus.at.IsZero() && time.Since(h.lastStatus.at) < statusTTL {
		return h.lastStatus.body, h.lastStatus.err
	}
	body, err := h.readStatus(time.Now())
	h.lastStatus = statusAnswer{body: body, err: err, at: time.Now()}
	return body, err
}

func (h *Handler) readStatus(now time.Time) ([]byte, error) {
	series, err := h.counts.Read(now)
	if err != nil {
		return nil, err
	}
	s := Status{
		Now:     now.UTC().Truncate(time.Second),
		Start:   series.Start,
		Step:    60,
		Events:  series.Events,
		Refused: series.Refused,
	}
	s.Events1h, s.Events24h = totals(series.Events)
	s.Refused1h, s.Refused24h = totals(series.Refused)
	// the stream's own clock: it says when the last message was stored even
	// if the dispatcher, which counts, is behind. A fan-out copy is stored
	// within the second of its event.
	if si, err := h.js.StreamInfo(h.stream); err != nil {
		slog.Warn("Failed to read the last event time", "error", err)
	} else if !si.State.LastTime.IsZero() {
		t := si.State.LastTime.UTC().Truncate(time.Second)
		s.LastEvent = &t
	}
	return json.Marshal(s)
}

func totals(counts []int64) (lastHour, all int64) {
	for i, n := range counts {
		all += n
		if i >= len(counts)-60 {
			lastHour += n
		}
	}
	return lastHour, all
}
