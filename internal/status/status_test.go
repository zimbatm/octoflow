package status

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/natstest"
)

func at(s *Series, t time.Time) int {
	return int(t.UTC().Truncate(time.Minute).Sub(s.Start) / time.Minute)
}

func read(t *testing.T, js nats.JetStreamContext, now time.Time) *Series {
	t.Helper()
	s, err := NewReader(js).Read(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Events) != Minutes || len(s.Refused) != Minutes {
		t.Fatalf("series of %d and %d minutes, want %d", len(s.Events), len(s.Refused), Minutes)
	}
	return s
}

func TestCountsSurviveARestartOfTheCounter(t *testing.T) {
	_, js := natstest.Start(t)
	now := time.Now()
	early, late := now.Add(-90*time.Minute), now.Add(-2*time.Minute)

	first := NewCounter(js, 1)
	first.Add(Events, early, 3)
	first.Add(Events, late, 4)
	first.Add(Refused, late, 1)
	first.Close()

	second := NewCounter(js, 1)
	second.Add(Events, late, 5)
	second.Close()

	s := read(t, js, now)
	if got := s.Events[at(s, early)]; got != 3 {
		t.Errorf("events 90 minutes ago = %d, want 3", got)
	}
	if got := s.Events[at(s, late)]; got != 9 {
		t.Errorf("events 2 minutes ago = %d, want 4 from before the restart and 5 after", got)
	}
	if got := s.Refused[at(s, late)]; got != 1 {
		t.Errorf("refused 2 minutes ago = %d, want 1", got)
	}
	var total int64
	for _, n := range s.Events {
		total += n
	}
	if total != 12 {
		t.Errorf("events in 24 hours = %d, want 12", total)
	}
	if want := now.UTC().Truncate(time.Minute).Add(-(Minutes - 1) * time.Minute); !s.Start.Equal(want) {
		t.Errorf("start = %v, want %v", s.Start, want)
	}
}

func TestTwoCountersDoNotOverwriteEachOther(t *testing.T) {
	_, js := natstest.Start(t)
	now := time.Now()
	minute := now.Add(-time.Minute)

	var wg sync.WaitGroup
	for range 2 {
		c := NewCounter(js, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				c.Add(Events, minute, 1)
				_ = c.Flush()
			}
			c.Close()
		}()
	}
	wg.Wait()

	s := read(t, js, now)
	if got := s.Events[at(s, minute)]; got != 100 {
		t.Errorf("events = %d, want 100 from two writers of one key", got)
	}
}

func TestCounterCreatesADeletedBucketAgain(t *testing.T) {
	_, js := natstest.Start(t)
	now := time.Now()
	c := NewCounter(js, 1)
	defer c.Close()
	c.Add(Events, now, 1)
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := js.DeleteKeyValue(Bucket); err != nil {
		t.Fatal(err)
	}
	c.Add(Events, now, 2)
	// the write that finds the bucket gone keeps its counts for the next one
	if err := c.Flush(); err != nil {
		if err := c.Flush(); err != nil {
			t.Fatalf("second write after the delete: %v", err)
		}
	}
	s := read(t, js, now)
	if got := s.Events[at(s, now)]; got != 2 {
		t.Errorf("events = %d, want the 2 counted after the delete", got)
	}
}

func TestOldTimesAreDropped(t *testing.T) {
	_, js := natstest.Start(t)
	now := time.Now()
	c := NewCounter(js, 1)
	c.Add(Events, now.Add(-25*time.Hour), 7)
	c.Close()
	if _, err := js.KeyValue(Bucket); err == nil {
		t.Error("a count older than the series created the bucket")
	}
}

func TestReadWithoutABucketIsZeros(t *testing.T) {
	_, js := natstest.Start(t)
	s := read(t, js, time.Now())
	for i := range s.Events {
		if s.Events[i] != 0 || s.Refused[i] != 0 {
			t.Fatalf("minute %d is not zero", i)
		}
	}
	if _, err := js.KeyValue(Bucket); err == nil {
		t.Error("reading created the bucket")
	}
}

func TestBucketExpiresItsEntries(t *testing.T) {
	_, js := natstest.Start(t)
	c := NewCounter(js, 1)
	c.Add(Events, time.Now(), 1)
	c.Close()
	kv, err := js.KeyValue(Bucket)
	if err != nil {
		t.Fatal(err)
	}
	st, err := kv.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.TTL() < 25*time.Hour {
		t.Errorf("entries expire after %v; a reader needs 24 hours and the current one", st.TTL())
	}
}

func TestRaiseKeepsTheLargerCount(t *testing.T) {
	_, js := natstest.Start(t)
	at := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)
	c := NewCounter(js, 1)
	c.Add(Events, at, 5)
	c.Close()

	raised, err := Raise(js, 1, Events, map[time.Time]int64{
		at:                       3,
		at.Add(-time.Minute):     7,
		at.Add(-30 * time.Hour):  9,
		at.Add(20 * time.Minute): 9,
	})
	if err != nil || raised != 1 {
		t.Fatalf("raised %d, %v; want 1", raised, err)
	}
	s, err := NewReader(js).Read(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	at0 := int(at.Sub(s.Start) / time.Minute)
	if s.Events[at0] != 5 || s.Events[at0-1] != 7 {
		t.Errorf("counts %d and %d, want 5 and 7", s.Events[at0], s.Events[at0-1])
	}
	if raised, err := Raise(js, 1, Events, map[time.Time]int64{at.Add(-time.Minute): 7}); err != nil || raised != 0 {
		t.Errorf("a second run raised %d, %v", raised, err)
	}
}
