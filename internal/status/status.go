// Package status keeps per-minute counts in a NATS KV bucket: the events
// that entered the stream and the deliveries the receiver refused. The
// SSE server reads them for GET /status; no count lives in one process.
package status

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const Bucket = "octoflow_status"

// The two series. Each has its own keys, so their writers never collide.
const (
	Events  = "events"
	Refused = "refused"
)

// Minutes is the length of a Series: 24 hours.
const Minutes = 24 * 60

// entryTTL outlives the 24 hours that are read, with room for a
// dispatcher that works through a backlog.
const entryTTL = 48 * time.Hour

// flushEvery bounds the writes: at most one per key in this time, whatever
// the event rate.
const flushEvery = 5 * time.Second

// hour holds the counts of the 60 minutes of one key.
type hour [60]int64

func key(kind string, at time.Time) string {
	return kind + "." + at.UTC().Format("2006-01-02T15")
}

// Counter adds counts to the bucket. It collects them in memory and writes
// each touched hour at most every five seconds; Close writes the rest. A
// write that fails is tried again with the next one, so a NATS outage
// loses nothing while the process lives.
type Counter struct {
	js       nats.JetStreamContext
	replicas int

	mu      sync.Mutex
	pending map[string]*hour

	// one flush at a time; guards kv
	flushMu sync.Mutex
	kv      nats.KeyValue

	dirty chan struct{}
	stop  chan struct{}
	done  chan struct{}
	close sync.Once
}

// NewCounter starts the writer. The bucket is opened, and created with
// replicas, at the first write: a service does not wait for it to start.
func NewCounter(js nats.JetStreamContext, replicas int) *Counter {
	c := &Counter{
		js:       js,
		replicas: replicas,
		pending:  map[string]*hour{},
		dirty:    make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go c.run()
	return c
}

// Add counts n in the minute of at. A time outside the hours that Read
// returns is dropped: a dispatcher that replays a week must not write it.
func (c *Counter) Add(kind string, at time.Time, n int64) {
	if age := time.Since(at); age > Minutes*time.Minute || age < -time.Hour {
		return
	}
	c.mu.Lock()
	k := key(kind, at)
	h := c.pending[k]
	if h == nil {
		h = &hour{}
		c.pending[k] = h
	}
	h[at.UTC().Minute()] += n
	c.mu.Unlock()
	select {
	case c.dirty <- struct{}{}:
	default:
	}
}

func (c *Counter) run() {
	defer close(c.done)
	for {
		select {
		case <-c.stop:
			return
		case <-c.dirty:
		}
		if err := c.Flush(); err != nil {
			slog.Warn("Failed to write status counts", "bucket", Bucket, "error", err)
		}
		select {
		case <-c.stop:
			return
		case <-time.After(flushEvery):
		}
	}
}

// Close stops the writer and writes what is left. The NATS connection must
// still be open.
func (c *Counter) Close() {
	c.close.Do(func() {
		close(c.stop)
		<-c.done
		if err := c.Flush(); err != nil {
			slog.Warn("Failed to write the last status counts", "bucket", Bucket, "error", err)
		}
	})
}

// Flush writes the collected counts now. What it cannot write stays
// collected.
func (c *Counter) Flush() error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.mu.Lock()
	pending := c.pending
	c.pending = map[string]*hour{}
	c.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}

	var first error
	for k, delta := range pending {
		err := c.add(k, delta)
		if err == nil {
			continue
		}
		if first == nil {
			first = err
		}
		c.mu.Lock()
		if h := c.pending[k]; h != nil {
			for i, n := range delta {
				h[i] += n
			}
		} else {
			c.pending[k] = delta
		}
		c.mu.Unlock()
	}
	if first != nil {
		select {
		case c.dirty <- struct{}{}:
		default:
		}
	}
	return first
}

// add is a compare-and-set: another process may write the same key, two
// dispatchers for example, and neither may overwrite the other's counts.
func (c *Counter) add(k string, delta *hour) error {
	if c.kv == nil {
		kv, err := openBucket(c.js, c.replicas)
		if err != nil {
			return err
		}
		c.kv = kv
	}
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		var h hour
		var entry nats.KeyValueEntry
		entry, err = c.kv.Get(k)
		switch {
		case err == nil:
			// an unreadable value starts again from zero
			_ = json.Unmarshal(entry.Value(), &h)
			_, err = c.kv.Update(k, sum(&h, delta), entry.Revision())
		case errors.Is(err, nats.ErrKeyNotFound):
			_, err = c.kv.Create(k, sum(&h, delta))
		}
		if err == nil || !conflict(err) {
			break
		}
	}
	if err != nil {
		// the bucket may be gone; open or create it again next time
		c.kv = nil
		return fmt.Errorf("key %s: %w", k, err)
	}
	return nil
}

func sum(h, delta *hour) []byte {
	for i, n := range delta {
		h[i] += n
	}
	data, _ := json.Marshal(h)
	return data
}

func conflict(err error) bool {
	if errors.Is(err, nats.ErrKeyExists) {
		return true
	}
	var api *nats.APIError
	return errors.As(err, &api) &&
		(api.ErrorCode == nats.JSErrCodeStreamWrongLastSequence || api.ErrorCode == nats.JSErrCodeStreamWrongLastSequenceConstant)
}

func openBucket(js nats.JetStreamContext, replicas int) (nats.KeyValue, error) {
	kv, err := js.KeyValue(Bucket)
	if errors.Is(err, nats.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      Bucket,
			Description: "per-minute counts for GET /status",
			Storage:     nats.FileStorage,
			Replicas:    replicas,
			TTL:         entryTTL,
		})
	}
	return kv, err
}

// Series is the last 24 hours, one count per minute. The last minute is
// the current one and still grows.
type Series struct {
	Start   time.Time
	Events  []int64
	Refused []int64
}

// Reader reads the bucket and never creates it: before the first count
// there is no bucket, and the answer is zeros.
type Reader struct {
	js nats.JetStreamContext
	mu sync.Mutex
	kv nats.KeyValue
}

func NewReader(js nats.JetStreamContext) *Reader {
	return &Reader{js: js}
}

// Read returns the Minutes minutes that end with the minute of now. It
// costs one direct get per hour and series, and no consumer.
func (r *Reader) Read(now time.Time) (*Series, error) {
	end := now.UTC().Truncate(time.Minute)
	s := &Series{
		Start:   end.Add(-(Minutes - 1) * time.Minute),
		Events:  make([]int64, Minutes),
		Refused: make([]int64, Minutes),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.kv == nil {
		kv, err := r.js.KeyValue(Bucket)
		if errors.Is(err, nats.ErrBucketNotFound) {
			return s, nil
		}
		if err != nil {
			return nil, err
		}
		r.kv = kv
	}
	for kind, out := range map[string][]int64{Events: s.Events, Refused: s.Refused} {
		for t := s.Start.Truncate(time.Hour); !t.After(end); t = t.Add(time.Hour) {
			entry, err := r.kv.Get(key(kind, t))
			if errors.Is(err, nats.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				// the bucket may be gone; look it up again next time
				r.kv = nil
				return nil, err
			}
			var h hour
			if err := json.Unmarshal(entry.Value(), &h); err != nil {
				continue
			}
			for m, n := range h {
				if i := int(t.Add(time.Duration(m)*time.Minute).Sub(s.Start) / time.Minute); i >= 0 && i < Minutes {
					out[i] = n
				}
			}
		}
	}
	return s, nil
}
