// Package names is the index from GitHub names to the numeric ids that
// subjects carry. The dispatcher writes it from the events it routes; the
// SSE server reads it to resolve a name in a path.
package names

import (
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/pkg/events"
)

const Bucket = "octoflow_names"

// The kinds of name, each the first token of a key.
const (
	Org  = "org"
	Repo = "repo"
	User = "user"
)

const (
	// GitHub allows 39 bytes in a login and 100 in a repository name
	maxPart  = 100
	maxKnown = 1 << 16
	maxCache = 4096
	cacheTTL = time.Minute
	warnGap  = time.Minute
)

// Open opens the index bucket, creating it on first use.
func Open(js nats.JetStreamContext, replicas int) (nats.KeyValue, error) {
	kv, err := js.KeyValue(Bucket)
	if errors.Is(err, nats.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      Bucket,
			Description: "GitHub names to ids",
			Storage:     nats.FileStorage,
			Replicas:    replicas,
		})
	}
	return kv, err
}

// Key builds the key of a name: the kind, then each part in lower case,
// joined by dots. GitHub names are case-insensitive and made of letters,
// digits, '-', '_' and '.'; any other byte makes the name invalid, which
// also keeps wildcards and separators out of the key. A '.' in a part is
// written '=': a KV key is a subject, where '.' separates tokens and may
// not lead, trail or repeat, and a repository may be called .github.
func Key(kind string, parts ...string) (string, bool) {
	var b strings.Builder
	b.WriteString(kind)
	for _, part := range parts {
		if part == "" || len(part) > maxPart {
			return "", false
		}
		b.WriteByte('.')
		for i := 0; i < len(part); i++ {
			c := part[i]
			switch {
			case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_':
				b.WriteByte(c)
			case c >= 'A' && c <= 'Z':
				b.WriteByte(c + 'a' - 'A')
			case c == '.':
				b.WriteByte('=')
			default:
				return "", false
			}
		}
	}
	return b.String(), true
}

func isID(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Writer records the names of events. It remembers what it wrote, so the
// bucket gets one write per new or changed pair and not one per event.
type Writer struct {
	kv       nats.KeyValue
	mu       sync.Mutex
	known    map[string]string
	lastWarn time.Time
}

func NewWriter(kv nats.KeyValue) *Writer {
	return &Writer{kv: kv, known: map[string]string{}}
}

// Record indexes the organization, the repository and the sender of event.
// It never fails: a pair that could not be written is tried again with the
// next event that carries it. A nil Writer records nothing.
func (w *Writer) Record(event *events.GitHubEvent) {
	if w == nil {
		return
	}
	w.put(event.OrganizationID, Org, event.Organization)
	if owner, name, ok := strings.Cut(event.Repository, "/"); ok {
		w.put(event.RepositoryID, Repo, owner, name)
	}
	w.put(event.SenderID, User, event.Sender)
}

func (w *Writer) put(id, kind string, parts ...string) {
	key, ok := Key(kind, parts...)
	if !ok || !isID(id) {
		return
	}
	w.mu.Lock()
	known := w.known[key] == id
	w.mu.Unlock()
	if known {
		return
	}
	// not under the lock: a slow bucket must not stop the other workers on
	// names they know. Two workers may write the same new pair; that is harmless.
	if _, err := w.kv.PutString(key, id); err != nil {
		w.warn(key, err)
		return
	}
	w.mu.Lock()
	if len(w.known) >= maxKnown {
		clear(w.known)
	}
	w.known[key] = id
	w.mu.Unlock()
}

// warn logs at most once per warnGap: when the bucket is down, every event
// fails the same way.
func (w *Writer) warn(key string, err error) {
	w.mu.Lock()
	quiet := time.Since(w.lastWarn) < warnGap
	if !quiet {
		w.lastWarn = time.Now()
	}
	w.mu.Unlock()
	if !quiet {
		slog.Warn("Failed to write the name index", "key", key, "error", err)
	}
}

type cached struct {
	id    string
	until time.Time
}

// Resolver looks names up. It keeps an answer for cacheTTL, so a name that
// moved to another id is followed after that time at the latest.
type Resolver struct {
	kv    nats.KeyValue
	now   func() time.Time
	mu    sync.Mutex
	cache map[string]cached
}

func NewResolver(kv nats.KeyValue) *Resolver {
	return &Resolver{kv: kv, now: time.Now, cache: map[string]cached{}}
}

// Resolve returns the id of a name, or "" when the index does not have it
// or the name is not a valid one. An unknown name is not cached, so it
// resolves with the first event that carries it. A nil Resolver knows no
// name.
func (r *Resolver) Resolve(kind string, parts ...string) (string, error) {
	if r == nil {
		return "", nil
	}
	key, ok := Key(kind, parts...)
	if !ok {
		return "", nil
	}
	r.mu.Lock()
	c, hit := r.cache[key]
	r.mu.Unlock()
	if hit && r.now().Before(c.until) {
		return c.id, nil
	}
	entry, err := r.kv.Get(key)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	id := string(entry.Value())
	// the id goes into a subject; trust the bucket no more than the client
	if !isID(id) {
		return "", nil
	}
	r.mu.Lock()
	if len(r.cache) >= maxCache {
		clear(r.cache)
	}
	r.cache[key] = cached{id: id, until: r.now().Add(cacheTTL)}
	r.mu.Unlock()
	return id, nil
}
