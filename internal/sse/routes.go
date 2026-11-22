package sse

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/pkg/events"
)

// Routes registers the SSE endpoints and /stats on mux. Only numeric ids
// reach a subject, which may not take wildcards from a client; a name in a
// path is first resolved to its id (names.go).
func Routes(mux *http.ServeMux, h *Handler, prefix string) {
	get := func(pattern string, fn http.HandlerFunc) {
		mux.Handle("GET "+pattern, cors(fn))
		mux.Handle("OPTIONS "+pattern, cors(nil))
	}

	// events_stored and stored_since are left out when the stream cannot
	// say; the client count never depends on NATS.
	get("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := map[string]any{"total_clients": h.Clients()}
		if n, since, err := h.Stored(natsInternal.PrimarySubjects(prefix)); err != nil {
			slog.Warn("Failed to count stored events", "error", err)
		} else {
			stats["events_stored"] = n
			if !since.IsZero() {
				stats["stored_since"] = since.UTC().Format(time.RFC3339)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	})

	get("/site", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(h.Site)
	})

	get("/events", func(w http.ResponseWriter, r *http.Request) {
		// the primary subjects, with org.> written so that the last token
		// is the type
		serve(h, w, r, "all", []string{prefix + ".org.*.>", prefix + ".global.>"})
	})

	get("/events/org/{org_id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := orgID(h, w, r)
		if !ok {
			return
		}
		serve(h, w, r, "org-"+id, []string{fmt.Sprintf("%s.org.%s.>", prefix, id)})
	})

	get("/events/repo/{repo_id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := numeric(w, r, "repo_id")
		if !ok {
			return
		}
		serve(h, w, r, "repo-"+id, []string{fmt.Sprintf("%s.repo.%s.>", prefix, id)})
	})

	get("/events/user/{user_id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := userID(h, w, r)
		if !ok {
			return
		}
		serve(h, w, r, "user-"+id, []string{fmt.Sprintf("%s.user.%s.*", prefix, id)})
	})

	get("/events/pr/{repo_id}/{number}", func(w http.ResponseWriter, r *http.Request) {
		repo, ok := numeric(w, r, "repo_id")
		if !ok {
			return
		}
		n, ok := numeric(w, r, "number")
		if !ok {
			return
		}
		serve(h, w, r, "pr-"+repo+"-"+n, []string{fmt.Sprintf("%s.pr.%s.%s.>", prefix, repo, n)})
	})

	get("/events/issue/{repo_id}/{number}", func(w http.ResponseWriter, r *http.Request) {
		repo, ok := numeric(w, r, "repo_id")
		if !ok {
			return
		}
		n, ok := numeric(w, r, "number")
		if !ok {
			return
		}
		serve(h, w, r, "issue-"+repo+"-"+n, []string{fmt.Sprintf("%s.issue.%s.%s.>", prefix, repo, n)})
	})

	nameRoutes(get, h, prefix)

	get("/events/type/{event_type}", func(w http.ResponseWriter, r *http.Request) {
		t := r.PathValue("event_type")
		if !isEventType(t) {
			http.NotFound(w, r)
			return
		}
		serve(h, w, r, "type-"+t, []string{
			fmt.Sprintf("%s.org.*.%s", prefix, t),
			fmt.Sprintf("%s.global.%s", prefix, t),
		})
	})
}

func serve(h *Handler, w http.ResponseWriter, r *http.Request, streamID string, subjects []string) {
	q := r.URL.Query()
	subjects, err := selectTypes(subjects, q.Get("type"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var filter *events.EventFilter
	if actions := q.Get("action"); actions != "" {
		filter = &events.EventFilter{Actions: strings.Split(actions, ",")}
	}
	h.Stream(w, r, streamID, subjects, filter)
}

// maxTypes bounds the subjects of one consumer; GitHub has fewer event types.
const maxTypes = 100

// selectTypes narrows subjects to the comma-separated types: every subject
// ends in the event type, so a type takes the place of the last token. The
// server then filters, and a replay counts only these types. A subject
// that already ends in a type stays if that type is listed.
func selectTypes(subjects []string, list string) ([]string, error) {
	if list == "" {
		return subjects, nil
	}
	var types []string
	for _, t := range strings.Split(list, ",") {
		if !isEventType(t) {
			return nil, fmt.Errorf("type: %q is not an event type; give lower-case letters and underscores, with commas between types", t)
		}
		// JetStream refuses a consumer that has the same subject twice
		if !slices.Contains(types, t) {
			types = append(types, t)
		}
	}
	if len(types) > maxTypes {
		return nil, fmt.Errorf("type: more than %d event types", maxTypes)
	}
	var out []string
	for _, s := range subjects {
		i := strings.LastIndexByte(s, '.') + 1
		switch last := s[i:]; {
		case last == ">" || last == "*":
			for _, t := range types {
				out = append(out, s[:i]+t)
			}
		case slices.Contains(types, last):
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("type: this route has one event type, and the list does not have it")
	}
	return out, nil
}

// cors lets browsers on other origins read the streams; the data is
// public. A nil next answers the preflight.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Cache-Control, Last-Event-ID")
		if next == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func numeric(w http.ResponseWriter, r *http.Request, name string) (string, bool) {
	v := r.PathValue(name)
	if v == "" {
		http.NotFound(w, r)
		return "", false
	}
	for _, c := range v {
		if c < '0' || c > '9' {
			http.NotFound(w, r)
			return "", false
		}
	}
	return v, true
}

func isEventType(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && c != '_' {
			return false
		}
	}
	return true
}
