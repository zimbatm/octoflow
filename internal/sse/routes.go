package sse

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	natsInternal "github.com/zimbatm/octoflow/internal/nats"
	"github.com/zimbatm/octoflow/pkg/events"
)

// Routes serves the SSE endpoints, /health and /stats. Every id in a path
// must be numeric; a subject may not take wildcards from a client.
func Routes(h *Handler, prefix string) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"healthy","service":"sse-server"}`)
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := json.Marshal(h.GetStats())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(stats)
	})

	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		serve(h, w, r, "all", natsInternal.PrimarySubjects(prefix))
	})

	mux.HandleFunc("GET /events/org/{org_id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := numeric(w, r, "org_id")
		if !ok {
			return
		}
		serve(h, w, r, "org-"+id, []string{fmt.Sprintf("%s.org.%s.>", prefix, id)})
	})

	mux.HandleFunc("GET /events/repo/{repo_id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := numeric(w, r, "repo_id")
		if !ok {
			return
		}
		serve(h, w, r, "repo-"+id, []string{fmt.Sprintf("%s.repo.%s.>", prefix, id)})
	})

	mux.HandleFunc("GET /events/user/{user_id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := numeric(w, r, "user_id")
		if !ok {
			return
		}
		serve(h, w, r, "user-"+id, []string{fmt.Sprintf("%s.user.%s.*", prefix, id)})
	})

	mux.HandleFunc("GET /events/pr/{repo_id}/{number}", func(w http.ResponseWriter, r *http.Request) {
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

	mux.HandleFunc("GET /events/issue/{repo_id}/{number}", func(w http.ResponseWriter, r *http.Request) {
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

	mux.HandleFunc("GET /events/type/{event_type}", func(w http.ResponseWriter, r *http.Request) {
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

	return cors(mux)
}

func serve(h *Handler, w http.ResponseWriter, r *http.Request, streamID string, subjects []string) {
	h.ServeHTTP(w, r, streamID, subjects, filterFromQuery(r))
}

// cors lets browsers on other origins read the streams. The data is public.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Cache-Control, Last-Event-ID")
		if r.Method == http.MethodOptions {
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

// filterFromQuery reads ?type=a,b and ?action=x,y.
func filterFromQuery(r *http.Request) *events.EventFilter {
	types := r.URL.Query().Get("type")
	actions := r.URL.Query().Get("action")
	if types == "" && actions == "" {
		return nil
	}
	f := &events.EventFilter{}
	if types != "" {
		f.Events = strings.Split(types, ",")
	}
	if actions != "" {
		f.Actions = strings.Split(actions, ",")
	}
	return f
}
