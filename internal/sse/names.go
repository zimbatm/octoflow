package sse

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/zimbatm/octoflow/internal/names"
)

// nameRoutes registers the routes that take a repository as owner/name.
// They have one path segment more than the id routes, so the two never
// match the same request.
func nameRoutes(get func(string, http.HandlerFunc), h *Handler, prefix string) {
	get("/events/repo/{owner}/{name}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := repoID(h, w, r)
		if !ok {
			return
		}
		serve(h, w, r, "repo-"+id, []string{fmt.Sprintf("%s.repo.%s.>", prefix, id)})
	})

	get("/events/pr/{owner}/{name}/{number}", func(w http.ResponseWriter, r *http.Request) {
		n, ok := numeric(w, r, "number")
		if !ok {
			return
		}
		repo, ok := repoID(h, w, r)
		if !ok {
			return
		}
		serve(h, w, r, "pr-"+repo+"-"+n, []string{fmt.Sprintf("%s.pr.%s.%s.>", prefix, repo, n)})
	})

	get("/events/issue/{owner}/{name}/{number}", func(w http.ResponseWriter, r *http.Request) {
		n, ok := numeric(w, r, "number")
		if !ok {
			return
		}
		repo, ok := repoID(h, w, r)
		if !ok {
			return
		}
		serve(h, w, r, "issue-"+repo+"-"+n, []string{fmt.Sprintf("%s.issue.%s.%s.>", prefix, repo, n)})
	})
}

func repoID(h *Handler, w http.ResponseWriter, r *http.Request) (string, bool) {
	return resolve(h, w, "repository", names.Repo, r.PathValue("owner"), r.PathValue("name"))
}

// orgID and userID read a path value that is an id or a login. All digits
// is an id, as before names existed; a login made of digits only is
// reachable by its id alone.
func orgID(h *Handler, w http.ResponseWriter, r *http.Request) (string, bool) {
	return idOrName(h, w, r.PathValue("org_id"), "organization", names.Org)
}

func userID(h *Handler, w http.ResponseWriter, r *http.Request) (string, bool) {
	return idOrName(h, w, r.PathValue("user_id"), "user", names.User)
}

func idOrName(h *Handler, w http.ResponseWriter, v, what, kind string) (string, bool) {
	digits := v != ""
	for _, c := range v {
		if c < '0' || c > '9' {
			digits = false
			break
		}
	}
	if digits {
		return v, true
	}
	return resolve(h, w, what, kind, v)
}

// resolve answers the request itself when it has no id to return. The
// caller builds subjects from the id only: the name stops here.
func resolve(h *Handler, w http.ResponseWriter, what, kind string, parts ...string) (string, bool) {
	id, err := h.Names.Resolve(kind, parts...)
	switch {
	case err != nil:
		slog.Warn("Failed to read the name index", "kind", kind, "error", err)
		jsonError(w, http.StatusServiceUnavailable, "the name index does not answer; use the numeric id")
	case id == "":
		jsonError(w, http.StatusNotFound, fmt.Sprintf(
			"octoflow has no event of this %s; it only knows the names it has seen in an event", what))
	}
	return id, id != ""
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
