package ghevents

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRepoEvents(t *testing.T) {
	var gotAuth, gotPage, gotIfNoneMatch string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPage = r.URL.Query().Get("page")
		gotIfNoneMatch = r.Header.Get("If-None-Match")
		switch r.URL.Path {
		case "/repos/NixOS/nixpkgs/events":
			w.Header().Set("ETag", `"abc"`)
			w.Header().Set("X-Poll-Interval", "30")
			if gotIfNoneMatch == `"abc"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			_, _ = w.Write([]byte(`[` + apiEvent + `]`))
		case "/repos/NixOS/nixpkgs":
			_, _ = w.Write([]byte(`{"id":4542716,"full_name":"NixOS/nixpkgs","owner":{"id":487568,"login":"NixOS"}}`))
		default:
			http.Error(w, "nope", http.StatusForbidden)
		}
	}))
	defer srv.Close()

	c := NewClient("tok")
	c.BaseURL = srv.URL
	ctx := context.Background()

	page, err := c.RepoEvents(ctx, "NixOS/nixpkgs", 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer tok" || gotPage != "2" {
		t.Errorf("auth = %q, page = %q", gotAuth, gotPage)
	}
	if page.NotModified || len(page.Events) != 1 || page.Events[0].ID != "53000000001" {
		t.Errorf("page = %+v", page)
	}
	if page.ETag != `"abc"` || page.PollInterval != 30*time.Second {
		t.Errorf("etag = %q, poll interval = %v", page.ETag, page.PollInterval)
	}

	again, err := c.RepoEvents(ctx, "NixOS/nixpkgs", 1, page.ETag)
	if err != nil {
		t.Fatal(err)
	}
	if !again.NotModified || again.ETag != `"abc"` || again.PollInterval != 30*time.Second {
		t.Errorf("conditional request: %+v", again)
	}

	info, err := c.Repo(ctx, "NixOS/nixpkgs")
	if err != nil {
		t.Fatal(err)
	}
	if info.ID != 4542716 || info.Owner.ID != 487568 {
		t.Errorf("repo = %+v", info)
	}

	if _, err := c.RepoEvents(ctx, "other/repo", 1, ""); err == nil {
		t.Error("a 403 must be an error")
	}
	if _, err := c.Repo(ctx, "other/repo"); err == nil {
		t.Error("a 403 must be an error")
	}
}
