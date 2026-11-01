package ghevents

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestRepoEventsRateLimit(t *testing.T) {
	reset := time.Now().Add(2 * time.Minute).Truncate(time.Second)
	var mode string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode {
		case "quota-exhausted":
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset.Unix(), 10))
			w.WriteHeader(http.StatusForbidden)
		case "secondary":
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
		case "forbidden":
			w.Header().Set("X-RateLimit-Remaining", "41")
			w.WriteHeader(http.StatusForbidden)
		default:
			w.Header().Set("X-RateLimit-Remaining", "41")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset.Unix(), 10))
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer srv.Close()
	c := NewClient("")
	c.BaseURL = srv.URL
	ctx := context.Background()

	mode = "quota-exhausted"
	_, err := c.RepoEvents(ctx, "o/r", 1, "")
	var rl *RateLimitError
	if !errors.As(err, &rl) || !rl.Reset.Equal(reset) {
		t.Errorf("403 with quota 0: err = %v, want RateLimitError until %v", err, reset)
	}
	if _, err := c.Repo(ctx, "o/r"); !errors.As(err, &rl) {
		t.Errorf("Repo under rate limit: err = %v", err)
	}

	mode = "secondary"
	_, err = c.RepoEvents(ctx, "o/r", 1, "")
	if !errors.As(err, &rl) || time.Until(rl.Reset) < 20*time.Second {
		t.Errorf("429 with Retry-After: err = %v", err)
	}

	mode = "forbidden"
	_, err = c.RepoEvents(ctx, "o/r", 1, "")
	if err == nil || errors.As(err, &rl) {
		t.Errorf("403 with quota left must be a plain error, got %v", err)
	}

	mode = "ok"
	page, err := c.RepoEvents(ctx, "o/r", 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if page.Quota.Remaining != 41 || !page.Quota.Reset.Equal(reset) {
		t.Errorf("quota = %+v", page.Quota)
	}
}
