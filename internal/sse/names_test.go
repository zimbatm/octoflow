package sse

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/zimbatm/octoflow/internal/names"
	"github.com/zimbatm/octoflow/pkg/events"
)

// newNamedFixture is newFixture with an index that knows org 1 as NixOS,
// repo 7 as NixOS/nixpkgs and user 9 as zimbatm, and with one copy on a
// pr, an issue and a user subject.
func newNamedFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	kv, err := names.Open(f.js, 1)
	if err != nil {
		t.Fatal(err)
	}
	names.NewWriter(kv).Record(&events.GitHubEvent{
		Organization: "NixOS", OrganizationID: "1",
		Repository: "NixOS/nixpkgs", RepositoryID: "7",
		Sender: "zimbatm", SenderID: "9",
	})
	// a login of digits only, and a value that no dispatcher writes
	for key, id := range map[string]string{"org.2": "1", "user.2": "9", "org.evil": "1.>"} {
		if _, err := kv.PutString(key, id); err != nil {
			t.Fatal(err)
		}
	}
	f.h.Names = names.NewResolver(kv)

	for id, subject := range map[string]string{
		"c-pr":    "p.pr.7.42.pull_request",
		"c-issue": "p.issue.7.3.issues",
		"c-user":  "p.user.9.push",
		"c-user2": "p.user.2.push",
	} {
		data, _ := json.Marshal(&events.GitHubEvent{ID: id, Event: "push", Payload: json.RawMessage(`{}`)})
		if err := f.pub.PublishToSubject(context.Background(), subject, "push", data, id); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func TestNameRoutesResolveInAnyCase(t *testing.T) {
	f := newNamedFixture(t)
	for path, want := range map[string]string{
		"/events/org/nixos?last=2":              "d3 d6",
		"/events/org/NIXOS?last=2":              "d3 d6",
		"/events/repo/nixos/NixPkgs?last=2":     "d5 d6",
		"/events/user/Zimbatm?last=1":           "c-user",
		"/events/pr/NixOS/nixpkgs/42?last=1":    "c-pr",
		"/events/issue/nixos/nixpkgs/3?last=1":  "c-issue",
		"/events/issue/nixos/nixpkgs/3?last=50": "c-issue",
	} {
		got, code := f.read(path, "", len(strings.Fields(want)))
		if strings.Join(ids(got), " ") != want {
			t.Errorf("%s = %v (status %d), want %s", path, ids(got), code, want)
		}
	}
}

func TestIDRoutesAreNotNames(t *testing.T) {
	f := newNamedFixture(t)
	for path, want := range map[string]string{
		"/events/org/1?last=2":     "d3 d6",
		"/events/repo/7?last=2":    "d5 d6",
		"/events/pr/7/42?last=1":   "c-pr",
		"/events/issue/7/3?last=1": "c-issue",
		"/events/user/9?last=1":    "c-user",
		// the index maps the logins "2" to org 1 and user 9: digits stay an id
		"/events/org/2?last=2":  "d4 d5",
		"/events/user/2?last=1": "c-user2",
	} {
		got, code := f.read(path, "", len(strings.Fields(want)))
		if strings.Join(ids(got), " ") != want {
			t.Errorf("%s = %v (status %d), want %s", path, ids(got), code, want)
		}
	}
}

func TestNamesWithoutAnIDAreNotFound(t *testing.T) {
	f := newNamedFixture(t)
	for _, path := range []string{
		"/events/org/numtide",
		"/events/user/nobody",
		"/events/repo/nixos/nix",
		"/events/repo/nixos",
		"/events/pr/nixos/nix/42",
		"/events/issue/nobody/nixpkgs/3",
		"/events/pr/nixos/nixpkgs/x",
		"/events/org/nix*",
		"/events/org/%3E",
		"/events/org/1.%3E",
		"/events/org/1.*",
		"/events/user/9%20",
		"/events/user/dependabot[bot]",
		"/events/repo/nixos/%2A",
		"/events/repo/*/nixpkgs",
		"/events/repo/nixos/nixpkgs%2Fx",
		"/events/repo/nixos/nixpkgs.%3E",
		"/events/pr/nixos/%3E/42",
		"/events/org/evil",
	} {
		resp, err := http.Get(f.server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404", path, resp.StatusCode)
		}
	}
	// a request that got a subject would have opened a stream and a consumer
	if n := f.consumerCount(); n != 0 || f.h.Clients() != 0 {
		t.Errorf("%d consumers and %d clients after requests without an id, want none", n, f.h.Clients())
	}

	resp, err := http.Get(f.server.URL + "/events/repo/nixos/nix")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body.Error, "no event of this repository") {
		t.Errorf("error = %q, want it to say that octoflow has no event of this repository", body.Error)
	}
	if resp.Header.Get("Content-Type") != "application/json" || resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("headers = %v, want JSON that a page on another origin can read", resp.Header)
	}
}

func TestANameResolvesWithItsFirstEvent(t *testing.T) {
	f := newNamedFixture(t)
	if _, code := f.read("/events/org/numtide", "", 0); code != http.StatusNotFound {
		t.Fatalf("before the event: status %d, want 404", code)
	}
	kv, err := names.Open(f.js, 1)
	if err != nil {
		t.Fatal(err)
	}
	// org 2 is renamed: the event that carries the new name is enough
	names.NewWriter(kv).Record(&events.GitHubEvent{Organization: "numtide", OrganizationID: "2"})
	got, code := f.read("/events/org/numtide?last=2", "", 2)
	if want := "d4 d5"; strings.Join(ids(got), " ") != want {
		t.Errorf("after the event = %v (status %d), want %s", ids(got), code, want)
	}
}
