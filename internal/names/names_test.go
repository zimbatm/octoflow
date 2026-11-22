package names

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/natstest"
	"github.com/zimbatm/octoflow/pkg/events"
)

func TestKey(t *testing.T) {
	tests := []struct {
		kind  string
		parts []string
		want  string
	}{
		{Org, []string{"NixOS"}, "org.nixos"},
		{User, []string{"some_user-1"}, "user.some_user-1"},
		{Repo, []string{"NixOS", "nixpkgs"}, "repo.nixos.nixpkgs"},
		{Repo, []string{"numtide", ".github"}, "repo.numtide.=github"},
		{Repo, []string{"a", "b..c."}, "repo.a.b==c="},
		{Org, []string{""}, ""},
		{Org, []string{"a*"}, ""},
		{Org, []string{">"}, ""},
		{Org, []string{"a b"}, ""},
		{Org, []string{"a=b"}, ""},
		{User, []string{"dependabot[bot]"}, ""},
		{User, []string{"zimbätm"}, ""},
		{Repo, []string{"a", "b/c"}, ""},
		{Repo, []string{"a", ""}, ""},
		{Org, []string{strings.Repeat("a", 101)}, ""},
	}
	for _, tt := range tests {
		got, ok := Key(tt.kind, tt.parts...)
		if got != tt.want || ok != (tt.want != "") {
			t.Errorf("Key(%s, %q) = %q, %v; want %q", tt.kind, tt.parts, got, ok, tt.want)
		}
	}
}

func open(t *testing.T) (nats.JetStreamContext, nats.KeyValue) {
	t.Helper()
	_, js := natstest.Start(t)
	kv, err := Open(js, 1)
	if err != nil {
		t.Fatal(err)
	}
	return js, kv
}

func value(t *testing.T, kv nats.KeyValue, key string) (string, uint64) {
	t.Helper()
	e, err := kv.Get(key)
	if err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	return string(e.Value()), e.Revision()
}

func TestWriterWritesAPairOnce(t *testing.T) {
	_, kv := open(t)
	w := NewWriter(kv)
	ev := &events.GitHubEvent{
		Organization: "NixOS", OrganizationID: "1",
		Repository: "NixOS/nixpkgs", RepositoryID: "7",
		Sender: "Zimbatm", SenderID: "9",
	}
	w.Record(ev)
	for key, want := range map[string]string{"org.nixos": "1", "repo.nixos.nixpkgs": "7", "user.zimbatm": "9"} {
		if got, _ := value(t, kv, key); got != want {
			t.Errorf("%s = %s, want %s", key, got, want)
		}
	}

	for i := 0; i < 5; i++ {
		w.Record(ev)
	}
	// revisions count the writes to the bucket: three keys, three writes
	if _, rev := value(t, kv, "user.zimbatm"); rev != 3 {
		t.Errorf("revision %d after the same event six times, want 3: a known pair must not be written again", rev)
	}

	renamed := *ev
	renamed.Repository = "NixOS/pkgs"
	w.Record(&renamed)
	if got, rev := value(t, kv, "repo.nixos.pkgs"); got != "7" || rev != 4 {
		t.Errorf("new name = %s at revision %d, want 7 at 4", got, rev)
	}
	if got, _ := value(t, kv, "repo.nixos.nixpkgs"); got != "7" {
		t.Errorf("old name = %s, want it to stay on 7", got)
	}

	reused := *ev
	reused.RepositoryID = "8"
	w.Record(&reused)
	if got, _ := value(t, kv, "repo.nixos.nixpkgs"); got != "8" {
		t.Errorf("name taken by another repository = %s, want 8", got)
	}
}

func TestWriterSkipsWhatIsNotAName(t *testing.T) {
	_, kv := open(t)
	w := NewWriter(kv)
	w.Record(&events.GitHubEvent{
		Organization: "NixOS", OrganizationID: "1.>",
		Repository: "nixpkgs", RepositoryID: "7",
		Sender: "dependabot[bot]", SenderID: "9",
	})
	keys, err := kv.Keys()
	if err != nats.ErrNoKeysFound {
		t.Errorf("keys = %v, %v; want none: an id with a wildcard, a repository without owner, a login with brackets", keys, err)
	}
	var none *Writer
	none.Record(&events.GitHubEvent{})
}

func TestWriterTriesAFailedPairAgain(t *testing.T) {
	js, kv := open(t)
	w := NewWriter(kv)
	ev := &events.GitHubEvent{Sender: "zimbatm", SenderID: "9"}

	if err := js.DeleteKeyValue(Bucket); err != nil {
		t.Fatal(err)
	}
	w.Record(ev)

	kv, err := Open(js, 1)
	if err != nil {
		t.Fatal(err)
	}
	w.Record(ev)
	if got, _ := value(t, kv, "user.zimbatm"); got != "9" {
		t.Errorf("user.zimbatm = %s, want 9", got)
	}
}

func TestResolver(t *testing.T) {
	_, kv := open(t)
	NewWriter(kv).Record(&events.GitHubEvent{Repository: "NixOS/nixpkgs", RepositoryID: "7"})
	if _, err := kv.PutString("org.evil", "1.>"); err != nil {
		t.Fatal(err)
	}
	r := NewResolver(kv)
	now := time.Now()
	r.now = func() time.Time { return now }

	tests := []struct {
		kind  string
		parts []string
		want  string
	}{
		{Repo, []string{"nixos", "NIXPKGS"}, "7"},
		{Repo, []string{"nixos", "nix"}, ""},
		{Repo, []string{"nixos", "*"}, ""},
		{Org, []string{"evil"}, ""},
	}
	for _, tt := range tests {
		if got, err := r.Resolve(tt.kind, tt.parts...); err != nil || got != tt.want {
			t.Errorf("Resolve(%s, %q) = %q, %v; want %q", tt.kind, tt.parts, got, err, tt.want)
		}
	}

	if _, err := kv.PutString("repo.nixos.nixpkgs", "8"); err != nil {
		t.Fatal(err)
	}
	if got, _ := r.Resolve(Repo, "NixOS", "nixpkgs"); got != "7" {
		t.Errorf("within the expiry = %q, want the cached 7", got)
	}
	now = now.Add(cacheTTL)
	if got, _ := r.Resolve(Repo, "NixOS", "nixpkgs"); got != "8" {
		t.Errorf("after the expiry = %q, want 8", got)
	}

	// a name without an event is not cached: it resolves with its first event
	if _, err := kv.PutString("repo.nixos.nix", "5"); err != nil {
		t.Fatal(err)
	}
	if got, _ := r.Resolve(Repo, "NixOS", "nix"); got != "5" {
		t.Errorf("a name that just appeared = %q, want 5", got)
	}

	var none *Resolver
	if got, err := none.Resolve(Org, "nixos"); got != "" || err != nil {
		t.Errorf("nil resolver = %q, %v", got, err)
	}
}
