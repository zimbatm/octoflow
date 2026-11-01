package main

import (
	"strings"
	"testing"

	"github.com/zimbatm/octoflow/internal/config"
)

func joined(ss []service) string { return strings.Join(names(ss), " ") }

func TestPick(t *testing.T) {
	cfg := &config.Config{}
	got, err := pick("all", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if joined(got) != "webhook dispatcher sse dashboard" {
		t.Errorf("all without S3 or public repos = %q", joined(got))
	}

	cfg.S3.Bucket = "b"
	cfg.PublicIngest.Repos = []string{"NixOS/nixpkgs"}
	got, _ = pick("all", cfg)
	if joined(got) != "webhook dispatcher s3 sse dashboard public-ingest" {
		t.Errorf("all with S3 and public repos = %q", joined(got))
	}

	got, err = pick("s3", cfg)
	if err != nil || joined(got) != "s3" {
		t.Errorf("s3 = %q, %v", joined(got), err)
	}
	if _, err := pick("nope", cfg); err == nil {
		t.Error("unknown service must be an error")
	}
	if needsNATS(mustPick(t, "dashboard", cfg)) {
		t.Error("the dashboard does not need NATS")
	}
	if !needsNATS(mustPick(t, "all", cfg)) {
		t.Error("all needs NATS")
	}
}

func mustPick(t *testing.T, name string, cfg *config.Config) []service {
	t.Helper()
	ss, err := pick(name, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return ss
}
