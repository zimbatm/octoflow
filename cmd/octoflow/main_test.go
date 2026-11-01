package main

import (
	"io"
	"net/http"
	"net/http/httptest"
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

func TestHTTPServicesShareAnAddress(t *testing.T) {
	cfg := &config.Config{}
	cfg.Webhook.ListenAddr, cfg.SSE.ListenAddr, cfg.Dashboard.ListenAddr = ":1", ":1", ":2"
	cfg.Webhook.AllowUnsigned = true
	cfg.Dashboard.WebDir = "../../web"
	servers, err := httpServers(mustPick(t, "all", cfg), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(servers) != 2 || servers[0].Addr != ":1" || servers[1].Addr != ":2" {
		t.Fatalf("servers = %v", servers)
	}

	shared := httptest.NewServer(servers[0].Handler)
	defer shared.Close()
	for path, want := range map[string]int{"/health": 200, "/webhook": 200, "/stats": 200, "/": 404} {
		resp, err := http.Get(shared.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s on the shared server = %d, want %d", path, resp.StatusCode, want)
		}
	}

	dash := httptest.NewServer(servers[1].Handler)
	defer dash.Close()
	resp, err := http.Get(dash.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "new EventSource(") {
		t.Errorf("dashboard / = %d, %d bytes", resp.StatusCode, len(body))
	}
}

func TestWebhookRefusesToStartWithoutASecret(t *testing.T) {
	cfg := &config.Config{}
	cfg.Webhook.ListenAddr = ":1"
	if _, err := httpServers(mustPick(t, "webhook", cfg), cfg, nil); err == nil {
		t.Error("no secret and no WEBHOOK_ALLOW_UNSIGNED must be an error")
	}
}
