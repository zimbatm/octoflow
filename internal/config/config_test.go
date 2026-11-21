package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadFromEnvDefaults(t *testing.T) {
	for _, v := range []string{"WEBHOOK_LISTEN_ADDR", "WEBHOOK_SECRET", "NATS_URL", "NATS_STREAM_NAME", "S3_BUCKET", "SSE_LISTEN_ADDR"} {
		t.Setenv(v, "")
	}

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() failed: %v", err)
	}

	if cfg.Webhook.ListenAddr != ":8080" {
		t.Errorf("Expected webhook listen addr :8080, got %s", cfg.Webhook.ListenAddr)
	}
	if cfg.NATS.StreamName != "GITHUB_EVENTS_RAW" {
		t.Errorf("Expected stream name GITHUB_EVENTS_RAW, got %s", cfg.NATS.StreamName)
	}
	if cfg.SSE.ListenAddr != ":8080" || cfg.Dashboard.ListenAddr != ":8080" {
		t.Errorf("SSE and dashboard must share :8080 by default, got %s and %s", cfg.SSE.ListenAddr, cfg.Dashboard.ListenAddr)
	}
	if err := cfg.S3.Validate(); err == nil {
		t.Error("Expected S3 validation error when S3_BUCKET is missing")
	}
}

func TestLoadFromEnvOverrides(t *testing.T) {
	t.Setenv("HTTP_LISTEN_ADDR", ":7070")
	t.Setenv("WEBHOOK_LISTEN_ADDR", ":9090")
	t.Setenv("WEBHOOK_SECRET", "my-secret")
	t.Setenv("NATS_URL", "nats://custom:4222")
	t.Setenv("S3_BUCKET", "test-bucket")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() failed: %v", err)
	}

	if cfg.Webhook.ListenAddr != ":9090" {
		t.Errorf("Expected webhook listen addr :9090, got %s", cfg.Webhook.ListenAddr)
	}
	if cfg.Webhook.Secret != "my-secret" {
		t.Errorf("Expected webhook secret 'my-secret', got %s", cfg.Webhook.Secret)
	}
	if cfg.NATS.Servers[0] != "nats://custom:4222" {
		t.Errorf("Expected NATS server nats://custom:4222, got %s", cfg.NATS.Servers[0])
	}
	if err := cfg.S3.Validate(); err != nil {
		t.Errorf("Expected S3 config to validate: %v", err)
	}
}

func TestParseBytes(t *testing.T) {
	for in, want := range map[string]int64{"0": 0, "1024": 1024, "10K": 10 << 10, "10KiB": 10 << 10, "2MB": 2 << 20, "10GiB": 10 << 30, "1T": 1 << 40} {
		got, err := parseBytes(in)
		if err != nil || got != want {
			t.Errorf("parseBytes(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "x", "10X", "-1", "1.5G", "GiB"} {
		if _, err := parseBytes(in); err == nil {
			t.Errorf("parseBytes(%q) accepted", in)
		}
	}
}

func TestParseAge(t *testing.T) {
	for in, want := range map[string]time.Duration{"36h": 36 * time.Hour, "30d": 30 * 24 * time.Hour, "1s": time.Second, "90m": 90 * time.Minute} {
		got, err := parseAge(in)
		if err != nil || got != want {
			t.Errorf("parseAge(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "30", "d", "1.5d", "-1d", "0", "500ms", "week"} {
		if got, err := parseAge(in); err == nil {
			t.Errorf("parseAge(%q) = %v, want an error", in, got)
		}
	}
}

func TestStreamAgesFromEnv(t *testing.T) {
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NATS.StreamMaxAge != 7*24*time.Hour || cfg.NATS.StreamCopyMaxAge != 0 || cfg.Dispatch.SkipUserEvents != nil {
		t.Errorf("defaults = %v, %v, %v; want 7 days, copies as long as the stream, every user copy",
			cfg.NATS.StreamMaxAge, cfg.NATS.StreamCopyMaxAge, cfg.Dispatch.SkipUserEvents)
	}

	t.Setenv("NATS_STREAM_MAX_AGE", "30d")
	t.Setenv("NATS_STREAM_COPY_MAX_AGE", "168h")
	t.Setenv("DISPATCH_USER_SKIP_EVENTS", "check_run, workflow_job")
	cfg, err = LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NATS.StreamMaxAge != 30*24*time.Hour || cfg.NATS.StreamCopyMaxAge != 7*24*time.Hour {
		t.Errorf("ages = %v, %v", cfg.NATS.StreamMaxAge, cfg.NATS.StreamCopyMaxAge)
	}
	if got := strings.Join(cfg.Dispatch.SkipUserEvents, " "); got != "check_run workflow_job" {
		t.Errorf("skip = %q", got)
	}

	t.Setenv("NATS_STREAM_MAX_AGE", "soon")
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "NATS_STREAM_MAX_AGE") {
		t.Errorf("err = %v, want one that names NATS_STREAM_MAX_AGE", err)
	}
}
