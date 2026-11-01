package config

import (
	"testing"
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
