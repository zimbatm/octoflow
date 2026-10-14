package config

import (
	"os"
	"testing"
)

func TestLoadFromEnv(t *testing.T) {
	// Save current env vars
	oldVars := make(map[string]string)
	envVars := []string{
		"WEBHOOK_LISTEN_ADDR",
		"WEBHOOK_SECRET",
		"NATS_URL",
		"NATS_STREAM_NAME",
		"S3_BUCKET",
		"S3_REGION",
		"S3_ACCESS_KEY_ID",
		"S3_SECRET_ACCESS_KEY",
		"SSE_PORT",
	}

	for _, v := range envVars {
		oldVars[v] = os.Getenv(v)
		os.Unsetenv(v)
	}

	// Restore env vars after test
	defer func() {
		for k, v := range oldVars {
			if v != "" {
				os.Setenv(k, v)
			}
		}
	}()

	// Test with minimal required env vars
	os.Setenv("S3_BUCKET", "test-bucket")
	os.Setenv("S3_REGION", "us-east-1")
	os.Setenv("S3_ACCESS_KEY_ID", "test-key")
	os.Setenv("S3_SECRET_ACCESS_KEY", "test-secret")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() failed: %v", err)
	}

	// Check defaults
	if cfg.Webhook.ListenAddr != ":8080" {
		t.Errorf("Expected webhook listen addr :8080, got %s", cfg.Webhook.ListenAddr)
	}

	if cfg.NATS.StreamName != "GITHUB_EVENTS_RAW" {
		t.Errorf("Expected stream name GITHUB_EVENTS_RAW, got %s", cfg.NATS.StreamName)
	}

	if cfg.SSE.ListenAddr != ":8081" {
		t.Errorf("Expected SSE listen addr :8081, got %s", cfg.SSE.ListenAddr)
	}

	// Test with custom values
	os.Setenv("WEBHOOK_LISTEN_ADDR", ":9090")
	os.Setenv("WEBHOOK_SECRET", "my-secret")
	os.Setenv("NATS_URL", "nats://custom:4222")

	cfg, err = LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() with custom values failed: %v", err)
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
}

func TestLoadFromEnvWithoutS3(t *testing.T) {
	t.Setenv("S3_BUCKET", "")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() failed without S3_BUCKET: %v", err)
	}
	if err := cfg.S3.Validate(); err == nil {
		t.Error("Expected S3 validation error when S3_BUCKET is missing")
	}
}

// Helper function to compare string slices
func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
