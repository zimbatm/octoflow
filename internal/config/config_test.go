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

	if cfg.NATS.StreamName != "GITHUB_EVENTS" {
		t.Errorf("Expected stream name GITHUB_EVENTS, got %s", cfg.NATS.StreamName)
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

func TestLoadFromEnvMissingRequired(t *testing.T) {
	// Save and clear env vars
	oldBucket := os.Getenv("S3_BUCKET")
	oldRegion := os.Getenv("S3_REGION")
	oldAccessKey := os.Getenv("S3_ACCESS_KEY_ID")
	oldSecretKey := os.Getenv("S3_SECRET_ACCESS_KEY")

	os.Unsetenv("S3_BUCKET")
	os.Unsetenv("S3_REGION")
	os.Unsetenv("S3_ACCESS_KEY_ID")
	os.Unsetenv("S3_SECRET_ACCESS_KEY")

	defer func() {
		if oldBucket != "" {
			os.Setenv("S3_BUCKET", oldBucket)
		}
		if oldRegion != "" {
			os.Setenv("S3_REGION", oldRegion)
		}
		if oldAccessKey != "" {
			os.Setenv("S3_ACCESS_KEY_ID", oldAccessKey)
		}
		if oldSecretKey != "" {
			os.Setenv("S3_SECRET_ACCESS_KEY", oldSecretKey)
		}
	}()

	_, err := LoadFromEnv()
	if err == nil {
		t.Error("Expected error when S3 config is missing")
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
