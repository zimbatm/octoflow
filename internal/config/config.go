package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/zimbatm/octoflow/pkg/events"
)

// Config holds all configuration for the octoflow system
type Config struct {
	// Webhook receiver configuration
	Webhook WebhookConfig `json:"webhook"`

	// NATS configuration
	NATS NATSConfig `json:"nats"`

	// S3 storage configuration
	S3 S3Config `json:"s3"`

	// SSE server configuration
	SSE SSEConfig `json:"sse"`

	// Event processing configuration
	Processing ProcessingConfig `json:"processing"`

	// Monitoring configuration
	Monitoring MonitoringConfig `json:"monitoring"`
}

// WebhookConfig holds webhook receiver configuration
type WebhookConfig struct {
	// Server address to listen on
	ListenAddr string `json:"listen_addr" default:":8080"`

	// GitHub webhook secret for signature validation
	Secret string `json:"secret"`

	// Maximum payload size in bytes (GitHub max is 25MB)
	MaxPayloadSize int64 `json:"max_payload_size" default:"26214400"` // 25MB

	// Request timeout
	RequestTimeout time.Duration `json:"request_timeout" default:"10s"`

	// Allowed GitHub organizations (empty = all allowed)
	AllowedOrgs []string `json:"allowed_orgs,omitempty"`

	// Health check path
	HealthPath string `json:"health_path" default:"/health"`

	// Metrics path
	MetricsPath string `json:"metrics_path" default:"/metrics"`
}

// NATSConfig holds NATS connection configuration
type NATSConfig struct {
	// NATS server URLs
	Servers []string `json:"servers" default:"[\"nats://localhost:4222\"]"`

	// Subject prefix for events
	SubjectPrefix string `json:"subject_prefix" default:"github.events"`

	// Enable JetStream for persistence
	JetStream bool `json:"jetstream" default:"true"`

	// JetStream configuration
	StreamName     string        `json:"stream_name" default:"GITHUB_EVENTS_RAW"`
	StreamReplicas int           `json:"stream_replicas" default:"3"`
	StreamMaxAge   time.Duration `json:"stream_max_age" default:"168h"` // 7 days

	// Connection settings
	MaxReconnects     int           `json:"max_reconnects" default:"10"`
	ReconnectWait     time.Duration `json:"reconnect_wait" default:"2s"`
	ConnectionTimeout time.Duration `json:"connection_timeout" default:"5s"`

	// Authentication
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Token    string `json:"token,omitempty"`
	NKey     string `json:"nkey,omitempty"`
}

// S3Config holds S3 storage configuration
type S3Config struct {
	// AWS region
	Region string `json:"region" default:"us-east-1"`

	// S3 bucket name
	Bucket string `json:"bucket"`

	// Key prefix for stored files
	KeyPrefix string `json:"key_prefix" default:"github-events"`

	// Batch settings
	BatchSize     int           `json:"batch_size" default:"1000"`
	BatchTimeout  time.Duration `json:"batch_timeout" default:"5m"`
	BatchMaxBytes int64         `json:"batch_max_bytes" default:"104857600"` // 100MB

	// Parquet settings
	CompressionType string `json:"compression_type" default:"snappy"`
	RowGroupSize    int    `json:"row_group_size" default:"10000"`

	// Retention settings
	// RetentionDays - removed, manage lifecycle at bucket level

	// AWS credentials (uses default credential chain if empty)
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`

	// S3 endpoint (for S3-compatible services)
	Endpoint string `json:"endpoint,omitempty"`
}

// SSEConfig holds SSE server configuration
type SSEConfig struct {
	// Server address to listen on
	ListenAddr string `json:"listen_addr" default:":8081"`
}

// ProcessingConfig holds event processing configuration
type ProcessingConfig struct {
	// Deduplication settings
	DeduplicationTTL time.Duration `json:"deduplication_ttl" default:"24h"`

	// Processing workers
	NumWorkers int `json:"num_workers" default:"10"`

	// Event filters for different components
	NATSFilters map[string]*events.EventFilter `json:"nats_filters,omitempty"`
	S3Filter    *events.EventFilter            `json:"s3_filter,omitempty"`
	SSEFilter   *events.EventFilter            `json:"sse_filter,omitempty"`
}

// MonitoringConfig holds monitoring configuration
type MonitoringConfig struct {
	// Enable metrics collection
	Enabled bool `json:"enabled" default:"true"`

	// Metrics prefix
	MetricsPrefix string `json:"metrics_prefix" default:"octoflow"`

	// Log level
	LogLevel string `json:"log_level" default:"info"`

	// Log format (json or text)
	LogFormat string `json:"log_format" default:"json"`
}

// LoadFromEnv loads configuration from environment variables
func LoadFromEnv() (*Config, error) {
	cfg := &Config{}

	// Webhook configuration
	cfg.Webhook.ListenAddr = getEnvOrDefault("WEBHOOK_LISTEN_ADDR", ":8080")
	cfg.Webhook.Secret = os.Getenv("WEBHOOK_SECRET")
	// Secret is optional - if not set, signature validation will be skipped

	// NATS configuration
	cfg.NATS.Servers = []string{getEnvOrDefault("NATS_URL", "nats://localhost:4222")}
	cfg.NATS.SubjectPrefix = getEnvOrDefault("NATS_SUBJECT_PREFIX", "github.events")
	cfg.NATS.StreamName = getEnvOrDefault("NATS_STREAM_NAME", "GITHUB_EVENTS_RAW")

	// Parse stream replicas from env
	if replicasStr := os.Getenv("NATS_STREAM_REPLICAS"); replicasStr != "" {
		if replicas, err := strconv.Atoi(replicasStr); err == nil {
			cfg.NATS.StreamReplicas = replicas
		}
	}

	// S3 configuration
	cfg.S3.Region = getEnvOrDefault("AWS_REGION", "us-east-1")
	cfg.S3.Bucket = os.Getenv("S3_BUCKET")
	cfg.S3.KeyPrefix = getEnvOrDefault("S3_KEY_PREFIX", "github-events")
	cfg.S3.Endpoint = os.Getenv("S3_ENDPOINT")
	cfg.S3.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	cfg.S3.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")

	// SSE configuration
	cfg.SSE.ListenAddr = getEnvOrDefault("SSE_LISTEN_ADDR", ":8081")

	// Set defaults for other fields
	setDefaults(cfg)

	return cfg, nil
}

// getEnvOrDefault returns the environment variable value or a default
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// setDefaults sets default values for configuration fields
func setDefaults(cfg *Config) {
	// Webhook defaults
	if cfg.Webhook.MaxPayloadSize == 0 {
		cfg.Webhook.MaxPayloadSize = 26214400 // 25MB
	}
	if cfg.Webhook.RequestTimeout == 0 {
		cfg.Webhook.RequestTimeout = 10 * time.Second
	}
	if cfg.Webhook.HealthPath == "" {
		cfg.Webhook.HealthPath = "/health"
	}
	if cfg.Webhook.MetricsPath == "" {
		cfg.Webhook.MetricsPath = "/metrics"
	}

	// NATS defaults
	if cfg.NATS.StreamName == "" {
		cfg.NATS.StreamName = "GITHUB_EVENTS_RAW"
	}
	if cfg.NATS.StreamReplicas == 0 {
		cfg.NATS.StreamReplicas = 1 // Single node for local development
	}
	if cfg.NATS.StreamMaxAge == 0 {
		cfg.NATS.StreamMaxAge = 168 * time.Hour // 7 days
	}
	if cfg.NATS.MaxReconnects == 0 {
		cfg.NATS.MaxReconnects = 10
	}
	if cfg.NATS.ReconnectWait == 0 {
		cfg.NATS.ReconnectWait = 2 * time.Second
	}
	if cfg.NATS.ConnectionTimeout == 0 {
		cfg.NATS.ConnectionTimeout = 5 * time.Second
	}
	cfg.NATS.JetStream = true

	// S3 defaults
	if cfg.S3.BatchSize == 0 {
		cfg.S3.BatchSize = 1000
	}
	if cfg.S3.BatchTimeout == 0 {
		cfg.S3.BatchTimeout = 5 * time.Minute
	}
	if cfg.S3.BatchMaxBytes == 0 {
		cfg.S3.BatchMaxBytes = 104857600 // 100MB
	}
	if cfg.S3.CompressionType == "" {
		cfg.S3.CompressionType = "snappy"
	}
	if cfg.S3.RowGroupSize == 0 {
		cfg.S3.RowGroupSize = 10000
	}

	// SSE defaults - only ListenAddr is needed now

	// Processing defaults
	if cfg.Processing.DeduplicationTTL == 0 {
		cfg.Processing.DeduplicationTTL = 24 * time.Hour
	}
	if cfg.Processing.NumWorkers == 0 {
		cfg.Processing.NumWorkers = 10
	}

	// Monitoring defaults
	cfg.Monitoring.Enabled = true
	if cfg.Monitoring.MetricsPrefix == "" {
		cfg.Monitoring.MetricsPrefix = "octoflow"
	}
	if cfg.Monitoring.LogLevel == "" {
		cfg.Monitoring.LogLevel = "info"
	}
	if cfg.Monitoring.LogFormat == "" {
		cfg.Monitoring.LogFormat = "json"
	}
}

// Validate reports missing settings. Only the s3-worker needs S3, so LoadFromEnv does not call it.
func (c *S3Config) Validate() error {
	if c.Bucket == "" {
		return fmt.Errorf("S3_BUCKET environment variable is required")
	}
	return nil
}
