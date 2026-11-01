package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the settings of all services. Every value comes from the
// environment; there is no configuration file.
type Config struct {
	Webhook    WebhookConfig
	NATS       NATSConfig
	S3         S3Config
	SSE        SSEConfig
	Processing ProcessingConfig
}

type WebhookConfig struct {
	ListenAddr string
	// empty disables signature validation
	Secret         string
	MaxPayloadSize int64
	RequestTimeout time.Duration
	// empty allows every organization
	AllowedOrgs []string
	HealthPath  string
	MetricsPath string
}

type NATSConfig struct {
	Servers       []string
	SubjectPrefix string
	JetStream     bool

	StreamName     string
	StreamReplicas int
	StreamMaxAge   time.Duration

	MaxReconnects     int
	ReconnectWait     time.Duration
	ConnectionTimeout time.Duration

	Username string
	Password string
	Token    string
	NKey     string
}

type S3Config struct {
	Region    string
	Bucket    string
	KeyPrefix string

	BatchSize     int
	BatchTimeout  time.Duration
	BatchMaxBytes int64

	CompressionType string
	RowGroupSize    int

	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// empty means AWS; set for R2, rustfs and other S3-compatible stores
	Endpoint string
}

type SSEConfig struct {
	ListenAddr string
}

type ProcessingConfig struct {
	// pull workers per durable consumer in the dispatcher and the s3-worker
	NumWorkers int
}

// LoadFromEnv reads the environment. It does not check the S3 settings;
// only the s3-worker needs them, see S3Config.Validate.
func LoadFromEnv() (*Config, error) {
	cfg := &Config{
		Webhook: WebhookConfig{
			ListenAddr:     getEnvOrDefault("WEBHOOK_LISTEN_ADDR", ":8080"),
			Secret:         os.Getenv("WEBHOOK_SECRET"),
			MaxPayloadSize: 25 << 20,
			RequestTimeout: 10 * time.Second,
			HealthPath:     "/health",
			MetricsPath:    "/metrics",
		},
		NATS: NATSConfig{
			Servers:           []string{getEnvOrDefault("NATS_URL", "nats://localhost:4222")},
			SubjectPrefix:     getEnvOrDefault("NATS_SUBJECT_PREFIX", "github.events"),
			JetStream:         true,
			StreamName:        getEnvOrDefault("NATS_STREAM_NAME", "GITHUB_EVENTS_RAW"),
			StreamReplicas:    1,
			StreamMaxAge:      7 * 24 * time.Hour,
			MaxReconnects:     10,
			ReconnectWait:     2 * time.Second,
			ConnectionTimeout: 5 * time.Second,
		},
		S3: S3Config{
			Region:          getEnvOrDefault("AWS_REGION", "us-east-1"),
			Bucket:          os.Getenv("S3_BUCKET"),
			KeyPrefix:       getEnvOrDefault("S3_KEY_PREFIX", "github-events"),
			BatchSize:       1000,
			BatchTimeout:    5 * time.Minute,
			BatchMaxBytes:   100 << 20,
			CompressionType: "snappy",
			RowGroupSize:    10000,
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			Endpoint:        os.Getenv("S3_ENDPOINT"),
		},
		SSE: SSEConfig{
			ListenAddr: getEnvOrDefault("SSE_LISTEN_ADDR", ":8081"),
		},
		Processing: ProcessingConfig{
			NumWorkers: 10,
		},
	}

	if s := os.Getenv("NATS_STREAM_REPLICAS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("NATS_STREAM_REPLICAS: %w", err)
		}
		cfg.NATS.StreamReplicas = n
	}

	return cfg, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Validate reports missing settings. Only the s3-worker needs S3, so LoadFromEnv does not call it.
func (c *S3Config) Validate() error {
	if c.Bucket == "" {
		return fmt.Errorf("S3_BUCKET environment variable is required")
	}
	return nil
}
