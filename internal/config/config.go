package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the settings of all services. Every value comes from the
// environment; there is no configuration file.
type Config struct {
	Webhook      WebhookConfig
	NATS         NATSConfig
	S3           S3Config
	SSE          SSEConfig
	Dashboard    DashboardConfig
	PublicIngest PublicIngestConfig
	Processing   ProcessingConfig
	Log          LogConfig
	Metrics      MetricsConfig
}

type WebhookConfig struct {
	ListenAddr string
	// empty disables signature validation; the receiver refuses to start
	// that way unless AllowUnsigned is set
	Secret         string
	AllowUnsigned  bool
	MaxPayloadSize int64
	RequestTimeout time.Duration
	// empty allows every organization
	AllowedOrgs []string
	HealthPath  string
}

type LogConfig struct {
	// text or json
	Format string
	// debug, info, warn or error
	Level string
}

type MetricsConfig struct {
	// address of the Prometheus listener; empty disables it
	ListenAddr string
}

type NATSConfig struct {
	Servers       []string
	SubjectPrefix string

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

type DashboardConfig struct {
	ListenAddr string
	// directory with index.html
	WebDir string
}

type PublicIngestConfig struct {
	// repositories without a webhook, as owner/name; empty disables the service
	Repos       []string
	GitHubToken string

	EventsAPI         bool
	EventsAPIInterval time.Duration

	GHArchive    bool
	GHArchiveURL string
	// first hour to import when no progress is saved; zero means three hours ago
	GHArchiveStart time.Time
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
			AllowUnsigned:  envBool("WEBHOOK_ALLOW_UNSIGNED", false),
			MaxPayloadSize: 25 << 20,
			RequestTimeout: 10 * time.Second,
			AllowedOrgs:    splitList(os.Getenv("WEBHOOK_ALLOWED_ORGS")),
			HealthPath:     "/health",
		},
		NATS: NATSConfig{
			Servers:           []string{getEnvOrDefault("NATS_URL", "nats://localhost:4222")},
			SubjectPrefix:     getEnvOrDefault("NATS_SUBJECT_PREFIX", "github.events"),
			StreamName:        getEnvOrDefault("NATS_STREAM_NAME", "GITHUB_EVENTS_RAW"),
			StreamReplicas:    1,
			StreamMaxAge:      7 * 24 * time.Hour,
			MaxReconnects:     10,
			ReconnectWait:     2 * time.Second,
			ConnectionTimeout: 5 * time.Second,
			Username:          os.Getenv("NATS_USERNAME"),
			Password:          os.Getenv("NATS_PASSWORD"),
			Token:             os.Getenv("NATS_TOKEN"),
			NKey:              os.Getenv("NATS_NKEY_SEED"),
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
		Dashboard: DashboardConfig{
			ListenAddr: getEnvOrDefault("DASHBOARD_LISTEN_ADDR", ":8082"),
			WebDir:     getEnvOrDefault("WEB_DIR", "./web"),
		},
		PublicIngest: PublicIngestConfig{
			Repos:             splitList(os.Getenv("PUBLIC_REPOS")),
			GitHubToken:       os.Getenv("GITHUB_TOKEN"),
			EventsAPI:         envBool("EVENTS_API_ENABLED", true),
			EventsAPIInterval: time.Minute,
			GHArchive:         envBool("GHARCHIVE_ENABLED", true),
			GHArchiveURL:      getEnvOrDefault("GHARCHIVE_URL", "https://data.gharchive.org"),
		},
		Processing: ProcessingConfig{
			NumWorkers: 10,
		},
		Log: LogConfig{
			Format: getEnvOrDefault("LOG_FORMAT", "text"),
			Level:  getEnvOrDefault("LOG_LEVEL", "info"),
		},
		Metrics: MetricsConfig{
			ListenAddr: os.Getenv("METRICS_LISTEN_ADDR"),
		},
	}

	switch cfg.Log.Format {
	case "text", "json":
	default:
		return nil, fmt.Errorf("LOG_FORMAT must be text or json, got %q", cfg.Log.Format)
	}
	switch cfg.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return nil, fmt.Errorf("LOG_LEVEL must be debug, info, warn or error, got %q", cfg.Log.Level)
	}

	if s := os.Getenv("NATS_STREAM_REPLICAS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("NATS_STREAM_REPLICAS: %w", err)
		}
		cfg.NATS.StreamReplicas = n
	}
	if s := os.Getenv("EVENTS_API_INTERVAL"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("EVENTS_API_INTERVAL: %w", err)
		}
		cfg.PublicIngest.EventsAPIInterval = d
	}
	if s := os.Getenv("GHARCHIVE_START"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, fmt.Errorf("GHARCHIVE_START: %w", err)
		}
		cfg.PublicIngest.GHArchiveStart = t
	}

	return cfg, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func envBool(key string, def bool) bool {
	b, err := strconv.ParseBool(os.Getenv(key))
	if err != nil {
		return def
	}
	return b
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Validate reports missing settings. Only the s3-worker needs S3, so LoadFromEnv does not call it.
func (c *S3Config) Validate() error {
	if c.Bucket == "" {
		return fmt.Errorf("S3_BUCKET environment variable is required")
	}
	return nil
}
