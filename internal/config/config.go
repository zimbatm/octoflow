package config

import (
	"errors"
	"fmt"
	"os"
	"slices"
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
	// oldest messages go first once the stream reaches it; 0 is unlimited
	StreamMaxBytes int64

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

	// a batch closes on BatchSize events, BatchMaxBytes of payload or
	// BatchTimeout after its first event, whichever comes first
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
	// open streams beyond it get a 503; each one holds a JetStream consumer
	MaxClients int
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

// LoadFromEnv reads the environment. It does not check the S3 settings;
// only the s3 service needs them, see S3Config.Validate.
func LoadFromEnv() (*Config, error) {
	replicas, err := envParse("NATS_STREAM_REPLICAS", 1, strconv.Atoi)
	if err != nil {
		return nil, err
	}
	maxBytes, err := envParse("NATS_STREAM_MAX_BYTES", int64(0), parseBytes)
	if err != nil {
		return nil, err
	}
	pollInterval, err := envParse("EVENTS_API_INTERVAL", time.Minute, time.ParseDuration)
	if err != nil {
		return nil, err
	}
	archiveStart, err := envParse("GHARCHIVE_START", time.Time{}, func(s string) (time.Time, error) {
		return time.Parse(time.RFC3339, s)
	})
	if err != nil {
		return nil, err
	}
	logFormat, err := envParse("LOG_FORMAT", "text", oneOf("text", "json"))
	if err != nil {
		return nil, err
	}
	logLevel, err := envParse("LOG_LEVEL", "info", oneOf("debug", "info", "warn", "error"))
	if err != nil {
		return nil, err
	}
	batchSize, err := envParse("S3_BATCH_SIZE", 1000, strconv.Atoi)
	if err != nil {
		return nil, err
	}
	batchTimeout, err := envParse("S3_BATCH_TIMEOUT", time.Hour, time.ParseDuration)
	if err != nil {
		return nil, err
	}
	batchMaxBytes, err := envParse("S3_BATCH_MAX_BYTES", int64(100<<20), parseBytes)
	if err != nil {
		return nil, err
	}
	maxClients, err := envParse("SSE_MAX_CLIENTS", 1000, strconv.Atoi)
	if err != nil {
		return nil, err
	}
	// the webhook, SSE and dashboard routes share one server unless a
	// service is given its own address.
	httpAddr := getEnvOrDefault("HTTP_LISTEN_ADDR", ":8080")
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Webhook: WebhookConfig{
			ListenAddr:     getEnvOrDefault("WEBHOOK_LISTEN_ADDR", httpAddr),
			Secret:         os.Getenv("WEBHOOK_SECRET"),
			AllowUnsigned:  envBool("WEBHOOK_ALLOW_UNSIGNED", false),
			MaxPayloadSize: 25 << 20,
			RequestTimeout: 10 * time.Second,
			AllowedOrgs:    splitList(os.Getenv("WEBHOOK_ALLOWED_ORGS")),
		},
		NATS: NATSConfig{
			Servers:           []string{getEnvOrDefault("NATS_URL", "nats://localhost:4222")},
			SubjectPrefix:     getEnvOrDefault("NATS_SUBJECT_PREFIX", "github.events"),
			StreamName:        getEnvOrDefault("NATS_STREAM_NAME", "GITHUB_EVENTS_RAW"),
			StreamReplicas:    replicas,
			StreamMaxAge:      7 * 24 * time.Hour,
			StreamMaxBytes:    maxBytes,
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
			BatchSize:       batchSize,
			BatchTimeout:    batchTimeout,
			BatchMaxBytes:   batchMaxBytes,
			CompressionType: "snappy",
			RowGroupSize:    10000,
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			Endpoint:        os.Getenv("S3_ENDPOINT"),
		},
		SSE: SSEConfig{
			ListenAddr: getEnvOrDefault("SSE_LISTEN_ADDR", httpAddr),
			MaxClients: maxClients,
		},
		Dashboard: DashboardConfig{
			ListenAddr: getEnvOrDefault("DASHBOARD_LISTEN_ADDR", httpAddr),
			WebDir:     getEnvOrDefault("WEB_DIR", "./web"),
		},
		PublicIngest: PublicIngestConfig{
			Repos:             splitList(os.Getenv("PUBLIC_REPOS")),
			GitHubToken:       os.Getenv("GITHUB_TOKEN"),
			EventsAPI:         envBool("EVENTS_API_ENABLED", true),
			EventsAPIInterval: pollInterval,
			GHArchive:         envBool("GHARCHIVE_ENABLED", true),
			GHArchiveURL:      getEnvOrDefault("GHARCHIVE_URL", "https://data.gharchive.org"),
			GHArchiveStart:    archiveStart,
		},
		Log:     LogConfig{Format: logFormat, Level: logLevel},
		Metrics: MetricsConfig{ListenAddr: os.Getenv("METRICS_LISTEN_ADDR")},
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

// envParse returns def when key is unset, else parse(value); a parse
// error names the variable.
func envParse[T any](key string, def T, parse func(string) (T, error)) (T, error) {
	s := os.Getenv(key)
	if s == "" {
		return def, nil
	}
	v, err := parse(s)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return v, nil
}

func oneOf(allowed ...string) func(string) (string, error) {
	return func(s string) (string, error) {
		if slices.Contains(allowed, s) {
			return s, nil
		}
		return "", fmt.Errorf("must be one of %s, got %q", strings.Join(allowed, ", "), s)
	}
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

// Validate reports missing settings. Only the s3 service needs S3, so LoadFromEnv does not call it.
func (c *S3Config) Validate() error {
	if c.Bucket == "" {
		return errors.New("S3_BUCKET environment variable is required")
	}
	return nil
}

// parseBytes reads a byte count: a bare number, or one with a binary
// suffix K, M, G or T, optionally followed by B or iB, as in 10GiB.
func parseBytes(s string) (int64, error) {
	num := strings.TrimRight(s, "KMGTBi")
	suffix := strings.TrimSuffix(strings.TrimSuffix(s[len(num):], "B"), "i")
	shift := map[string]uint{"": 0, "K": 10, "M": 20, "G": 30, "T": 40}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not a size: %q", s)
	}
	sh, ok := shift[suffix]
	if !ok || n < 0 {
		return 0, fmt.Errorf("not a size: %q", s)
	}
	return n << sh, nil
}
