// octoflow runs one service per invocation, or all of them in one process.
//
//	octoflow webhook|dispatcher|s3|sse|dashboard|public-ingest|all|version
//	octoflow restore --from YYYY-MM-DD ...
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/metrics"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

// numWorkers is how many goroutines pull from a durable consumer.
const numWorkers = 10

type service struct {
	name string
	// nats is false for services that never talk to the stream.
	nats bool
	// enabled says whether `all` runs the service with this configuration;
	// nil means always.
	enabled func(*config.Config) bool
	run     func(ctx context.Context, cfg *config.Config, js nats.JetStreamContext) error
}

var services = []service{
	{"webhook", true, nil, runWebhook},
	{"dispatcher", true, nil, runDispatcher},
	{"s3", true, s3Enabled, runS3},
	{"sse", true, nil, runSSE},
	{"dashboard", false, nil, runDashboard},
	{"public-ingest", true, publicIngestEnabled, runPublicIngest},
}

func s3Enabled(cfg *config.Config) bool           { return cfg.S3.Bucket != "" }
func publicIngestEnabled(cfg *config.Config) bool { return len(cfg.PublicIngest.Repos) > 0 }

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version", "--version":
		fmt.Println("octoflow", version)
		return
	case "restore":
		os.Exit(runRestore(os.Args[2:]))
	}
	if len(os.Args) != 2 {
		usage()
		os.Exit(2)
	}
	cfg, err := config.LoadFromEnv()
	if err != nil {
		fatal("Failed to load configuration", err)
	}
	setupLogging(cfg.Log)
	selected, err := pick(os.Args[1], cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		usage()
		os.Exit(2)
	}
	slog.Info("octoflow starting", "version", version, "services", names(selected))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var js nats.JetStreamContext
	if needsNATS(selected) {
		nc, j, err := natsInternal.ConnectJetStream(&cfg.NATS)
		if err != nil {
			fatal("Failed to connect to NATS", err)
		}
		if nc.MaxPayload() < cfg.Webhook.MaxPayloadSize && slices.ContainsFunc(selected, func(s service) bool { return s.name == "webhook" }) {
			slog.Warn("NATS max_payload is below the webhook payload limit; large deliveries will fail",
				"nats_max_payload", nc.MaxPayload(), "webhook_max_payload", cfg.Webhook.MaxPayloadSize)
		}
		// closed after every service returned: the s3 service acks in its final flush.
		defer nc.Close()
		js = j
	}

	var wg sync.WaitGroup
	var failed atomic.Bool
	start := func(name string, run func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := run(); err != nil {
				slog.Error("Service failed", "service", name, "error", err)
				failed.Store(true)
				stop()
			}
			slog.Info("Service stopped", "service", name)
		}()
	}
	for _, s := range selected {
		slog.Info("Starting service", "service", s.name)
		start(s.name, func() error { return s.run(ctx, cfg, js) })
	}
	if cfg.Metrics.ListenAddr != "" {
		start("metrics", func() error { return serveMetrics(ctx, cfg.Metrics.ListenAddr) })
		if js != nil {
			start("stream-stats", func() error { watchStream(ctx, js, cfg.NATS.StreamName); return nil })
		}
	}

	<-ctx.Done()
	slog.Info("Shutting down")
	wg.Wait()
	if failed.Load() {
		os.Exit(1)
	}
	slog.Info("Shutdown complete")
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: octoflow webhook|dispatcher|s3|sse|dashboard|public-ingest|all")
	fmt.Fprintln(os.Stderr, "       octoflow restore --from YYYY-MM-DD [--to YYYY-MM-DD] [--event a,b] [--repo ID] [--org ID] [--dry-run] [--force]")
	fmt.Fprintln(os.Stderr, "       octoflow version")
	fmt.Fprintln(os.Stderr, "  all runs every service in this process; s3 only with S3_BUCKET, public-ingest only with PUBLIC_REPOS")
	fmt.Fprintln(os.Stderr, "  restore publishes archived events from S3 back into the stream")
}

func fatal(msg string, err error) {
	slog.Error(msg, "error", err)
	os.Exit(1)
}

func setupLogging(cfg config.LogConfig) {
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.Level))
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if cfg.Format == "json" {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h))
}

// pick resolves a command line argument to the services to run.
func pick(name string, cfg *config.Config) ([]service, error) {
	if name == "all" {
		var out []service
		for _, s := range services {
			if s.enabled == nil || s.enabled(cfg) {
				out = append(out, s)
			}
		}
		return out, nil
	}
	for _, s := range services {
		if s.name == name {
			return []service{s}, nil
		}
	}
	return nil, fmt.Errorf("unknown service %q", name)
}

func names(selected []service) []string {
	out := make([]string, 0, len(selected))
	for _, s := range selected {
		out = append(out, s.name)
	}
	return out
}

func needsNATS(selected []service) bool {
	for _, s := range selected {
		if s.nats {
			return true
		}
	}
	return false
}

// serveMetrics exposes Prometheus metrics and a liveness check for the
// process, whatever services it runs.
func serveMetrics(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	slog.Info("Metrics listening", "address", addr)
	return serveHTTP(ctx, &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second})
}

// watchStream refreshes the stream gauges every 30 seconds.
func watchStream(ctx context.Context, js nats.JetStreamContext, stream string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		if info, err := js.StreamInfo(stream); err == nil {
			metrics.StreamMessages.Set(float64(info.State.Msgs))
			metrics.StreamBytes.Set(float64(info.State.Bytes))
			metrics.StreamConsumers.Set(float64(info.State.Consumers))
		} else if ctx.Err() == nil {
			slog.Warn("Failed to read stream info", "stream", stream, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// serveHTTP runs srv until ctx ends, then gives in-flight requests 30 seconds.
func serveHTTP(ctx context.Context, srv *http.Server) error {
	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

// pullLoop fetches from a durable pull consumer with numWorkers goroutines
// and hands each message to handle, until ctx ends and the workers return.
func pullLoop(ctx context.Context, cfg *config.Config, js nats.JetStreamContext, consumer string, handle func(*nats.Msg)) {
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			sub, err := js.PullSubscribe("", consumer, nats.Bind(cfg.NATS.StreamName, consumer))
			if err != nil {
				slog.Error("Failed to create subscription", "consumer", consumer, "worker_id", workerID, "error", err)
				return
			}
			for ctx.Err() == nil {
				msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
				if err != nil && !errors.Is(err, nats.ErrTimeout) {
					if ctx.Err() != nil {
						break
					}
					slog.Error("Error fetching messages", "consumer", consumer, "worker_id", workerID, "error", err)
					// fetch fails at once while NATS reconnects; do not spin.
					time.Sleep(time.Second)
					continue
				}
				for _, msg := range msgs {
					handle(msg)
				}
			}
		}(i)
	}
	wg.Wait()
}

func terminate(msg *nats.Msg, why string, err error) {
	slog.Error(why, "subject", msg.Subject, "error", err)
	if err := msg.Term(); err != nil {
		slog.Warn("Failed to terminate message", "subject", msg.Subject, "error", err)
	}
}
