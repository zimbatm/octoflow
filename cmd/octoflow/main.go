// octoflow runs one service per invocation, or all of them in one process.
//
//	octoflow webhook|dispatcher|s3|sse|dashboard|public-ingest|all
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
)

type service struct {
	name string
	// nats is false for services that never talk to the stream.
	nats bool
	run  func(ctx context.Context, cfg *config.Config, js nats.JetStreamContext) error
}

var services = []service{
	{"webhook", true, runWebhook},
	{"dispatcher", true, runDispatcher},
	{"s3", true, runS3},
	{"sse", true, runSSE},
	{"dashboard", false, runDashboard},
	{"public-ingest", true, runPublicIngest},
}

func main() {
	if len(os.Args) != 2 {
		usage()
		os.Exit(2)
	}
	cfg, err := config.LoadFromEnv()
	if err != nil {
		fatal("Failed to load configuration", err)
	}
	selected, err := pick(os.Args[1], cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var js nats.JetStreamContext
	if needsNATS(selected) {
		nc, err := natsInternal.Connect(&cfg.NATS)
		if err != nil {
			fatal("Failed to connect to NATS", err)
		}
		// closed after every service returned: the s3 service acks in its final flush.
		defer nc.Close()
		js, err = nc.JetStream()
		if err != nil {
			fatal("Failed to create JetStream context", err)
		}
		if err := natsInternal.EnsureStream(js, &cfg.NATS); err != nil {
			fatal("Failed to initialize NATS stream", err)
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []error
	for _, s := range selected {
		wg.Add(1)
		go func(s service) {
			defer wg.Done()
			slog.Info("Starting service", "service", s.name)
			if err := s.run(ctx, cfg, js); err != nil {
				slog.Error("Service failed", "service", s.name, "error", err)
				mu.Lock()
				failures = append(failures, fmt.Errorf("%s: %w", s.name, err))
				mu.Unlock()
				stop()
			}
			slog.Info("Service stopped", "service", s.name)
		}(s)
	}
	<-ctx.Done()
	slog.Info("Shutting down")
	wg.Wait()

	if len(failures) > 0 {
		os.Exit(1)
	}
	slog.Info("Shutdown complete")
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: octoflow webhook|dispatcher|s3|sse|dashboard|public-ingest|all")
	fmt.Fprintln(os.Stderr, "  all runs every service in this process; s3 only with S3_BUCKET, public-ingest only with PUBLIC_REPOS")
}

func fatal(msg string, err error) {
	slog.Error(msg, "error", err)
	os.Exit(1)
}

// pick resolves a command line argument to the services to run.
func pick(name string, cfg *config.Config) ([]service, error) {
	if name == "all" {
		var out []service
		for _, s := range services {
			switch s.name {
			case "s3":
				if cfg.S3.Bucket == "" {
					continue
				}
			case "public-ingest":
				if len(cfg.PublicIngest.Repos) == 0 {
					continue
				}
			}
			out = append(out, s)
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

func needsNATS(selected []service) bool {
	for _, s := range selected {
		if s.nats {
			return true
		}
	}
	return false
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

// pullLoop fetches from a durable pull consumer with NumWorkers goroutines
// and hands each message to handle, until ctx ends and the workers return.
func pullLoop(ctx context.Context, cfg *config.Config, js nats.JetStreamContext, consumer string, handle func(*nats.Msg)) {
	var wg sync.WaitGroup
	for i := 0; i < cfg.Processing.NumWorkers; i++ {
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
