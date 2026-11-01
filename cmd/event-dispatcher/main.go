package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/dispatch"
	natsInternal "github.com/zimbatm/octoflow/internal/nats"
)

const consumerName = "event-dispatcher"

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	nc, err := natsInternal.Connect(&cfg.NATS)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}
	if err := natsInternal.EnsureStream(js, &cfg.NATS); err != nil {
		log.Fatalf("Failed to initialize NATS stream: %v", err)
	}
	publisher := natsInternal.NewPublisher(js, &cfg.NATS)

	if err := natsInternal.EnsureConsumer(js, cfg.NATS.StreamName, &nats.ConsumerConfig{
		Durable:        consumerName,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubjects: natsInternal.PrimarySubjects(cfg.NATS.SubjectPrefix),
		MaxAckPending:  1000,
	}); err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < cfg.Processing.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			subscribeAndDispatch(ctx, js, cfg, publisher, workerID)
		}(i)
	}

	<-ctx.Done()
	slog.Info("Starting graceful shutdown")
	wg.Wait()
	slog.Info("Shutdown complete")
}

func subscribeAndDispatch(ctx context.Context, js nats.JetStreamContext, cfg *config.Config,
	publisher *natsInternal.Publisher, workerID int) {

	sub, err := js.PullSubscribe("", consumerName, nats.Bind(cfg.NATS.StreamName, consumerName))
	if err != nil {
		slog.Error("Failed to create subscription", "worker_id", workerID, "error", err)
		return
	}
	slog.Info("Worker started", "worker_id", workerID)

	for ctx.Err() == nil {
		msgs, err := sub.Fetch(10, nats.MaxWait(time.Second))
		if err != nil && !errors.Is(err, nats.ErrTimeout) {
			if ctx.Err() != nil {
				break
			}
			slog.Error("Error fetching messages", "worker_id", workerID, "error", err)
			// fetch fails at once while NATS reconnects; do not spin.
			time.Sleep(time.Second)
			continue
		}

		for _, msg := range msgs {
			_, err := dispatch.Dispatch(ctx, publisher, cfg.NATS.SubjectPrefix, msg.Data)
			switch {
			case errors.Is(err, dispatch.ErrUndecodable):
				slog.Error("Dropping undecodable message", "subject", msg.Subject, "error", err)
				if err := msg.Term(); err != nil {
					slog.Warn("Failed to terminate message", "subject", msg.Subject, "error", err)
				}
			case err != nil:
				slog.Error("Error dispatching message", "subject", msg.Subject, "error", err)
				natsInternal.AckOrNak(msg, err)
			default:
				natsInternal.AckOrNak(msg, nil)
			}
		}
	}
	slog.Info("Worker shutting down", "worker_id", workerID)
}
