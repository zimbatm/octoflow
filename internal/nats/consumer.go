package nats

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

// PrimarySubjects match the events as ingested. The dispatcher's fan-out
// copies (repo, user, pr, issue) share the stream, so a consumer that reads
// prefix.> sees each event several times.
func PrimarySubjects(prefix string) []string {
	return []string{prefix + ".org.>", prefix + ".global.>"}
}

// EnsureConsumer creates the durable consumer, or updates an existing one
// so that config changes such as new filter subjects take effect.
// It waits up to a minute for the stream to appear.
func EnsureConsumer(js nats.JetStreamContext, stream string, cc *nats.ConsumerConfig) error {
	for attempt := 0; attempt < 30; attempt++ {
		_, err := js.ConsumerInfo(stream, cc.Durable)
		switch {
		case err == nil:
			_, err = js.UpdateConsumer(stream, cc)
			return err
		case errors.Is(err, nats.ErrConsumerNotFound):
			_, err = js.AddConsumer(stream, cc)
			return err
		case errors.Is(err, nats.ErrStreamNotFound):
			slog.Info("Waiting for stream to be created", "stream", stream)
			time.Sleep(2 * time.Second)
		default:
			return err
		}
	}
	return fmt.Errorf("stream %s not found", stream)
}

// AckOrNak acks msg when err is nil and naks it otherwise. A failed ack
// only delays the message: JetStream redelivers it after AckWait.
func AckOrNak(msg *nats.Msg, err error) {
	var ackErr error
	if err != nil {
		ackErr = msg.Nak()
	} else {
		ackErr = msg.Ack()
	}
	if ackErr != nil {
		slog.Warn("Failed to acknowledge message", "subject", msg.Subject, "error", ackErr)
	}
}
