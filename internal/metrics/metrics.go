// Package metrics holds the Prometheus series of every service. Labels
// are bounded: event types, statuses, routes and sources, never ids.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	WebhookRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_webhook_requests_total",
		Help: "Webhook requests by event type, organization and outcome",
	}, []string{"event_type", "organization", "status"})

	WebhookRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "octoflow_webhook_request_duration_seconds",
		Help:    "Time from request start to reply",
		Buckets: prometheus.DefBuckets,
	}, []string{"event_type"})

	WebhookPayloadSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "octoflow_webhook_payload_size_bytes",
		Help:    "Size of webhook payloads",
		Buckets: prometheus.ExponentialBuckets(100, 10, 7),
	}, []string{"event_type"})

	NATSPublishedEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_nats_published_events_total",
		Help: "Publishes to JetStream by event type and outcome",
	}, []string{"stream", "event_type", "status"})

	NATSPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "octoflow_nats_publish_duration_seconds",
		Help:    "Time until JetStream acked a publish",
		Buckets: prometheus.DefBuckets,
	}, []string{"stream"})

	DispatchedEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_dispatched_events_total",
		Help: "Events the dispatcher handled, by outcome",
	}, []string{"status"})

	S3BatchesWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_s3_batches_written_total",
		Help: "Parquet batches uploaded, by outcome",
	}, []string{"status"})

	S3EventsPerBatch = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "octoflow_s3_events_per_batch",
		Help:    "Events per uploaded batch",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	})

	S3BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "octoflow_s3_batch_size_bytes",
		Help:    "Payload bytes per uploaded batch",
		Buckets: prometheus.ExponentialBuckets(1000, 10, 7),
	})

	S3WriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "octoflow_s3_write_duration_seconds",
		Help:    "Time to encode and upload a batch",
		Buckets: prometheus.DefBuckets,
	})

	SSEClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "octoflow_sse_clients",
		Help: "Open SSE connections",
	})

	SSEEventsSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_sse_events_sent_total",
		Help: "Events written to SSE clients, by route kind",
	}, []string{"route"})

	PublicIngestEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_public_ingest_events_total",
		Help: "Events published by public-ingest, by source",
	}, []string{"source"})

	PublicIngestPolls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_public_ingest_polls_total",
		Help: "Events API polls, by outcome",
	}, []string{"status"})

	StreamMessages = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "octoflow_stream_messages",
		Help: "Messages in the events stream, primary and fan-out copies",
	})

	StreamBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "octoflow_stream_bytes",
		Help: "Bytes stored in the events stream",
	})

	StreamConsumers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "octoflow_stream_consumers",
		Help: "Consumers on the events stream, including SSE clients",
	})
)
