package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Webhook metrics
	WebhookRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_webhook_requests_total",
		Help: "Total number of webhook requests received",
	}, []string{"event_type", "organization", "status"})

	WebhookRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "octoflow_webhook_request_duration_seconds",
		Help:    "Duration of webhook request processing",
		Buckets: prometheus.DefBuckets,
	}, []string{"event_type"})

	WebhookPayloadSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "octoflow_webhook_payload_size_bytes",
		Help:    "Size of webhook payloads in bytes",
		Buckets: prometheus.ExponentialBuckets(100, 10, 7), // 100B to 100MB
	}, []string{"event_type"})

	// NATS metrics
	NATSPublishedEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_nats_published_events_total",
		Help: "Total number of events published to NATS",
	}, []string{"stream", "event_type", "status"})

	NATSPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "octoflow_nats_publish_duration_seconds",
		Help:    "Duration of NATS publish operations",
		Buckets: prometheus.DefBuckets,
	}, []string{"stream"})

	NATSRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_nats_publish_retries_total",
		Help: "Total number of NATS publish retries",
	}, []string{"stream", "event_type"})

	// S3 metrics
	S3BatchesWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_s3_batches_written_total",
		Help: "Total number of batches written to S3",
	}, []string{"status"})

	S3EventsPerBatch = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "octoflow_s3_events_per_batch",
		Help:    "Number of events per S3 batch",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1 to 512
	})

	S3BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "octoflow_s3_batch_size_bytes",
		Help:    "Size of S3 batches in bytes",
		Buckets: prometheus.ExponentialBuckets(1000, 10, 7), // 1KB to 1GB
	})

	S3WriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "octoflow_s3_write_duration_seconds",
		Help:    "Duration of S3 write operations",
		Buckets: prometheus.DefBuckets,
	})

	// SSE metrics
	SSEConnectedClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "octoflow_sse_connected_clients",
		Help: "Number of currently connected SSE clients",
	})

	SSEEventsStreamed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_sse_events_streamed_total",
		Help: "Total number of events streamed via SSE",
	}, []string{"event_type"})

	// Reorder metrics
	ReorderBufferSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "octoflow_reorder_buffer_size",
		Help: "Current size of the event reorder buffer",
	})

	ReorderEventAge = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "octoflow_reorder_event_age_seconds",
		Help:    "Age of events when flushed from reorder buffer",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10), // 0.1s to 100s
	})

	ReorderEventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_reorder_events_processed_total",
		Help: "Total number of events processed by reorder worker",
	}, []string{"status"})

	// General metrics
	EventProcessingErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "octoflow_event_processing_errors_total",
		Help: "Total number of event processing errors",
	}, []string{"component", "error_type"})

)

// RecordWebhookRequest records webhook request metrics
func RecordWebhookRequest(eventType, organization, status string, duration float64, payloadSize int) {
	WebhookRequestsTotal.WithLabelValues(eventType, organization, status).Inc()
	WebhookRequestDuration.WithLabelValues(eventType).Observe(duration)
	WebhookPayloadSize.WithLabelValues(eventType).Observe(float64(payloadSize))
}

// RecordNATSPublish records NATS publish metrics
func RecordNATSPublish(stream, eventType, status string, duration float64) {
	NATSPublishedEvents.WithLabelValues(stream, eventType, status).Inc()
	NATSPublishDuration.WithLabelValues(stream).Observe(duration)
}

// RecordS3Write records S3 write metrics
func RecordS3Write(status string, eventCount int, batchSize int64, duration float64) {
	S3BatchesWritten.WithLabelValues(status).Inc()
	S3EventsPerBatch.Observe(float64(eventCount))
	S3BatchSize.Observe(float64(batchSize))
	S3WriteDuration.Observe(duration)
}
