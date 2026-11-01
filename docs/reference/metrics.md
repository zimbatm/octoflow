# Metrics

Prometheus text format on `METRICS_LISTEN_ADDR` at `/metrics`, with a
liveness check at `/healthz`. One listener per process; in `octoflow
all` every series is on it. Labels are bounded: event types, statuses,
route kinds and sources, never ids, with one exception noted below.

## Webhook receiver

| Series | Labels | Meaning |
|---|---|---|
| `octoflow_webhook_requests_total` | `event_type`, `organization`, `status` | one per request; `event_type` is set only after the signature was verified |
| `octoflow_webhook_request_duration_seconds` | `event_type` | histogram, request start to reply |
| `octoflow_webhook_payload_size_bytes` | `event_type` | histogram |

`status` values: `success`, `success_ping`, `invalid_signature`,
`missing_headers`, `bad_content_type`, `method_not_allowed`,
`too_large`, `read_error`, `org_not_allowed`, `invalid_event`,
`publish_error`. `organization` is the login from the payload, one
series per organization that delivers.

## Publishing

| Series | Labels | Meaning |
|---|---|---|
| `octoflow_nats_published_events_total` | `stream`, `event_type`, `status` | JetStream publishes from any service |
| `octoflow_nats_publish_duration_seconds` | `stream` | histogram, until the ack |

`status`: `success`, `publish_error`, `ack_error`, `timeout`,
`marshal_error`. `stream` is always `events`.

## Dispatcher

| Series | Labels | Meaning |
|---|---|---|
| `octoflow_dispatched_events_total` | `status` | `ok`, `error` (nak, redelivered), `undecodable` (terminated) |

## S3 service

| Series | Labels | Meaning |
|---|---|---|
| `octoflow_s3_batches_written_total` | `status` | `success` or `upload_error` |
| `octoflow_s3_events_per_batch` | | histogram |
| `octoflow_s3_batch_size_bytes` | | histogram of payload bytes per batch |
| `octoflow_s3_write_duration_seconds` | | histogram, encode plus upload |

## SSE server

| Series | Labels | Meaning |
|---|---|---|
| `octoflow_sse_clients` | | open connections |
| `octoflow_sse_events_sent_total` | `route` | `all`, `org`, `repo`, `user`, `pr`, `issue`, `type` |

## public-ingest

| Series | Labels | Meaning |
|---|---|---|
| `octoflow_public_ingest_polls_total` | `status` | `ok`, `not_modified`, `rate_limited`, `error` |
| `octoflow_public_ingest_events_total` | `source` | `events-api`, `gharchive` |

## Stream

Refreshed every 30 seconds by any process with a NATS connection and a
metrics listener.

| Series | Meaning |
|---|---|
| `octoflow_stream_messages` | messages in the stream, primary and copies |
| `octoflow_stream_bytes` | bytes stored |
| `octoflow_stream_consumers` | consumers, SSE connections included |

## Errors

| Series | Labels | Meaning |
|---|---|---|
| `octoflow_event_processing_errors_total` | `component`, `error_type` | currently `webhook`/`parse_error` when a payload's routing fields cannot be read |

The Go runtime and process collectors of the Prometheus client are
exposed as well.
