# Configuration

Every setting comes from the environment. `nix develop` loads `.env`
from the repository root; `.env.example` is a template. A malformed
value stops the process with an error naming the variable.

## HTTP

| Variable | Default | Meaning |
|---|---|---|
| `HTTP_LISTEN_ADDR` | `:8080` | One server for the webhook, the SSE routes and the dashboard, with `GET /health`. |
| `WEBHOOK_LISTEN_ADDR`, `SSE_LISTEN_ADDR`, `DASHBOARD_LISTEN_ADDR` | `HTTP_LISTEN_ADDR` | Give a service its own server, for example to expose only `/webhook`. The NixOS module runs one process per service and sets these. |

The servers have no read or write timeout, because SSE streams stay
open; the webhook handler bounds its own requests.

## Webhook receiver

| Variable | Default | Meaning |
|---|---|---|
| `WEBHOOK_SECRET` | none | GitHub webhook secret. Without it the receiver refuses to start. |
| `WEBHOOK_ALLOW_UNSIGNED` | `false` | `true` lets the receiver start without a secret and accept unsigned deliveries. |
| `WEBHOOK_ALLOWED_ORGS` | empty, all allowed | Comma-separated organization logins; deliveries from others get a 403. |

Fixed: payloads up to 25 MB, 10 s to store an event before the receiver
answers 503; `GET /webhook` answers a help page.

## NATS

| Variable | Default | Meaning |
|---|---|---|
| `NATS_URL` | `nats://localhost:4222` | |
| `NATS_SUBJECT_PREFIX` | `github.events` | First tokens of every subject. |
| `NATS_STREAM_NAME` | `GITHUB_EVENTS_RAW` | |
| `NATS_STREAM_REPLICAS` | `1` | JetStream replicas; needs a cluster above 1. |
| `NATS_STREAM_MAX_BYTES` | none | Size cap of the stream, for example `10GiB`; the oldest messages are dropped first. A bare number is bytes; `K`, `M`, `G`, `T` are binary suffixes, `iB` or `B` may follow. Must fit in the server's `max_file_store`, or the stream cannot be created. Without it the server limit applies and publishes fail once it is full. |
| `NATS_USERNAME`, `NATS_PASSWORD` | none | User and password. |
| `NATS_TOKEN` | none | Token, when no user is set. |
| `NATS_NKEY_SEED` | none | NKey seed, when neither is set. |

nats-server must accept messages of 32 MiB (`max_payload: 33554432`):
GitHub sends deliveries up to 25 MB and the stored event adds an
envelope. The server default is 1 MiB, above which the receiver answers
503 to every delivery. The NixOS module sets it; the receiver logs a
warning at startup when the server limit is too low.

Fixed: file storage, 7 days retention, 24 h duplicate window (shortened
to the retention when that is less), 10 reconnects 2 s apart, 5 s
connection timeout.

## S3 (the `s3` service)

| Variable | Default | Meaning |
|---|---|---|
| `S3_BUCKET` | none | Required by the s3 service; `octoflow all` skips the service without it. |
| `AWS_REGION` | `us-east-1` | R2 wants `auto`. |
| `S3_ENDPOINT` | AWS for the region | `http://` or `https://` URL of an S3-compatible store. |
| `S3_KEY_PREFIX` | `github-events` | First path element of every object key. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | none | Static credentials. When the key id is unset, the AWS chain is tried: environment, shared credentials file, instance role. |
| `S3_BATCH_TIMEOUT` | `1h` | A batch closes this long after its first event. It is the archive lag, and the time events wait unacked in the stream; `AckWait` of the consumer is this plus two minutes. |
| `S3_BATCH_SIZE` | `1000` | Events per batch; a full batch closes at once. |
| `S3_BATCH_MAX_BYTES` | `100MiB` | Payload bytes per batch; same suffixes as `NATS_STREAM_MAX_BYTES`. Bounds the memory of the service. |

A quiet repository gets one file per hour at most; a busy one a file per
1000 events. Fixed: Snappy
compression, row groups of 10000.

## SSE server

| Variable | Default | Meaning |
|---|---|---|

| `SSE_MAX_CLIENTS` | `1000` | Open streams per process; each holds a JetStream consumer. Beyond it a request gets a 503. `0` is unlimited. |

Fixed: `?last=` defaults to 50 and is capped at 1000; heartbeat every
30 s; a consumer per connection, removed one minute after the client
leaves.

## Dashboard

| Variable | Default | Meaning |
|---|---|---|
| `WEB_DIR` | `./web` | Directory with `index.html`, served at `/`. |

## public-ingest

| Variable | Default | Meaning |
|---|---|---|
| `PUBLIC_REPOS` | none | Comma-separated `owner/name`; required by the service, `octoflow all` skips it without. |
| `GITHUB_TOKEN` | none | Raises the Events API quota from 60 to 5000 requests per hour. |
| `EVENTS_API_ENABLED` | `true` | |
| `EVENTS_API_INTERVAL` | `1m` | GitHub's `X-Poll-Interval` wins when longer. |
| `GHARCHIVE_ENABLED` | `true` | |
| `GHARCHIVE_URL` | `https://data.gharchive.org` | |
| `GHARCHIVE_START` | three hours ago | RFC 3339; first hour to import when no position is saved. |

## Process

| Variable | Default | Meaning |
|---|---|---|
| `METRICS_LISTEN_ADDR` | none | Serves `/metrics` and `/healthz`; unset disables both. |
| `LOG_FORMAT` | `text` | `text` or `json`, on stderr. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. |

Fixed: 10 pull workers per durable consumer in the dispatcher and the
s3 service; 30 s for in-flight HTTP requests at shutdown.
