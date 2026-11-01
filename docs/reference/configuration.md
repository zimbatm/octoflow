# Configuration

Every setting comes from the environment. `nix develop` loads `.env`
from the repository root; `.env.example` is a template. A malformed
value stops the process with an error naming the variable.

## Webhook receiver

| Variable | Default | Meaning |
|---|---|---|
| `WEBHOOK_SECRET` | none | GitHub webhook secret. Without it the receiver refuses to start. |
| `WEBHOOK_ALLOW_UNSIGNED` | `false` | `true` lets the receiver start without a secret and accept unsigned deliveries. |
| `WEBHOOK_ALLOWED_ORGS` | empty, all allowed | Comma-separated organization logins; deliveries from others get a 403. |
| `WEBHOOK_LISTEN_ADDR` | `:8080` | |

Fixed: payloads up to 25 MB, 10 s to store an event before the receiver
answers 503, health on `/health`.

## NATS

| Variable | Default | Meaning |
|---|---|---|
| `NATS_URL` | `nats://localhost:4222` | |
| `NATS_SUBJECT_PREFIX` | `github.events` | First tokens of every subject. |
| `NATS_STREAM_NAME` | `GITHUB_EVENTS_RAW` | |
| `NATS_STREAM_REPLICAS` | `1` | JetStream replicas; needs a cluster above 1. |
| `NATS_USERNAME`, `NATS_PASSWORD` | none | User and password. |
| `NATS_TOKEN` | none | Token, when no user is set. |
| `NATS_NKEY_SEED` | none | NKey seed, when neither is set. |

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

Fixed: batches of 1000 events, 100 MB of payload or 5 minutes, Snappy
compression, row groups of 10000.

## SSE server

| Variable | Default | Meaning |
|---|---|---|
| `SSE_LISTEN_ADDR` | `:8081` | |

Fixed: `?last=` defaults to 50 and is capped at 1000; heartbeat every
30 s; a consumer per connection, removed one minute after the client
leaves.

## Dashboard

| Variable | Default | Meaning |
|---|---|---|
| `DASHBOARD_LISTEN_ADDR` | `:8082` | |
| `WEB_DIR` | `./web` | Directory with `index.html`. |

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
