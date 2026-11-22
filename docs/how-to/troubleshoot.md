# Troubleshoot

Each section starts from a symptom. Logs go to stderr (hivemind shows
them) or the journal (`journalctl -u octoflow-<service>` on NixOS).
Metrics are on `METRICS_LISTEN_ADDR` when it is set; the NixOS module
sets one port per unit.

## GitHub shows failed deliveries

Open the delivery in GitHub's "Recent Deliveries" and read the response.

| Response | Cause | Fix |
|---|---|---|
| 401 Invalid signature | the secret in GitHub differs from `WEBHOOK_SECRET` | set the same value on both sides, then "Redeliver" |
| 400 Content-Type must be application/json | the webhook uses form encoding | change the content type in GitHub |
| 403 Organization not allowed | `WEBHOOK_ALLOWED_ORGS` does not list it | add the organization |
| 413 Payload too large | over 25 MB | GitHub's own limit; nothing to do |
| 503 Failed to store event | NATS did not acknowledge within the request timeout | check NATS; "Redeliver" once it is back |
| connection refused or timeout | the receiver is down or unreachable | `systemctl status octoflow-webhook`, nginx, DNS |

`octoflow_webhook_requests_total` counts every delivery by status.

## The Status section of the page looks wrong

The landing page reads [`GET /status`](../reference/sse-api.md#status).
`curl -s http://localhost:8080/status | jq 'del(.events, .refused)'`
prints the totals without the two series.

| Symptom | Cause | Check |
|---|---|---|
| "No status from this server" | the SSE server is older than `/status`, or answers 503 | `curl -i .../status`; the SSE log names the NATS error |
| the chart is flat at zero, the last event is recent | the dispatcher is down or behind; it writes the counts | `systemctl status octoflow-dispatcher`, `octoflow_consumer_pending{consumer="event-dispatcher"}` |
| the last event is old | nothing reaches the stream | "Recent Deliveries" in GitHub, then `systemctl status octoflow-webhook` |
| refused deliveries above zero | the receiver answered 503, 413 or 400 to a delivery | `octoflow_webhook_requests_total` by `status`, and the table above |
| GitHub shows failed deliveries, the page shows none refused | the request did not reach the receiver (502 from nginx, DNS), or the status is one that is not counted | the nginx log; 401 and 403 are never counted |

The counts are in the KV bucket `octoflow_status`:
`nats kv ls octoflow_status`, then `nats kv get octoflow_status events.2026-11-22T10`.
To start the chart again from zero, `nats kv del octoflow_status -f`
deletes the bucket; the writers create it again with their next count.

## The receiver does not start

`WEBHOOK_SECRET is not set` in the log: set it, or set
`WEBHOOK_ALLOW_UNSIGNED=true` for a local setup only.

`Failed to connect to NATS`: `NATS_URL` points nowhere, or NATS has no
JetStream (`nats-server -c nats.conf`).

## Events are stored but a consumer gets nothing

- Check the subject. `nats sub "github.events.>"` prints everything;
  narrow down from there. Ids are numeric; a login in the subject
  matches nothing.
- Fan-out copies come from the dispatcher. If only
  `github.events.org.>` has messages, the dispatcher is down or behind:
  `nats consumer info GITHUB_EVENTS_RAW event-dispatcher`.
- A durable consumer created with `DeliverNew` skips what was stored
  before it existed.

## The dispatcher logs "Dropping undecodable message"

Something published a non-event into the stream, usually a `nats pub`
by hand. The message is terminated and no harm is done.

## Nothing arrives in S3

- The s3 service only runs with `S3_BUCKET`; in `octoflow all` it is
  skipped silently without it.
- Uploads happen every 1000 events, one hour after the first event of a
  batch (`S3_BATCH_TIMEOUT`), and on stop.
  With few events, wait for the hour, lower the timeout, or stop the service.
- `Failed to write batch to S3` in the log names the cause: credentials,
  a missing bucket, a wrong endpoint. The batch is not lost: its
  messages are redelivered from NATS after `AckWait` (batch timeout plus
  two minutes) and uploaded with the next batch.
- `octoflow_s3_batches_written_total{status="upload_error"}` counts the
  failures.

## SSE clients get nothing

- `curl -N 'http://localhost:8080/events?last=5'` must print a
  `connected` event at once. If not, the SSE server is down or NATS is.
- A route with a non-numeric id answers 404.
- Behind a proxy, buffering must be off and the read timeout long; the
  NixOS module configures nginx that way.
- `octoflow_sse_clients` shows how many connections the server sees.

## public-ingest publishes little or nothing

That is often expected; see [Public events](../explanation/public-events.md).
Otherwise:

- `GitHub rate limit reached` in the log: add `GITHUB_TOKEN`.
- `Failed to look up <repo>`: the name is wrong or the repository is
  private.
- `octoflow_public_ingest_polls_total{status="error"}` grows: read the
  log line next to it.

## The stream grows too large

Every event is stored once per routed subject, 3 to 6 copies, for 7
days by default (`NATS_STREAM_MAX_AGE`). `octoflow_stream_messages`
and `octoflow_stream_bytes` show the trend. To drop everything at once:
`nats stream purge GITHUB_EVENTS_RAW -f` keeps the consumers and removes
the messages. [Architecture](../explanation/architecture.md) says what a
smaller stream would take.

## A name route answers 404, or the status chart has a gap

The name index and the status counts are filled by the dispatcher as
events arrive. After an upgrade from a version without them, or after
their KV bucket was deleted, they start empty. Fill them from the
stream:

```bash
octoflow reindex
```

It needs the same `NATS_*` settings as the services. See
[Filling the buckets again](../reference/subjects.md#filling-the-buckets-again).
