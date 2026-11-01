# CLAUDE.md

Guidance for Claude Code when working in this repository.

## OctoFlow

OctoFlow captures GitHub events, publishes them to NATS JetStream, backs
them up to S3 as Parquet files and streams them to browsers over SSE.
The README is the reference; this file lists what you need to work here.

## Commands

```bash
nix develop            # Go, golangci-lint, nats-server, nats CLI, rustfs, hivemind
make build             # all binaries into ./bin
make test              # go test ./...
make lint              # golangci-lint run (config in .golangci.yml)
make fmt               # gofmt + go mod tidy
make run               # all services under hivemind, see Procfile
nix flake check        # builds, runs the Go tests and the NixOS VM test
```

Run a single service with `go run ./cmd/<name>`.

## Services (`cmd/`)

1. `webhook-receiver` (port 8080): validates the GitHub signature, publishes
   to the primary subjects, replies 200 only after JetStream acked.
2. `event-dispatcher`: consumes the primary subjects and republishes each
   event to repo, user, pr and issue subjects.
3. `s3-worker`: batches primary events into Parquet files in S3; acks only
   after the upload.
4. `sse-server` (port 8081): streams events to browsers; `Last-Event-ID`
   is the JetStream sequence.
5. `dashboard` (port 8082): serves `web/`.
6. `public-ingest`: reads the GitHub Events API and GH Archive for
   repositories without a webhook (best-effort, see the README).

## NATS subjects

- Primary: `github.events.org.{org_id}.{event_type}` and
  `github.events.global.{event_type}` (webhook receiver, public-ingest)
- Fan-out: `github.events.repo.{repo_id}.{event_type}`,
  `github.events.user.{user_id}.{event_type}`,
  `github.events.pr.{repo_id}.{number}[.{event_type}]`,
  `github.events.issue.{repo_id}.{number}[.{event_type}]`

All subjects share one stream, `GITHUB_EVENTS_RAW`, which deduplicates by
message id for 24 hours. A consumer of the whole stream sees each event
once per fan-out copy; consume the primary subjects
(`internal/nats.PrimarySubjects`) unless you want the copies.

## Packages

- `internal/config`: all settings, from the environment only
- `internal/nats`: connection, publisher, consumer helpers, stream setup
- `internal/webhook`: HTTP server and GitHub webhook handler
- `internal/storage`: S3 writer and the Parquet row schema
- `internal/sse`: SSE handler and writer
- `internal/ghevents`: Events API and GH Archive readers for public-ingest
- `pkg/events`: the event type, payload parsing and entity extraction

Tests exist for config, ghevents, storage, webhook and events. There is
no test for the NATS or SSE code outside the NixOS VM test.

## Environment variables

```bash
WEBHOOK_SECRET=...            # without it unsigned deliveries are accepted
WEBHOOK_ALLOWED_ORGS=a,b      # optional allow-list
S3_BUCKET=...                 # s3-worker only
S3_ENDPOINT=http://...        # R2, rustfs; unset for AWS
AWS_REGION / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
NATS_URL=nats://localhost:4222
NATS_USERNAME / NATS_PASSWORD / NATS_TOKEN / NATS_NKEY_SEED
PUBLIC_REPOS=NixOS/nixpkgs    # public-ingest only
```

See `.env.example`; `nix develop` loads `.env` when it exists.

## Scripts

- `scripts/quick-test.sh`, `scripts/test-webhook.sh`: post a sample webhook
- `scripts/test-sse.sh`: read the SSE endpoints
- `scripts/test-system.sh`: start everything and check the health endpoints
- `scripts/clear-streams.sh`: delete all NATS streams
- `scripts/create-bucket.sh`: create the bucket on the local rustfs

## Deployment

`modules/nixos/default.nix` exports `services.octoflow`; `checks/nixos.nix`
is its VM test. The Procfile's `tunnel-cloudflare` line carries a tunnel
id; replace it with yours or remove the line.
