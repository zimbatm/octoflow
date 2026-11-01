# CLAUDE.md

Guidance for Claude Code when working in this repository.

## OctoFlow

OctoFlow captures GitHub events, publishes them to NATS JetStream, backs
them up to S3 as Parquet files and streams them to browsers over SSE.
The README is the reference; this file lists what you need to work here.

## Commands

```bash
nix develop            # Go, golangci-lint, nats-server, nats CLI, rustfs, hivemind
make build             # all binaries into ./bin, then go build ./...
make test              # go test ./...; several packages start an embedded NATS server
make lint              # golangci-lint run (config in .golangci.yml)
make fmt               # gofmt + go mod tidy
make run               # all services under hivemind, see Procfile and .env.example
nix flake check        # builds the package, runs the Go tests and the NixOS VM test
```

Run a single service with `go run ./cmd/<name>`. After a go.mod change,
update `vendorHash` in package.nix (set it to `pkgs.lib.fakeHash`, run
`nix build`, copy the hash it prints).

## Services (`cmd/`)

1. `webhook-receiver` (port 8080): validates the GitHub signature, publishes
   to the primary subjects, replies 200 only after JetStream acked. Refuses
   to start without `WEBHOOK_SECRET` unless `WEBHOOK_ALLOW_UNSIGNED=true`.
2. `event-dispatcher`: consumes the primary subjects and republishes each
   event to repo, user, pr and issue subjects (`internal/dispatch`).
3. `s3-worker`: batches primary events into Parquet files in S3; acks only
   after the upload (`internal/storage`).
4. `sse-server` (port 8081): streams events to browsers; `Last-Event-ID`
   is the JetStream sequence (`internal/sse`).
5. `dashboard` (port 8082): serves `web/`; the page finds the SSE server
   from its own address or `?sse=`.
6. `public-ingest`: reads the GitHub Events API and GH Archive for
   repositories without a webhook (`internal/publicingest`, best-effort).

## NATS subjects

- Primary: `github.events.org.{org_id}.{event_type}` and
  `github.events.global.{event_type}` (webhook receiver, public-ingest)
- Fan-out: `github.events.repo.{repo_id}.{event_type}`,
  `github.events.user.{user_id}.{event_type}`,
  `github.events.pr.{repo_id}.{number}[.{event_type}]`,
  `github.events.issue.{repo_id}.{number}[.{event_type}]`

All subjects share one stream, `GITHUB_EVENTS_RAW`, which deduplicates by
message id for 24 hours (fan-out copies use `<id>@<subject>`). A consumer
of the whole stream sees each event once per copy; consume the primary
subjects (`internal/nats.PrimarySubjects`) unless you want the copies, and
use `pr.{repo}.*`, not `pr.{repo}.>`, for all pull requests of a repository.

## Packages

- `internal/config`: all settings, from the environment only
- `internal/nats`: connection, publisher, stream and consumer helpers
- `internal/natstest`: embedded nats-server for tests
- `internal/dispatch`: fan-out subjects and republishing
- `internal/webhook`: HTTP server and GitHub webhook handler
- `internal/storage`: Parquet batching behind an Uploader interface
- `internal/sse`: SSE handler, routes and writer
- `internal/ghevents`: Events API client, GH Archive reader, conversion
- `internal/publicingest`: polling loops and progress for public-ingest
- `pkg/events`: the event type, payload parsing and entity extraction
- `examples/`: two consumers, built with `go build ./...`

Every package but `internal/metrics` has tests. The NATS, dispatch,
SSE and public-ingest tests run against an embedded server; the storage
tests use a fake uploader. `checks/nixos.nix` runs the whole pipeline in
a VM.

## Environment variables

```bash
WEBHOOK_SECRET=...            # required; WEBHOOK_ALLOW_UNSIGNED=true to run without
WEBHOOK_ALLOWED_ORGS=a,b      # optional allow-list
S3_BUCKET=...                 # s3-worker only
S3_ENDPOINT=http://...        # R2, rustfs; unset for AWS
AWS_REGION / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY   # unset keys: AWS credential chain
NATS_URL=nats://localhost:4222
NATS_USERNAME / NATS_PASSWORD / NATS_TOKEN / NATS_NKEY_SEED
NATS_STREAM_REPLICAS=1
PUBLIC_REPOS=NixOS/nixpkgs    # public-ingest only; GITHUB_TOKEN, GHARCHIVE_START
DASHBOARD_LISTEN_ADDR=:8082
```

See `.env.example`; `nix develop` loads `.env` when it exists.

## Scripts

- `scripts/test-webhook.sh`: post a signed sample webhook
- `scripts/test-sse.sh`: read the SSE endpoints
- `scripts/test-system.sh`: start everything and check the health endpoints
- `scripts/clear-streams.sh`: delete all NATS streams
- `scripts/create-bucket.sh`: create the bucket on the local rustfs

## Deployment

`modules/nixos/default.nix` exports `services.octoflow`; `checks/nixos.nix`
is its VM test. To expose a local receiver, run
`cloudflared tunnel --url http://localhost:8080` yourself.
