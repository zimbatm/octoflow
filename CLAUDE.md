# CLAUDE.md

Guidance for Claude Code when working in this repository.

## OctoFlow

OctoFlow captures GitHub events, publishes them to NATS JetStream, backs
them up to S3 as Parquet files and streams them to browsers over SSE.
docs/README.md is the documentation index (Diátaxis: tutorials, how-to,
reference, explanation); this file lists what you need to work here.

## Commands

```bash
nix develop            # Go, golangci-lint, nats-server, nats CLI, rustfs, hivemind
make build             # bin/octoflow, then go build ./...
make test              # go test ./...; several packages start an embedded NATS server
make test-race         # the same with the race detector
make lint              # golangci-lint run (config in .golangci.yml)
make fmt               # gofmt + go mod tidy
make run               # all services as processes under hivemind, see Procfile
nix flake check        # builds the package, runs the Go tests and the NixOS VM test
```

One binary, one subcommand per service: `go run ./cmd/octoflow <service>`,
or `octoflow all` for every service in one process (s3 only with
`S3_BUCKET`, public-ingest only with `PUBLIC_REPOS`). After a go.mod
change, update `vendorHash` in package.nix (set it to `pkgs.lib.fakeHash`,
run `nix build`, copy the hash it prints).

## Services (`cmd/octoflow`)

1. `webhook`: validates the GitHub signature, publishes to the
   primary subjects, replies 200 only after JetStream acked. Refuses to
   start without `WEBHOOK_SECRET` unless `WEBHOOK_ALLOW_UNSIGNED=true`.
2. `dispatcher`: consumes the primary subjects and republishes each event
   to repo, user, pr and issue subjects (`internal/dispatch`).
3. `s3`: batches primary events into Parquet files in S3; acks only after
   the upload (`internal/storage`).
4. `sse`: streams events to browsers; `Last-Event-ID` is the
   JetStream sequence (`internal/sse`).
5. `dashboard`: serves `web/`; the page finds the SSE server
   from its own origin or `?sse=`.
6. `public-ingest`: reads the GitHub Events API and GH Archive for
   repositories without a webhook (`internal/publicingest`, best-effort).
7. `restore --from DATE`: publishes archived events from S3 back into the
   stream (`internal/restore`); not a service, exits when done.

The webhook, sse and dashboard routes share one server on
`HTTP_LISTEN_ADDR` (:8080) with `GET /health`; `*_LISTEN_ADDR` moves a
service to its own server. `main.go` owns config, the one NATS connection, the stream, signals and
shutdown; `services.go` has one `run` function per service.

## NATS subjects

- Primary: `github.events.org.{org_id}.{event_type}` and
  `github.events.global.{event_type}` (webhook, public-ingest)
- Fan-out: `github.events.repo.{repo_id}.{event_type}`,
  `github.events.user.{user_id}.{event_type}`,
  `github.events.pr.{repo_id}.{number}.{event_type}`,
  `github.events.issue.{repo_id}.{number}.{event_type}`

Every fan-out subject ends in the event type: one pull request is
`pr.{repo}.{n}.*`, all pull requests of a repository `pr.{repo}.*.*`.
All subjects share one stream, `GITHUB_EVENTS_RAW`, which deduplicates by
message id for 24 hours (copies use `<id>@<subject>`). A consumer of the
whole stream sees each event once per copy; consume the primary subjects
(`internal/nats.PrimarySubjects`) unless you want the copies.

## Packages

- `internal/config`: all settings, from the environment only
- `internal/nats`: connection, publisher, stream and consumer helpers
- `internal/natstest`: embedded nats-server for tests
- `internal/dispatch`: fan-out subjects and republishing
- `internal/webhook`: HTTP server and GitHub webhook handler
- `internal/storage`: Parquet batching behind an Uploader interface, and the reader
- `internal/restore`: archive to stream
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
S3_BUCKET=...                 # s3 only
S3_ENDPOINT=http://...        # R2, rustfs; unset for AWS
S3_BATCH_TIMEOUT=1h           # archive lag; S3_BATCH_SIZE=1000, S3_BATCH_MAX_BYTES=100MiB
AWS_REGION / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY   # unset keys: AWS credential chain
NATS_URL=nats://localhost:4222
NATS_USERNAME / NATS_PASSWORD / NATS_TOKEN / NATS_NKEY_SEED
NATS_STREAM_REPLICAS=1        # NATS_STREAM_MAX_BYTES=10GiB caps the stream, default none
PUBLIC_REPOS=NixOS/nixpkgs    # public-ingest only; GITHUB_TOKEN, EVENTS_API_INTERVAL, GHARCHIVE_START
HTTP_LISTEN_ADDR=:8080        # webhook, sse and dashboard; WEBHOOK_/SSE_/DASHBOARD_LISTEN_ADDR split them; WEB_DIR=./web
METRICS_LISTEN_ADDR=:9180     # Prometheus /metrics and /healthz; unset disables
LOG_FORMAT=text               # or json; LOG_LEVEL=debug|info|warn|error
```

See `.env.example`; `nix develop` loads `.env` when it exists.

## Scripts

- `scripts/test-webhook.sh`: post a signed sample webhook (org 222, repo 111, PR 7)
- `scripts/test-sse.sh`: read the SSE endpoints
- `scripts/test-system.sh`: start everything and check the health endpoints
- `scripts/clear-streams.sh`: delete all NATS streams
- `scripts/create-bucket.sh`: create the bucket on the local rustfs
- `scripts/load.sh`: local throughput measurement, numbers in docs/explanation/architecture.md

## Deployment

`modules/nixos/default.nix` exports `services.octoflow`, one systemd unit
per service running `octoflow <service>`; `checks/nixos.nix` is its VM
test. To expose a local receiver, run
`cloudflared tunnel --url http://localhost:8080` yourself.
