# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## OctoFlow - GitHub Organization Events System

OctoFlow captures GitHub webhook events, publishes them to NATS for real-time consumption, backs them up to S3 as Parquet files, and provides real-time streaming via Server-Sent Events (SSE).

## Build Commands

```bash
# Build all services
make build

# Build individual services
make webhook-receiver
make event-dispatcher
make s3-worker
make sse-server
make dashboard

# Run tests
make test
make test-coverage

# Format code
make fmt

# Run linter
make lint

# Clean build artifacts
make clean
```

## Development Commands

```bash
# Start all services with hivemind (recommended)
make run-local
# or
hivemind

# Run individual services
make run-webhook      # Port 8080
make run-dispatcher   # Background
make run-s3-worker    # Background
make run-sse          # Port 8081
make run-dashboard    # Port 8082

# Start NATS server only
make nats-server
```

## Architecture Overview

The system follows a two-stage event processing architecture:

1. **Webhook Receiver** (`cmd/webhook-receiver/`) - Receives GitHub webhooks, validates signatures, and publishes to NATS primary stream
2. **Event Dispatcher** (`cmd/event-dispatcher/`) - Consumes from primary stream and routes to entity-specific subjects (repo, user, PR, issue)
3. **S3 Worker** (`cmd/s3-worker/`) - Batches events and stores as Parquet files in S3
4. **SSE Server** (`cmd/sse-server/`) - Streams events to web clients in real-time
5. **Dashboard** (`cmd/dashboard/`) - Web UI for monitoring events

### NATS Subject Patterns

- Primary: `github.events.org.{org_id}.{event_type}` or `github.events.global.{event_type}`
- Repository: `github.events.repo.{repo_id}.{event_type}`
- User: `github.events.user.{user_id}.{event_type}` and `.activity`
- Pull Request: `github.events.pr.{repo_id}.{pr_number}.{event_type}`
- Issue: `github.events.issue.{repo_id}.{issue_number}.{event_type}`

### Key Packages

- `internal/config/` - Configuration management via environment variables
- `internal/nats/` - NATS connection, publishing, and schema management
- `internal/webhook/` - HTTP server and GitHub webhook handling
- `internal/storage/` - S3 writer with Parquet serialization
- `internal/sse/` - Server-Sent Events implementation
- `pkg/events/` - Event types, parsing, and filtering

## Required Environment Variables

```bash
# Required
export S3_BUCKET="your-s3-bucket-name"

# Recommended
export WEBHOOK_SECRET="your-github-webhook-secret"

# Optional overrides
export WEBHOOK_LISTEN_ADDR=":8080"
export SSE_LISTEN_ADDR=":8081"
export NATS_URL="nats://localhost:4222"
export S3_ENDPOINT="https://your-endpoint.com"  # For R2/MinIO
```

## Testing Scripts

```bash
# Quick integration test
./scripts/quick-test.sh

# Full system test
./scripts/test-system.sh

# Test webhook endpoint
./scripts/test-webhook.sh

# NATS stream management
./scripts/clear-streams.sh    # Clear all streams
./scripts/migrate-streams.sh  # Migrate stream schema
```

## Event Processing Flow

1. GitHub sends webhook to `/webhook` endpoint
2. Webhook receiver validates signature (if secret configured)
3. Event published to NATS with deduplication by GitHub delivery ID
4. Event dispatcher extracts entities and routes to specific subjects
5. Downstream consumers (S3, SSE, custom) subscribe to relevant subjects
6. S3 worker batches events and stores as daily-partitioned Parquet files

## Development with Nix

The project uses Nix flakes for reproducible development environments:

```bash
# Enter development shell
nix develop

# Format Nix files
nix fmt
```

## Cloudflare Tunnel Configuration

To add Cloudflare tunnel support, add this line to the Procfile:

```
tunnel-cloudflare: ./scripts/wait-for.sh 8080 cloudflared tunnel --url http://localhost:8080
```

This assumes you have `cloudflared` installed and configured with your tunnel credentials.