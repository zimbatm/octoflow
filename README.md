# Octoflow - GitHub Event Processing System

Octoflow captures GitHub webhook events and distributes them through NATS JetStream using numeric IDs for rename-proof subscriptions. Events flow through a two-stage processing pipeline that enables flexible routing by organization, repository, user, pull request, or issue.

## Key Features

- **Two-stage event routing**: Organization-based primary stream → entity-specific dispatch
- **Rename-proof subscriptions**: Uses GitHub numeric IDs instead of names
- **Automatic deduplication**: NATS JetStream prevents duplicate events using the GitHub delivery ID or event ID
- **Repositories without a webhook**: `public-ingest` reads the public Events API and GH Archive (best-effort, see below)
- **Multi-dimensional routing**: Subscribe by org, repo, user, PR, or issue
- **S3 backup**: Parquet files with daily partitioning for long-term storage
- **Real-time streaming**: SSE endpoint for web applications
- **Simple architecture**: Focused on reliability over complex features

## Architecture

```
GitHub Webhooks                 Events API + GH Archive
      |                                  |
      v                                  v
Webhook Receiver (Port 8080)      public-ingest
      |                                  |
      v                                  v
NATS JetStream - Primary Stream
  └── github.events.org.{org_id}.{event_type}
  └── github.events.global.{event_type}
      |
      v
Event Dispatcher (Entity Router)
      |
      ├── github.events.repo.{repo_id}.{event_type}
      ├── github.events.user.{user_id}.{event_type}
      ├── github.events.pr.{repo_id}.{pr_number}.{event_type}
      ├── github.events.pr.{repo_id}.{pr_number}
      ├── github.events.issue.{repo_id}.{issue_number}.{event_type}
      └── github.events.issue.{repo_id}.{issue_number}
            |
            v
      Consumer Applications
      ├── S3 Worker → Parquet files
      ├── SSE Server → Web dashboard
      └── Your custom consumers
```

### How It Works

1. **Webhook receives event** from GitHub with organization, repository, and user IDs
2. **Primary publish** to `github.events.org.{org_id}.{event_type}` with deduplication
3. **Event dispatcher** extracts entities and publishes to specific subjects
4. **Consumers subscribe** using wildcards to get exactly the events they need

### Why Two Stages?

- **Performance**: Webhook responds quickly without complex routing logic
- **Reliability**: Primary stream captures everything, dispatcher can retry
- **Flexibility**: Add new routing dimensions without changing webhook receiver
- **Simplicity**: Each component has a single responsibility

## Event Sources: What Is Complete

Octoflow has two ingests. Both publish to the same subjects in the same
shape, so a consumer reads one stream. `metadata.source` tells where an
event came from, if a consumer needs it.

| Source | Used for | Complete? | Delay | Payload |
|---|---|---|---|---|
| Webhook receiver | Repositories and organizations we can add a webhook to (`numtide`) | **Yes**. GitHub sends every subscribed event. A failed delivery gets a 503 and can be redelivered. | Seconds | Full webhook payload |
| `public-ingest`: Events API | Repositories we cannot add a webhook to (`NixOS/nixpkgs`) | **No, best-effort.** | 30 s to 6 h | Trimmed |
| `public-ingest`: GH Archive | Fills gaps from the Events API | **No, best-effort.** | About 1 h 10 min | Trimmed |

What "best-effort" means, measured for `NixOS/nixpkgs` on 2026-10-14:

- The Events API keeps only the newest 300 events of a repository, for at
  most 30 days. GitHub says it is "not built to serve real-time use cases"
  and that delays go from 30 s to 6 h
  ([docs](https://docs.github.com/en/rest/activity/events)). For 24 hours it
  held 277 nixpkgs events, while 1537 nixpkgs issues and pull requests
  changed in the same 24 hours. Most activity never shows up.
- GH Archive is built from the same public timeline
  ([gharchive.org](https://www.gharchive.org/)). Its hourly files held 6 to 7
  nixpkgs events per hour. Some hours are missing for good.
- Since 2025-10-07 the Events API trims payloads. A `pull_request` event
  carries only `id`, `number`, `url`, `base` and `head` of the pull request,
  without title, state or author. A `push` event has no commit list
  ([changelog](https://github.blog/changelog/2025-08-08-upcoming-changes-to-github-events-api-payloads/)).
  Fetch the object from the API when you need more.
- The Events API covers 16 event types. Webhook-only types such as
  `check_run`, `status` or `workflow_run` never arrive for nixpkgs.

Use the public events as a hint that something changed, not as a record of
everything that changed.

### How the public events are rewritten

- The id is `gh-event-<github event id>`. GH Archive keeps the Events API
  ids, so JetStream drops an event that both sources deliver (within its
  24 h deduplication window).
- `PullRequestReviewCommentEvent` becomes `pull_request_review_comment`, and
  so on.
- The payload gains the `repository`, `sender` and `organization` objects of
  a webhook, so the subjects use the same numeric ids.
- `metadata.created_at` is the time GitHub recorded the event;
  `received_at` is when octoflow read it.

Do not list a repository in `PUBLIC_REPOS` that also has a webhook. Webhook
deliveries and public events have different ids, so each event would
arrive twice.

### public-ingest configuration

```bash
PUBLIC_REPOS=NixOS/nixpkgs      # comma-separated, required
GITHUB_TOKEN=...                # optional; without it, 60 requests per hour
EVENTS_API_ENABLED=true
EVENTS_API_INTERVAL=1m          # GitHub's X-Poll-Interval wins when longer
GHARCHIVE_ENABLED=true
GHARCHIVE_START=2026-10-14T00:00:00Z   # first run only; default: 3 hours ago
```

Progress is kept in the NATS KV bucket `octoflow_public_ingest`.

## Quick Start

### Prerequisites

- Go 1.21+
- NATS Server with JetStream
- S3 bucket (AWS or compatible)
- GitHub organization webhook access

### Installation

```bash
# Using Nix (recommended)
nix develop
make build

# Or manually
go mod download
make build
```

### Configuration

```bash
# Required
export S3_BUCKET="your-bucket-name"

# Recommended for production
export WEBHOOK_SECRET="your-github-secret"

# Optional overrides
export WEBHOOK_LISTEN_ADDR=":8080"
export SSE_LISTEN_ADDR=":8081"
export NATS_URL="nats://localhost:4222"
```

### GitHub Webhook Setup

1. Go to your GitHub organization settings → Webhooks
2. Add webhook:
   - **Payload URL**: `http://your-server:8080/webhook`
   - **Content type**: `application/json`
   - **Secret**: Your webhook secret
   - **Events**: "Send me everything"

### Running

```bash
# Quick start - runs all services
cp .env.example .env
# Edit .env with your values
make run-local

# Or start services individually:
nats-server -js                    # NATS with JetStream
./bin/webhook-receiver             # Receives GitHub webhooks
./bin/event-dispatcher             # Routes to entity subjects  
./bin/s3-worker                   # Backs up to S3
./bin/sse-server                  # Streams to web clients
```

Dashboard available at http://localhost:8082

## Subscription Examples

### NATS CLI

```bash
# All events for organization 12345678
nats sub "github.events.org.12345678.>"

# All push events across all repos
nats sub "github.events.repo.*.push"

# Everything happening to user 11111111  
nats sub "github.events.user.11111111.*"

# Watch PR #456 in repo 87654321
nats sub "github.events.pr.87654321.456"

# All PRs in a repository
nats sub "github.events.pr.87654321.>"
```

### Go Application

```go
nc, _ := nats.Connect("nats://localhost:4222")
js, _ := nc.JetStream()

// Subscribe to all events for your org
sub, _ := js.Subscribe("github.events.org.12345678.>", func(msg *nats.Msg) {
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    // Process event...
})

// Track specific user activity
sub, _ := js.Subscribe("github.events.user.11111111.*", func(msg *nats.Msg) {
    // User performed an action
})

// Monitor a pull request
sub, _ := js.Subscribe("github.events.pr.87654321.456", func(msg *nats.Msg) {
    // PR was updated
})
```

### JavaScript (SSE)

Each event is a named SSE event (`pull_request`, `push`, ...). Its `id` is
the JetStream sequence, so the browser resumes after a reconnect with
`Last-Event-ID`. `?last=N` (at most 1000) replays the last N matching
events first; `?type=` and `?action=` filter.

```javascript
const events = new EventSource('https://events.nixpkgs.org/events/repo/4542716?type=pull_request&last=20');
events.addEventListener('pull_request', (event) => {
  const data = JSON.parse(event.data);
  console.log(`PR #${data.payload.number} ${data.action} by ${data.sender}`);
});
```

Routes: `/events` (all), `/events/org/{id}`, `/events/repo/{id}`,
`/events/user/{id}`, `/events/pr/{repo_id}/{number}`,
`/events/issue/{repo_id}/{number}`, `/events/type/{event}`.

## Common Use Cases

### CI/CD Automation

Trigger builds when specific branches are pushed:

```go
js.Subscribe("github.events.repo.*.push", func(msg *nats.Msg) {
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    
    if event.Ref == "refs/heads/main" {
        triggerBuild(event.Repository.ID)
    }
})
```

### Security Monitoring

Alert on repository visibility changes:

```go
js.Subscribe("github.events.repo.*.repository", func(msg *nats.Msg) {
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    
    if event.Action == "publicized" {
        sendAlert("Repository made public", event.Repository)
    }
})
```

### Developer Productivity

Track PR review times:

```go
js.Subscribe("github.events.pr.*.*.pull_request_review", func(msg *nats.Msg) {
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    
    recordReviewTime(event.PullRequest.ID, event.Review.SubmittedAt)
})
```

### Compliance Auditing

Query S3 for historical analysis:

```sql
-- Find all public repository events
SELECT event_time, repository_name, actor_login
FROM github_events 
WHERE event_type = 'repository' 
  AND action = 'publicized'
  AND year = 2025
ORDER BY event_time DESC;
```

## Why Numeric IDs?

GitHub entities can be renamed:
- Organizations change names
- Repositories get renamed or transferred  
- Users update their usernames

Using numeric IDs means:
- ✅ Subscriptions never break due to renames
- ✅ Historical data remains connected
- ✅ No need to update downstream systems
- ✅ Consistent routing across ownership transfers

## Event Filtering

Configure filters in your environment:

```yaml
processing:
  s3_filter:
    events: ["!ping"]  # Exclude ping events from S3
  
  sse_filter:
    events: ["pull_request", "issues"]  # Only these for SSE
```

## Monitoring

- Health checks: `/health` on ports 8080 and 8081
- Metrics: `/metrics` on webhook receiver
- NATS monitoring: http://localhost:8222

## Production Deployment

### Security

- Always set `WEBHOOK_SECRET` for signature validation
- Use TLS for webhook endpoints
- Enable NATS authentication
- Configure S3 encryption
- Add authentication to SSE endpoints

### Performance

- Webhook receiver: 10,000+ events/second
- NATS deduplication window: 24 hours
- S3 batching: 1000 events or 5 minutes
- SSE connections: 1000+ concurrent
- Event dispatcher: Parallel workers for routing

### NixOS

The flake exports `nixosModules.default`. It runs NATS with JetStream on
127.0.0.1, one systemd unit per component, and nginx with ACME:

```nix
{
  imports = [ inputs.octoflow.nixosModules.default ];
  services.octoflow = {
    enable = true;
    domain = "events.nixpkgs.org";
    webhook.environmentFile = "/run/secrets/octoflow-webhook";   # WEBHOOK_SECRET=
    s3 = {
      bucket = "octoflow";
      endpoint = "https://<account>.r2.cloudflarestorage.com";
      environmentFile = "/run/secrets/octoflow-s3";   # AWS_ACCESS_KEY_ID=, AWS_SECRET_ACCESS_KEY=
    };
    publicIngest = {
      repos = [ "NixOS/nixpkgs" ];
      environmentFile = "/run/secrets/octoflow-github";   # GITHUB_TOKEN=
    };
  };
}
```

`nix flake check` builds the binaries, runs the Go tests and runs
`checks/nixos.nix`, a VM test of this module.

## Troubleshooting

**Events not appearing?**
- Check webhook signature validation in logs
- Verify NATS JetStream is enabled
- Ensure event dispatcher is running
- Check your subscription wildcards

**High memory usage?**
- Reduce S3 batch size
- Lower NATS consumer prefetch
- Check for subscription backlogs

## Development

```bash
make test           # Run tests
make build          # Build all binaries
make run-local      # Start everything locally
```

## Documentation

- **[Getting Started Guide](docs/getting-started.md)** - Step-by-step setup instructions
- **[Quick Reference](docs/quick-reference.md)** - Common patterns and commands
- **[Stream Topology](docs/stream-topology.md)** - Detailed routing architecture
- **[NATS CLI Guide](docs/nats-cli-guide.md)** - Using NATS command line tools
- **[GitHub Event Types](docs/github-event-types.md)** - Event schemas and examples

## Example Applications

See the [examples/](examples/) directory for complete working examples:
- **[PR Monitor](examples/pr-monitor.go)** - Track pull request lifecycle
- **[Security Monitor](examples/security-monitor.go)** - Alert on security events
- **[Examples README](examples/README.md)** - Building your own consumers

## Architecture Deep Dive

### NATS JetStream Configuration

- **Stream**: `GITHUB_EVENTS_RAW`
- **Subjects**: `github.events.>` (flexible depth)
- **Deduplication**: 24-hour window on GitHub delivery ID
- **Retention**: Configurable (default 24 hours)
- **Delivery**: At-least-once guarantee

### Component Responsibilities

**Webhook Receiver**
- Validates GitHub signatures
- Publishes to org-based subjects
- Returns quickly to GitHub
- No complex processing

**Event Dispatcher**  
- Subscribes to primary stream
- Extracts entity IDs from events
- Publishes to entity-specific subjects
- Handles failures gracefully

**S3 Worker**
- Batches events for efficiency  
- Writes Parquet files with compression
- Partitions by date for queries
- Handles backpressure

**SSE Server**
- Streams filtered events to browsers
- Maintains persistent connections
- Supports reconnection with last-event-id

## Contributing

1. Fork the repository
2. Create a feature branch  
3. Run tests: `make test`
4. Ensure `gofmt` compliance
5. Submit a pull request

## License

MIT License - see LICENSE file for details