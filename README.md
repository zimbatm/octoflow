# Octoflow - GitHub Event Processing System

Octoflow captures GitHub webhook events and distributes them through NATS JetStream using numeric IDs for rename-proof subscriptions. Events flow through a two-stage processing pipeline that enables flexible routing by organization, repository, user, pull request, or issue.

## Key Features

- **Two-stage event routing**: Organization-based primary stream → entity-specific dispatch
- **Rename-proof subscriptions**: Uses GitHub numeric IDs instead of names
- **Automatic deduplication**: NATS JetStream prevents duplicate events using GitHub delivery ID
- **Multi-dimensional routing**: Subscribe by org, repo, user, PR, or issue
- **S3 backup**: Parquet files with daily partitioning for long-term storage
- **Real-time streaming**: SSE endpoint for web applications
- **Simple architecture**: Focused on reliability over complex features

## Architecture

```
GitHub Webhooks
      |
      v
Webhook Receiver (Port 8080)
      |
      v
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

```javascript
const events = new EventSource('http://localhost:8081/events?events=pull_request');
events.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log(`PR ${data.action}: ${data.pull_request.title}`);
};
```

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