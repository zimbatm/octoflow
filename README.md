# GitHub Organization Events System

A comprehensive event capture and distribution system for GitHub organizations that captures webhook events, publishes them to NATS for real-time consumption, backs them up to S3 as Parquet files, and provides real-time streaming via Server-Sent Events (SSE).

## Features

- **Webhook Reception**: Secure GitHub webhook endpoint with HMAC-SHA256 signature validation
- **Event Deduplication**: Single stream with automatic deduplication by GitHub delivery ID
- **Real-time Distribution**: Publishes events to NATS with JetStream for persistence
- **S3 Backup**: Stores events as Parquet files with daily partitioning and compression
- **SSE Streaming**: Real-time event streaming for building reactive applications
- **Event Filtering**: Configurable filters for different consumers (NATS, S3, SSE)
- **Auditing**: Complete event capture for compliance and security auditing
- **Monitoring**: Health checks and metrics endpoints

## Architecture

```
GitHub Webhooks
      |
      v
Webhook Receiver (Port 8080)
      |
      v
NATS JetStream (Primary Stream: org.{id}.{event})
      |
      v
Event Dispatcher (Entity Routing)
      |
      +-----> repo.{id}.{event}
      +-----> user.{id}.{event}  
      +-----> pr.{repo_id}.{number}
      +-----> issue.{repo_id}.{number}
      |
      v
Downstream Consumers:
      +-----> S3 Worker -----> S3 (Parquet files)
      +-----> SSE Server (Port 8081) -----> Web Dashboard
      +-----> Custom Consumers (optional)
```

### Stream Architecture

The system uses a two-stage event processing architecture:

1. **Primary Stream**: Webhook events published by organization ID
   - Subject pattern: `github.events.org.{org_id}.{event_type}` or `github.events.global.{event_type}`
   - Uses numeric IDs to handle renames gracefully
   - NATS JetStream provides automatic deduplication by GitHub delivery ID

2. **Entity Routing**: Event dispatcher extracts entities and republishes to:
   - Repository: `github.events.repo.{repo_id}.{event_type}`
   - User: `github.events.user.{user_id}.{event_type}` and `.activity`
   - Pull Request: `github.events.pr.{repo_id}.{pr_number}.{event_type}`
   - Issue: `github.events.issue.{repo_id}.{issue_number}.{event_type}`

This allows flexible subscriptions by organization, repository, user, PR, or issue.

## Prerequisites

- Go 1.21+
- NATS Server with JetStream enabled
- AWS S3 bucket (or S3-compatible storage)
- GitHub organization with webhook configuration access

## Installation

### Using Nix (Recommended)

```bash
nix develop
make build
```

### Manual Installation

```bash
go mod download
make build
```

## Configuration

The system uses environment variables for configuration. See `configs/config.example.yaml` for all available options.

### Required Environment Variables

```bash
# S3 bucket for event storage (required)
export S3_BUCKET="your-s3-bucket-name"

# Optional but recommended for production
export WEBHOOK_SECRET="your-github-webhook-secret"  # If not set, signature validation is skipped

# Optional: Override default ports
export WEBHOOK_LISTEN_ADDR=":8080"
export SSE_LISTEN_ADDR=":8081"
export NATS_URL="nats://localhost:4222"

# Optional: S3/R2 endpoint for S3-compatible services
export S3_ENDPOINT="https://your-endpoint.com"
```

### GitHub Webhook Setup

1. Go to your GitHub organization settings
2. Navigate to Webhooks
3. Add a new webhook with:
   - **Payload URL**: `http://your-server:8080/webhook`
   - **Content type**: `application/json`
   - **Secret**: Your configured webhook secret (optional but recommended)
   - **Events**: Select "Send me everything" or choose specific events

## Running the System

### Quick Start with Hivemind (Recommended)

```bash
# Copy and configure environment variables
cp .env.example .env
# Edit .env with your values

# Start all services at once
make run-local
# or
hivemind
```

This will start:
- NATS Server (port 4222, monitoring on 8222)
- Webhook Receiver (port 8080)
- Event Dispatcher (background)
- S3 Worker (background)
- SSE Server (port 8081)
- Dashboard (port 8082)

### Manual Start (Individual Services)

#### 1. Start NATS Server

```bash
nats-server -js
```

#### 2. Start the Webhook Receiver

```bash
make run-webhook
# or
./bin/webhook-receiver
```

#### 3. Start the Event Dispatcher

```bash
make run-dispatcher
# or
./bin/event-dispatcher
```

#### 4. Start the S3 Worker

```bash
make run-s3-worker
# or
./bin/s3-worker
```

#### 5. Start the SSE Server

```bash
make run-sse
# or
./bin/sse-server
```

#### 6. Open the Dashboard (Optional)

```bash
cd web
go run server.go
# Open http://localhost:8082 in your browser
```

## Event Filtering

Configure event filters in your environment or configuration file:

```yaml
processing:
  # Filter events for S3 backup
  s3_filter:
    events: ["!ping"]  # Exclude ping events
  
  # Filter events for SSE streaming
  sse_filter:
    events: ["pull_request", "issues"]
```

## Use Cases

### 1. Auditing and Compliance

Query historical events from S3 using AWS Athena:

```sql
SELECT event, repository, sender, received_at
FROM github_events
WHERE year = 2025 AND month = 1
  AND event = 'repository'
  AND action = 'publicized'
ORDER BY received_at DESC;
```

### 2. Real-time Pull Request Monitoring

Connect to the SSE endpoint to monitor pull requests:

```javascript
const eventSource = new EventSource('http://localhost:8081/events?events=pull_request');
eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log(`PR ${data.action} in ${data.repository}`);
};
```

### 3. Building Reactive Applications

Subscribe to NATS subjects for specific event types:

```go
nc, _ := nats.Connect("nats://localhost:4222")
js, _ := nc.JetStream()

// Subscribe by organization (numeric ID)
sub, _ := js.Subscribe("github.events.org.12345678.>", func(msg *nats.Msg) {
    // Process all events from organization
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    // React to the event...
})

// Subscribe by repository (numeric ID)
sub, _ := js.Subscribe("github.events.repo.87654321.>", func(msg *nats.Msg) {
    // Process all events for repository
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    // React to the event...
})

// Subscribe to all events for a specific user
sub, _ := js.Subscribe("github.events.user.11111111.activity", func(msg *nats.Msg) {
    // Process all activity for user
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    // React to the event...
})

// Subscribe to all events for a specific PR
sub, _ := js.Subscribe("github.events.pr.87654321.456", func(msg *nats.Msg) {
    // Process all events for PR #456
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    // React to the event...
})
```

## Monitoring

### Health Checks

- Webhook Receiver: `http://localhost:8080/health`
- SSE Server: `http://localhost:8081/health`

### Metrics

- Webhook Receiver: `http://localhost:8080/metrics`
- SSE Server Stats: `http://localhost:8081/stats`

## Development

### Project Structure

```
octoflow/
├── cmd/                    # Main applications
│   ├── webhook-receiver/   # GitHub webhook receiver
│   ├── event-dispatcher/  # Event entity router
│   ├── s3-worker/         # S3 backup worker
│   ├── sse-server/        # SSE streaming server
│   └── dashboard/         # Web dashboard server
├── internal/              # Internal packages
│   ├── config/           # Configuration management
│   ├── nats/             # NATS connection and schema management
│   ├── storage/          # S3 writer
│   ├── sse/              # SSE server
│   └── webhook/          # Webhook handler
├── pkg/                   # Public packages
│   ├── events/           # Event types and utilities
│   └── filters/          # Event filtering
└── web/                   # Frontend dashboard assets
```

### Running Tests

```bash
make test
```

### Building for Production

```bash
make build
```

## Security Considerations

1. **Webhook Validation**: All incoming webhooks are validated using HMAC-SHA256
2. **TLS**: Use HTTPS in production for webhook endpoints
3. **Authentication**: Configure authentication for SSE endpoints in production
4. **S3 Encryption**: Events are stored with server-side encryption
5. **Network Security**: Run NATS with authentication in production

## Performance

- Webhook receiver can handle 10,000+ events/second
- Single stream architecture with built-in deduplication
- NATS JetStream handles duplicate prevention automatically
- S3 batching optimizes storage and reduces API calls
- SSE server supports 1000+ concurrent connections
- NATS JetStream provides persistence and at-least-once delivery

## Troubleshooting

### Events Not Appearing

1. Check webhook receiver logs for signature validation errors
2. Verify NATS is running and accessible
3. Check S3 worker logs for batch processing errors

### SSE Connection Issues

1. Verify SSE server is running on the correct port
2. Check CORS settings if connecting from a browser
3. Ensure event filters match expected event types

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `make test`
5. Submit a pull request

## License

[Add your license here]