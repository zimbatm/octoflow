# Octoflow Examples

This directory contains example consumers that demonstrate how to build applications using Octoflow events.

## Available Examples

### PR Monitor (`pr-monitor.go`)

Monitors pull request activity for a specific repository.

**Features:**
- Tracks PR lifecycle (opened, closed, merged, reopened)
- Shows review status (approved, changes requested)
- Displays PR comments and updates
- Uses durable subscription for reliability

**Usage:**
```bash
export REPO_ID=12345678  # Your repository ID
export NATS_URL=nats://localhost:4222
go run pr-monitor.go
```

### Security Monitor (`security-monitor.go`)

Monitors security-relevant events across your organization.

**Features:**
- Alerts on repository visibility changes
- Tracks member additions/removals
- Monitors team permission changes
- Detects security advisories and scanning alerts
- Identifies suspicious activity patterns

**Usage:**
```bash
export ORG_ID=12345678  # Your organization ID
export NATS_URL=nats://localhost:4222
go run security-monitor.go
```

## Building Your Own Consumer

### Basic Pattern

```go
// 1. Connect to NATS
nc, err := nats.Connect("nats://localhost:4222")
js, err := nc.JetStream()

// 2. Subscribe to events
subject := "github.events.repo.*.push"
sub, err := js.Subscribe(subject, handleEvent,
    nats.Durable("my-consumer"),  // Survive restarts
    nats.DeliverNew(),            // Only new events
)

// 3. Process events
func handleEvent(msg *nats.Msg) {
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    
    // Your logic here
    
    msg.Ack() // Always acknowledge
}
```

### Subscription Patterns

```go
// Organization-wide
"github.events.org.12345678.>"

// Specific repository
"github.events.repo.87654321.>"

// All pushes across repos
"github.events.repo.*.push"

// Specific user activity
"github.events.user.11111111.*"

// PR tracking
"github.events.pr.87654321.456"
```

### Error Handling

```go
func handleEvent(msg *nats.Msg) {
    defer func() {
        if r := recover(); r != nil {
            log.Printf("Panic recovered: %v", r)
            msg.Nak() // Retry later
        }
    }()
    
    var event events.GitHubEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        log.Printf("Invalid event: %v", err)
        msg.Term() // Don't retry bad messages
        return
    }
    
    if err := processEvent(&event); err != nil {
        log.Printf("Processing failed: %v", err)
        msg.Nak() // Retry
        return
    }
    
    msg.Ack() // Success
}
```

### Consumer Groups

For scaling, use the same durable name across instances:

```go
// Instance 1
js.Subscribe(subject, handler, nats.Durable("processor"))

// Instance 2 (different process/machine)
js.Subscribe(subject, handler, nats.Durable("processor"))

// NATS will load balance between them
```

## Tips

1. **Use Durable Subscriptions**: Survive restarts and don't lose events
2. **Handle Errors Gracefully**: Use Nak() for retriable errors, Term() for bad data
3. **Add Monitoring**: Track processing time and error rates
4. **Test with Replay**: Use NATS replay to test with historical events
5. **Start Simple**: Begin with logging, then add your business logic

## More Examples

For more complex examples, see:
- CI/CD integration in the Getting Started guide
- Real-time dashboards using SSE
- S3 query examples for historical analysis