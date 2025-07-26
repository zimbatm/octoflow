# Octoflow Quick Reference

## Subscription Cheat Sheet

### Watch Everything
```bash
# All events in your org
nats sub "github.events.org.YOUR_ORG_ID.>"

# All events for a repo  
nats sub "github.events.repo.REPO_ID.>"

# All activity by a user
nats sub "github.events.user.USER_ID.*"
```

### Watch Specific Events
```bash
# Only pushes to main
nats sub "github.events.repo.*.push"

# Only PR reviews
nats sub "github.events.repo.*.pull_request_review"

# Only releases
nats sub "github.events.repo.*.release"
```

### Watch PRs and Issues
```bash
# Specific PR
nats sub "github.events.pr.REPO_ID.PR_NUMBER"

# All PRs in repo
nats sub "github.events.pr.REPO_ID.>"

# Specific issue
nats sub "github.events.issue.REPO_ID.ISSUE_NUMBER"
```

## Finding IDs

### Organization ID
```bash
curl -H "Authorization: token YOUR_TOKEN" \
  https://api.github.com/orgs/YOUR_ORG_NAME | jq .id
```

### Repository ID  
```bash
curl -H "Authorization: token YOUR_TOKEN" \
  https://api.github.com/repos/OWNER/REPO | jq .id
```

### User ID
```bash
curl -H "Authorization: token YOUR_TOKEN" \
  https://api.github.com/users/USERNAME | jq .id
```

## Common Patterns

### CI/CD Triggers
```go
// Build on push to main
js.Subscribe("github.events.repo.*.push", func(msg *nats.Msg) {
    var event GitHubEvent
    json.Unmarshal(msg.Data, &event)
    if event.Ref == "refs/heads/main" {
        triggerBuild()
    }
})
```

### Security Monitoring
```go
// Alert on public repos
js.Subscribe("github.events.repo.*.repository", func(msg *nats.Msg) {
    var event GitHubEvent
    json.Unmarshal(msg.Data, &event)
    if event.Action == "publicized" {
        sendAlert()
    }
})
```

### PR Automation
```go
// Auto-assign reviewers
js.Subscribe("github.events.pr.*.*.pull_request", func(msg *nats.Msg) {
    var event GitHubEvent
    json.Unmarshal(msg.Data, &event)
    if event.Action == "opened" {
        assignReviewers(event.PullRequest)
    }
})
```

## Event Structure

All events follow this structure:
```json
{
  "id": "12345678-1234-5678-1234-567812345678",
  "event": "pull_request",
  "action": "opened",
  "organization": {"id": 12345678, "login": "myorg"},
  "repository": {"id": 87654321, "name": "myrepo"},
  "sender": {"id": 11111111, "login": "username"},
  "received_at": "2025-01-26T12:00:00Z",
  // Event-specific payload
  "pull_request": {...},
  "issue": {...}
}
```

## Environment Variables

```bash
# Minimal setup
export S3_BUCKET="my-bucket"
export WEBHOOK_SECRET="github-secret"

# Override defaults  
export NATS_URL="nats://nats.example.com:4222"
export WEBHOOK_LISTEN_ADDR=":8080"
export SSE_LISTEN_ADDR=":8081"

# S3 compatible (e.g., Cloudflare R2)
export S3_ENDPOINT="https://account.r2.cloudflarestorage.com"
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
```

## Testing Your Setup

### 1. Send Test Webhook
```bash
# From GitHub webhook settings, send a ping event
# Or use the test webhook script
./scripts/test-webhook.sh
```

### 2. Check NATS Stream
```bash
# View stream info
nats stream info GITHUB_EVENTS_RAW

# Check for messages
nats stream view GITHUB_EVENTS_RAW
```

### 3. Subscribe to Test
```bash
# Watch for ping events
nats sub "github.events.org.*.ping"
```

## Debugging

### Check Component Health
```bash
curl http://localhost:8080/health  # Webhook receiver
curl http://localhost:8081/health  # SSE server
```

### View Logs
```bash
# If using hivemind/Procfile
tail -f *.log

# Or check systemd/docker logs
journalctl -u octoflow-webhook -f
docker logs octoflow-webhook -f
```

### Monitor NATS
```bash
# Stream stats
nats stream report

# Consumer info
nats consumer info GITHUB_EVENTS_RAW event-dispatcher

# Check subscriptions
nats server subscriptions
```