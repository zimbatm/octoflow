# Octoflow Quick Reference

## Subscription Cheat Sheet

All ids are numeric GitHub ids; see "Finding IDs" below.

### Watch Everything
```bash
# All events in your org, once each
nats sub "github.events.org.YOUR_ORG_ID.>"

# All events for a repo
nats sub "github.events.repo.REPO_ID.>"

# All activity by a user
nats sub "github.events.user.USER_ID.*"
```

### Watch Specific Events
```bash
# Only pushes, in every repo
nats sub "github.events.repo.*.push"

# Only PR reviews
nats sub "github.events.repo.*.pull_request_review"

# Only releases
nats sub "github.events.repo.*.release"
```

### Watch PRs and Issues
```bash
# One PR
nats sub "github.events.pr.REPO_ID.PR_NUMBER.*"

# All PRs in a repo
nats sub "github.events.pr.REPO_ID.*.*"

# One issue
nats sub "github.events.issue.REPO_ID.ISSUE_NUMBER.*"
```

Every fan-out subject ends in the event type, so `*` in that position
selects every type.

## Finding IDs

```bash
curl -s https://api.github.com/orgs/ORG_NAME | jq .id
curl -s https://api.github.com/repos/OWNER/REPO | jq .id
curl -s https://api.github.com/users/USERNAME | jq .id
```

Add `-H "Authorization: Bearer TOKEN"` for private names or more than 60
requests an hour.

## Event Structure

Every message is a `pkg/events.GitHubEvent`:

```json
{
  "id": "3fa3a85c-6a48-11f0-8793-19d640392600",
  "event": "pull_request",
  "action": "opened",
  "hook_id": "12345",
  "received_at": "2026-10-14T12:00:00Z",
  "repository": "numtide/octoflow",
  "repository_id": "87654321",
  "organization": "numtide",
  "organization_id": "12345678",
  "sender": "zimbatm",
  "sender_id": "11111111",
  "payload": { "...": "the raw GitHub webhook payload" },
  "metadata": { "source": "events-api" }
}
```

`id` is the GitHub delivery id, or `gh-event-<id>` for public-ingest.
The parsed fields are strings; decode `payload` for anything else.
`metadata` is only present on public-ingest events.

## Common Patterns

Decode `payload` into a struct with the fields you need:

```go
// Build on push to main
js.Subscribe("github.events.repo.*.push", func(msg *nats.Msg) {
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    var push struct {
        Ref string `json:"ref"`
    }
    json.Unmarshal(event.Payload, &push)
    if push.Ref == "refs/heads/main" {
        triggerBuild(event.RepositoryID)
    }
    msg.Ack()
}, nats.Durable("ci"), nats.DeliverNew())

// Alert on repositories made public
js.Subscribe("github.events.repo.*.repository", func(msg *nats.Msg) {
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    if event.Action == "publicized" {
        sendAlert(event.Repository)
    }
    msg.Ack()
}, nats.Durable("security"), nats.DeliverNew())

// Auto-assign reviewers on new PRs
js.Subscribe("github.events.pr.*.*.pull_request", func(msg *nats.Msg) {
    var event events.GitHubEvent
    json.Unmarshal(msg.Data, &event)
    var pr struct {
        PullRequest struct {
            Number int `json:"number"`
        } `json:"pull_request"`
    }
    json.Unmarshal(event.Payload, &pr)
    if event.Action == "opened" {
        assignReviewers(event.Repository, pr.PullRequest.Number)
    }
    msg.Ack()
}, nats.Durable("reviewers"), nats.DeliverNew())
```

See [examples/](../examples/) for complete programs.

## Environment Variables

```bash
# Required by the webhook receiver
export WEBHOOK_SECRET="github-secret"      # or WEBHOOK_ALLOW_UNSIGNED=true

# Required by the s3 service
export S3_BUCKET="my-bucket"
export AWS_ACCESS_KEY_ID="your-key"         # unset: AWS credential chain
export AWS_SECRET_ACCESS_KEY="your-secret"
export S3_ENDPOINT="https://account.r2.cloudflarestorage.com"   # R2, rustfs

# Overrides
export NATS_URL="nats://nats.example.com:4222"
export WEBHOOK_LISTEN_ADDR=":8080"
export SSE_LISTEN_ADDR=":8081"
export METRICS_LISTEN_ADDR=":9180"
export LOG_FORMAT=json
```

`.env.example` lists them all.

## Testing Your Setup

```bash
# Send a signed sample delivery
WEBHOOK_SECRET=... ./scripts/test-webhook.sh

# Look at the stream
nats stream info GITHUB_EVENTS_RAW
nats sub "github.events.org.*.pull_request"

# Read it back over SSE
curl -N 'http://localhost:8081/events?last=5'
```

## Debugging

```bash
# Health
curl http://localhost:8080/health      # webhook receiver
curl http://localhost:8081/health      # SSE server
curl http://localhost:9180/healthz     # any process with METRICS_LISTEN_ADDR

# Metrics
curl http://localhost:9180/metrics | grep octoflow_

# Logs (hivemind prints them; NixOS keeps them in the journal)
journalctl -u octoflow-webhook -f

# NATS
nats stream report
nats consumer info GITHUB_EVENTS_RAW event-dispatcher
nats consumer info GITHUB_EVENTS_RAW s3-worker
```
