# Getting Started with Octoflow

This guide walks you through setting up Octoflow to capture and process GitHub events for your organization.

## Prerequisites

Before starting, ensure you have:

- Go 1.21+ installed
- Access to a NATS server (or Docker to run one)
- An S3 bucket (AWS S3, Cloudflare R2, MinIO, etc.)
- Admin access to your GitHub organization

## Step 1: Install Octoflow

### Option A: Using Nix (Recommended)
```bash
git clone https://github.com/zimbatm/octoflow
cd octoflow
nix develop
make build
```

### Option B: Standard Go Build
```bash
git clone https://github.com/zimbatm/octoflow
cd octoflow
go mod download
make build
```

This creates binaries in the `bin/` directory:
- `webhook-receiver` - Receives GitHub webhooks
- `event-dispatcher` - Routes events to entity subjects
- `s3-worker` - Backs up events to S3
- `sse-server` - Streams events to web clients

## Step 2: Set Up NATS

### Option A: Quick Local Setup
```bash
# Install NATS
go install github.com/nats-io/nats-server/v2@latest

# Run with JetStream enabled
nats-server -js
```

### Option B: Docker
```bash
docker run -d --name nats \
  -p 4222:4222 \
  -p 8222:8222 \
  nats:latest \
  -js
```

### Option C: Production Setup
See [NATS deployment guide](https://docs.nats.io/running-a-nats-service/introduction/installation).

## Step 3: Configure Environment

Create a `.env` file:

```bash
# Required
S3_BUCKET=my-github-events

# Recommended - get from GitHub webhook settings
WEBHOOK_SECRET=your-webhook-secret

# S3 Configuration
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
# For S3-compatible services (optional)
# S3_ENDPOINT=https://s3.example.com

# Optional overrides
# NATS_URL=nats://localhost:4222
# WEBHOOK_LISTEN_ADDR=:8080
# SSE_LISTEN_ADDR=:8081
```

## Step 4: Configure GitHub Webhook

1. Find your organization ID:
```bash
curl -H "Authorization: token YOUR_GITHUB_TOKEN" \
  https://api.github.com/orgs/YOUR_ORG | jq .id
```

2. Go to your GitHub organization settings:
   - Navigate to **Settings** → **Webhooks**
   - Click **Add webhook**

3. Configure the webhook:
   - **Payload URL**: `https://your-domain.com/webhook` (or use ngrok for testing)
   - **Content type**: `application/json`
   - **Secret**: Match your `WEBHOOK_SECRET` environment variable
   - **SSL verification**: Enable (recommended)
   - **Events**: Select "Send me everything"
   - Click **Add webhook**

## Step 5: Start Octoflow

### Option A: All-in-One (Development)
```bash
make run-local
```

This starts all components using Hivemind.

### Option B: Individual Services (Production)
```bash
# Terminal 1: Webhook receiver
./bin/webhook-receiver

# Terminal 2: Event dispatcher
./bin/event-dispatcher

# Terminal 3: S3 worker
./bin/s3-worker

# Terminal 4: SSE server (optional)
./bin/sse-server
```

## Step 6: Verify Setup

1. **Test the webhook**:
   - Go to GitHub webhook settings
   - Click on your webhook
   - Go to "Recent Deliveries" tab
   - Click "Redeliver" on the ping event
   - Should show green checkmark

2. **Check NATS**:
```bash
# Install NATS CLI
go install github.com/nats-io/natscli/nats@latest

# Check stream
nats stream info GITHUB_EVENTS_RAW

# Subscribe to test
nats sub "github.events.org.YOUR_ORG_ID.>"
```

3. **Trigger a test event**:
   - Create an issue in any repo in your org
   - You should see the event in your NATS subscription

## Step 7: Build Your First Consumer

### Example: Slack Notifications for PRs

```go
package main

import (
    "encoding/json"
    "log"
    
    "github.com/nats-io/nats.go"
    "github.com/zimbatm/octoflow/pkg/events"
)

func main() {
    // Connect to NATS
    nc, err := nats.Connect("nats://localhost:4222")
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Close()
    
    // Create JetStream context
    js, err := nc.JetStream()
    if err != nil {
        log.Fatal(err)
    }
    
    // Subscribe to PR events for your org
    orgID := "12345678" // Your org ID
    _, err = js.Subscribe(
        "github.events.org." + orgID + ".pull_request",
        handlePullRequest,
    )
    if err != nil {
        log.Fatal(err)
    }
    
    log.Println("Listening for pull request events...")
    select {} // Block forever
}

func handlePullRequest(msg *nats.Msg) {
    var event events.GitHubEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        log.Printf("Error parsing event: %v", err)
        msg.Ack()
        return
    }
    
    // Send to Slack based on action
    switch event.Action {
    case "opened":
        notifySlack("New PR opened: " + event.PullRequest.Title)
    case "merged":
        notifySlack("PR merged: " + event.PullRequest.Title)
    }
    
    msg.Ack()
}

func notifySlack(message string) {
    // Your Slack integration here
    log.Printf("Would send to Slack: %s", message)
}
```

## Next Steps

- Read the [Quick Reference](quick-reference.md) for common patterns
- Explore [Stream Topology](stream-topology.md) for routing details
- Check [GitHub Event Types](github-event-types.md) for event schemas
- Set up monitoring and alerting for production

## Troubleshooting

### Webhook not receiving events?
- Check GitHub webhook recent deliveries for errors
- Ensure `WEBHOOK_SECRET` matches GitHub configuration
- Check webhook receiver logs: `docker logs webhook-receiver`

### Events not appearing in NATS?
- Verify NATS is running: `nats server info`
- Check stream exists: `nats stream list`
- Look for errors in dispatcher logs

### S3 uploads failing?
- Verify S3 credentials and bucket permissions
- Check S3 worker logs for specific errors
- Ensure bucket exists and is accessible

## Support

- GitHub Issues: [github.com/zimbatm/octoflow/issues](https://github.com/zimbatm/octoflow/issues)
- Documentation: [github.com/zimbatm/octoflow/docs](https://github.com/zimbatm/octoflow/docs)