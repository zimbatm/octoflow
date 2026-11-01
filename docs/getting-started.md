# Getting Started with Octoflow

This guide walks you through setting up Octoflow to capture and process GitHub events for your organization.

## Prerequisites

Before starting, ensure you have:

- Go 1.26 (`nix develop` provides it)
- Access to a NATS server (or Docker to run one)
- An S3 bucket (AWS S3, Cloudflare R2, rustfs, etc.)
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

This creates one binary, `bin/octoflow`, with a subcommand per service:
- `octoflow webhook` - Receives GitHub webhooks
- `octoflow dispatcher` - Routes events to entity subjects
- `octoflow s3` - Backs up events to S3
- `octoflow sse` - Streams events to web clients
- `octoflow all` - Everything in one process

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
# Required by the webhook receiver; the same value goes into GitHub
WEBHOOK_SECRET=your-webhook-secret

# Required by the s3 service only
S3_BUCKET=my-github-events
AWS_ACCESS_KEY_ID=your-access-key          # unset: AWS credential chain
AWS_SECRET_ACCESS_KEY=your-secret-key
# S3_ENDPOINT=https://s3.example.com       # R2, rustfs; unset for AWS

# Optional overrides
# NATS_URL=nats://localhost:4222
# WEBHOOK_LISTEN_ADDR=:8080
# SSE_LISTEN_ADDR=:8081
# METRICS_LISTEN_ADDR=:9180
```

`.env.example` lists every variable.

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
make run
```

This starts NATS, a local S3 store and every service as its own process
under Hivemind.

### Option B: One Process
```bash
./bin/octoflow all
```

Runs every service on one NATS connection; the s3 service only with
`S3_BUCKET`, public-ingest only with `PUBLIC_REPOS`.

### Option C: Individual Services (Production)
```bash
# Terminal 1: Webhook receiver
./bin/octoflow webhook

# Terminal 2: Event dispatcher
./bin/octoflow dispatcher

# Terminal 3: S3 worker
./bin/octoflow s3

# Terminal 4: SSE server (optional)
./bin/octoflow sse
```

On NixOS, `services.octoflow` in the flake's module does this with one
systemd unit per service; see the README.

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
        nats.Durable("slack-prs"), // survives restarts
        nats.DeliverNew(),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    log.Println("Listening for pull request events...")
    select {} // Block forever
}

// prPayload is the part of the webhook payload this program reads.
type prPayload struct {
    PullRequest struct {
        Title  string `json:"title"`
        Merged bool   `json:"merged"`
    } `json:"pull_request"`
}

func handlePullRequest(msg *nats.Msg) {
    var event events.GitHubEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        log.Printf("Not an event: %v", err)
        msg.Term() // retrying cannot help
        return
    }
    var p prPayload
    json.Unmarshal(event.Payload, &p)
    
    switch event.Action {
    case "opened":
        notifySlack("New PR opened: " + p.PullRequest.Title)
    case "closed":
        if p.PullRequest.Merged {
            notifySlack("PR merged: " + p.PullRequest.Title)
        }
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
- Check GitHub webhook recent deliveries: a 401 means the secret differs
  from `WEBHOOK_SECRET`, a 503 means NATS did not store the event
- Read the receiver's log (hivemind output, or
  `journalctl -u octoflow-webhook` on NixOS)
- `curl http://localhost:9180/metrics | grep octoflow_webhook_requests_total`
  shows how each delivery was counted, when `METRICS_LISTEN_ADDR` is set

### Events not appearing in NATS?
- Verify NATS is running: `nats server info`
- Check the stream exists: `nats stream info GITHUB_EVENTS_RAW`
- `nats consumer info GITHUB_EVENTS_RAW event-dispatcher` shows the
  dispatcher's backlog; look in its log for errors

### S3 uploads failing?
- Verify S3 credentials and bucket permissions
- Read the s3 service log: a failed upload is logged and the batch is
  retried from NATS after `AckWait`
- Ensure the bucket exists; `scripts/create-bucket.sh` makes it on the
  local rustfs

## Support

- GitHub Issues: [github.com/zimbatm/octoflow/issues](https://github.com/zimbatm/octoflow/issues)
- Documentation: [github.com/zimbatm/octoflow/docs](https://github.com/zimbatm/octoflow/docs)