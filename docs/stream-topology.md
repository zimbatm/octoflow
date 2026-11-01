# Octoflow Stream Topology

## Overview

Octoflow uses a two-stage event processing architecture designed for simplicity and flexibility:

1. **Primary Stream**: Webhook events published by organization ID using numeric IDs
2. **Entity Routing**: Event dispatcher extracts entities and republishes to specific subjects

## Event Flow

```
GitHub Webhook
      |
      v
Webhook Receiver (validates signature, quick response)
      |
      v
Primary Stream (NATS JetStream with deduplication)
  ├── github.events.org.{org_id}.{event_type}
  └── github.events.global.{event_type}
      |
      v
Event Dispatcher (extracts entities, parallel routing)
      |
      ├── github.events.repo.{repo_id}.{event_type}
      ├── github.events.user.{user_id}.{event_type}  
      ├── github.events.pr.{repo_id}.{pr_number}.{event_type}
      ├── github.events.pr.{repo_id}.{pr_number}
      ├── github.events.issue.{repo_id}.{issue_number}.{event_type}
      └── github.events.issue.{repo_id}.{issue_number}
```

## NATS JetStream Configuration

### Primary Stream: GITHUB_EVENTS_RAW

| Setting | Value | Purpose |
|---------|-------|----------|
| Subjects | `github.events.>` | Flexible depth for all routing patterns |
| Deduplication | 24-hour window | Prevents duplicate events using GitHub delivery ID |
| Retention | Configurable (default 24h) | How long to keep events in stream |
| Storage | File-based | Persistent storage for reliability |
| Replicas | Configurable | High availability in production |
| Max Consumers | Unlimited | Multiple applications can consume |

## Subject Naming Patterns

### Stage 1: Primary Subjects (Webhook Receiver)

Events are published to organization-based subjects using **numeric IDs**:

| Pattern | Example | When Used |
|---------|---------|-----------|  
| `github.events.org.{org_id}.{event_type}` | `github.events.org.12345678.push` | Organization-owned events |
| `github.events.global.{event_type}` | `github.events.global.sponsorship` | User-level events without org |

### Stage 2: Entity-Specific Subjects (Event Dispatcher)

The dispatcher extracts entities and publishes to multiple subjects:

#### Repository Events
| Pattern | Example | Description |
|---------|---------|-------------|
| `github.events.repo.{repo_id}.{event_type}` | `github.events.repo.87654321.push` | Specific event type for repo |
| `github.events.repo.{repo_id}.>` | `github.events.repo.87654321.>` | All events for repository |

#### User Events  
| Pattern | Example | Description |
|---------|---------|-------------|
| `github.events.user.{user_id}.{event_type}` | `github.events.user.11111111.pull_request` | Specific event type for user |
| `github.events.user.{user_id}.*` | `github.events.user.11111111.*` | All events involving user |

#### Pull Request Events
| Pattern | Example | Description |
|---------|---------|-------------|
| `github.events.pr.{repo_id}.{pr_number}.{event_type}` | `github.events.pr.87654321.456.pull_request_review` | Specific PR event |
| `github.events.pr.{repo_id}.{pr_number}` | `github.events.pr.87654321.456` | All events for PR |
| `github.events.pr.{repo_id}.*` | `github.events.pr.87654321.*` | All PRs in repository, one copy per event |

#### Issue Events
| Pattern | Example | Description |
|---------|---------|-------------|
| `github.events.issue.{repo_id}.{issue_number}.{event_type}` | `github.events.issue.87654321.123.issue_comment` | Specific issue event |
| `github.events.issue.{repo_id}.{issue_number}` | `github.events.issue.87654321.123` | All events for issue |
| `github.events.issue.{repo_id}.>` | `github.events.issue.87654321.>` | All issues in repository |

## Subscription Patterns

### By Scope

**Organization-wide**
```bash
# Everything in org 12345678
nats sub "github.events.org.12345678.>"

# Only pushes in org
nats sub "github.events.org.12345678.push"
```

**Repository-specific**
```bash  
# Everything in repo 87654321
nats sub "github.events.repo.87654321.>"

# Only releases
nats sub "github.events.repo.87654321.release"
```

**User activity**
```bash
# All activity by user 11111111
nats sub "github.events.user.11111111.*"

# Only their PRs
nats sub "github.events.user.11111111.pull_request"
```

**PR/Issue tracking**
```bash
# Watch PR #456
nats sub "github.events.pr.87654321.456"

# All PRs in repo
nats sub "github.events.pr.87654321.*"

# Issue #123 updates
nats sub "github.events.issue.87654321.123"
```

### By Event Type

```bash
# All pushes across all repos
nats sub "github.events.repo.*.push"

# All PR reviews
nats sub "github.events.repo.*.pull_request_review"

# All security advisories
nats sub "github.events.repo.*.security_advisory"
```

### Advanced Patterns

```bash
# Multiple event types for a repo
nats sub "github.events.repo.87654321.push" \
         "github.events.repo.87654321.pull_request" \
         "github.events.repo.87654321.release"

# All PR events across multiple repos
nats sub "github.events.pr.87654321.*" \
         "github.events.pr.87654322.>" \
         "github.events.pr.87654323.>"
```

## Design Benefits

### Rename-Proof Subscriptions
- Organizations, repos, and users can change names
- Numeric IDs remain constant
- No broken subscriptions or lost history
- Seamless handling of repository transfers

### Flexible Routing
- Subscribe at any granularity level
- Mix and match subscription patterns
- Easy to add new routing dimensions
- No need to modify webhook configuration

### Performance & Reliability  
- Webhook responds quickly (< 100ms)
- Complex routing happens asynchronously
- Built-in deduplication prevents double processing
- JetStream provides persistence and replay

### Operational Simplicity
- Single stream to backup and monitor
- Clear separation of concerns
- Easy to debug event flow
- Graceful degradation if dispatcher fails

## Example: Pull Request Event Flow

When user 11111111 opens PR #456 in repo 87654321 (org 12345678):

### 1. GitHub sends webhook
```json
{
  "action": "opened",
  "pull_request": {
    "id": 456,
    "user": {"id": 11111111},
    "base": {"repo": {"id": 87654321}}
  },
  "organization": {"id": 12345678}
}
```

### 2. Webhook receiver publishes
```
github.events.org.12345678.pull_request
```

### 3. Event dispatcher publishes to
```
github.events.repo.87654321.pull_request
github.events.user.11111111.pull_request  
github.events.pr.87654321.456.pull_request
github.events.pr.87654321.456
```

### 4. Consumers receive based on subscription
- **Org admin** subscribing to `org.12345678.>` sees it
- **Repo CI** subscribing to `repo.87654321.pull_request` triggers build
- **User dashboard** subscribing to `user.11111111.*` shows activity
- **PR bot** subscribing to `pr.87654321.456` starts processing

## Wildcard Reference

| Wildcard | Meaning | Example |
|----------|---------|---------|  
| `*` | Match exactly one token | `repo.*.push` matches `repo.123.push` |
| `>` | Match one or more tokens | `org.123.>` matches all org events |
| Multiple subjects | Subscribe to multiple patterns | See advanced patterns above |