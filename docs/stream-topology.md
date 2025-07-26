# GitHub Events Stream Topology

## Overview

The system uses a two-stage event processing architecture:
1. **Primary Stream**: Webhook events published by organization ID
2. **Entity Routing**: Event dispatcher republishes to entity-specific subjects

## Architecture

```
GitHub Webhook
      |
      v
Webhook Receiver
      |
      v
Primary Stream: github.events.org.{org_id}.{event_type}
                github.events.global.{event_type}
      |
      v
Event Dispatcher
      |
      +---> github.events.repo.{repo_id}.{event_type}
      +---> github.events.user.{user_id}.{event_type}
      +---> github.events.user.{user_id}.activity
      +---> github.events.pr.{repo_id}.{pr_number}.{event_type}
      +---> github.events.pr.{repo_id}.{pr_number}
      +---> github.events.issue.{repo_id}.{issue_number}.{event_type}
      +---> github.events.issue.{repo_id}.{issue_number}
```

## Stream Configuration

### GITHUB_EVENTS_RAW Stream
- **Purpose**: Store all GitHub webhook events with deduplication
- **Subjects**: `github.events.>` (flexible depth for all patterns)
- **Deduplication**: 24-hour window based on GitHub delivery ID
- **Retention**: Configured via NATS_STREAM_MAX_AGE (default 24h)
- **Max Consumers**: Unlimited

## Subject Patterns

### Primary Subjects (from Webhook)

Events are initially published to organization-based subjects using numeric IDs:

```
github.events.org.{organization_id}.{event_type}
github.events.global.{event_type}
```

Examples:
- `github.events.org.12345678.push`
- `github.events.org.12345678.pull_request`
- `github.events.global.sponsorship`

### Entity-Specific Subjects (from Dispatcher)

The event dispatcher extracts entities and republishes to:

#### Repository Events
```
github.events.repo.{repository_id}.{event_type}
```
- Example: `github.events.repo.87654321.push`

#### User Events
```
github.events.user.{user_id}.{event_type}
github.events.user.{user_id}.activity
```
- Example: `github.events.user.11111111.pull_request`
- Example: `github.events.user.11111111.activity` (all events)

#### Pull Request Events
```
github.events.pr.{repository_id}.{pr_number}.{event_type}
github.events.pr.{repository_id}.{pr_number}
```
- Example: `github.events.pr.87654321.456.pull_request_review`
- Example: `github.events.pr.87654321.456` (all PR events)

#### Issue Events
```
github.events.issue.{repository_id}.{issue_number}.{event_type}
github.events.issue.{repository_id}.{issue_number}
```
- Example: `github.events.issue.87654321.123.issue_comment`
- Example: `github.events.issue.87654321.123` (all issue events)

## Consumer Patterns

### Organization-wide Monitoring
```bash
# All events for organization 12345678
nats sub "github.events.org.12345678.>"
```

### Repository-specific Monitoring
```bash
# All events for repository 87654321
nats sub "github.events.repo.87654321.>"

# Only push events for repository
nats sub "github.events.repo.87654321.push"
```

### User Activity Tracking
```bash
# All activity for user 11111111
nats sub "github.events.user.11111111.activity"

# Only PR events for user
nats sub "github.events.user.11111111.pull_request"
```

### PR/Issue Tracking
```bash
# All events for PR #456
nats sub "github.events.pr.87654321.456"

# All events for issue #123
nats sub "github.events.issue.87654321.123"

# All PRs in repository
nats sub "github.events.pr.87654321.>"
```

### Event Type Filtering
```bash
# All push events across all repos
nats sub "github.events.repo.*.push"

# All PR reviews across all repos
nats sub "github.events.repo.*.pull_request_review"
```

## Benefits

1. **Rename-Proof**: Using numeric IDs prevents broken subscriptions when entities are renamed
2. **Flexible Routing**: Subscribe by organization, repository, user, PR, or issue
3. **Deduplication**: NATS JetStream prevents duplicate events
4. **Multi-Dimensional**: Same event accessible through multiple subscription patterns
5. **Performance**: Initial routing is simple; complex routing handled asynchronously

## Event Flow Example

When GitHub sends a `pull_request` event:

1. **Webhook receives event** with:
   - Organization ID: 12345678
   - Repository ID: 87654321  
   - PR Number: 456
   - User IDs: [11111111 (author), 22222222 (reviewer)]

2. **Webhook publishes to**:
   - `github.events.org.12345678.pull_request`

3. **Dispatcher receives event and publishes to**:
   - `github.events.repo.87654321.pull_request`
   - `github.events.user.11111111.pull_request`
   - `github.events.user.11111111.activity`
   - `github.events.user.22222222.pull_request`
   - `github.events.user.22222222.activity`
   - `github.events.pr.87654321.456.pull_request`
   - `github.events.pr.87654321.456`

This allows consumers to subscribe at any level of granularity based on their needs.