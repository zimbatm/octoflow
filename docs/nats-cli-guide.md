# NATS CLI Guide for Octoflow

This guide documents the correct NATS CLI commands for interacting with JetStream streams in non-interactive environments.

## Common Issues and Solutions

### 1. Viewing Stream Messages

❌ **Incorrect** (requires interactive terminal):
```bash
nats stream view GITHUB_EVENTS_RAW --last 5
nats stream view GITHUB_EVENTS_RAW 5
```

✅ **Correct alternatives**:

```bash
# Get stream info as JSON
nats stream info GITHUB_EVENTS_RAW -j

# Get stream state
nats stream info GITHUB_EVENTS_RAW -j | jq '.state'

# Use nats sub to view recent messages
nats sub "github.events.raw.>" --last-per-subject=5

# Create a consumer and fetch messages programmatically
nats consumer create GITHUB_EVENTS_RAW viewer --deliver=last --ack=none --defaults
nats consumer next GITHUB_EVENTS_RAW viewer --count=5
```

### 2. Creating Consumers

❌ **Incorrect** (prompts for input):
```bash
nats consumer create GITHUB_EVENTS_RAW temp-reader --deliver=all --ack=none
```

✅ **Correct** (use --defaults to avoid prompts):
```bash
nats consumer create GITHUB_EVENTS_RAW temp-reader --deliver=all --ack=none --defaults
```

### 3. Useful NATS Commands for Octoflow

#### List all streams
```bash
nats stream ls
```

#### Get detailed stream info
```bash
nats stream info GITHUB_EVENTS_RAW
```

#### Purge streams (remove all messages)
```bash
nats stream purge GITHUB_EVENTS_RAW -f
```

#### Delete streams entirely
```bash
nats stream rm GITHUB_EVENTS_RAW -f
```

#### Monitor stream activity
```bash
nats stream report
```

#### List consumers
```bash
nats consumer ls GITHUB_EVENTS_RAW
```

#### Get consumer info
```bash
nats consumer info GITHUB_EVENTS_RAW webhook-receiver
```

#### Subscribe to events in real-time
```bash
# Raw events
nats sub "github.events.raw.>"

# Ordered events
nats sub "github.events.ordered.>"

# Specific event types
nats sub "github.events.raw.push"
nats sub "github.events.ordered.pull_request"
```

#### Publish test messages
```bash
echo '{"test": "data"}' | nats pub github.events.raw.test
```

## Tips for Non-Interactive Environments

1. Always use `-f` or `--force` flags to skip confirmations
2. Use `--defaults` when creating consumers to avoid prompts
3. Use `-j` for JSON output that can be parsed with `jq`
4. Avoid commands that require terminal interaction (like `nats stream view`)
5. Use `nats sub` with filters for viewing messages instead of stream view

## Debugging Stream Issues

```bash
# Check stream configuration
nats stream info GITHUB_EVENTS_RAW -j | jq '.config'

# Check for subject overlaps
nats stream ls -j | jq '.[] | {name: .name, subjects: .config.subjects}'

# Monitor message flow
nats events

# Check server info
nats server info
```

## NATS Context Setup

If you haven't already, set up a NATS context for easier CLI usage:

```bash
nats context add local --server=nats://localhost:4222 --description="Local NATS"
nats context select local
```

This avoids having to specify `--server` for every command.