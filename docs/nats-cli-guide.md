# NATS CLI Guide for Octoflow

The `nats` CLI is in the devshell. These commands work in scripts and
agents, where nothing can answer a prompt.

## The stream

```bash
nats stream ls
nats stream info GITHUB_EVENTS_RAW
nats stream info GITHUB_EVENTS_RAW -j | jq '.state'
nats stream report
```

One stream holds everything: the primary subjects
`github.events.org.{org_id}.{type}` and `github.events.global.{type}`,
and the dispatcher's copies under `repo`, `user`, `pr` and `issue`.

## Reading messages

`nats stream view` needs a terminal. Use a subscription or a consumer:

```bash
# Live, once per event
nats sub "github.events.org.>"

# The last message on each subject
nats sub "github.events.repo.*.push" --last-per-subject

# Fetch five stored messages without a terminal
nats consumer create GITHUB_EVENTS_RAW viewer --deliver=all --ack=none --defaults
nats consumer next GITHUB_EVENTS_RAW viewer --count=5
nats consumer rm GITHUB_EVENTS_RAW viewer -f
```

`--defaults` and `-f` skip the prompts.

## Consumers

```bash
nats consumer ls GITHUB_EVENTS_RAW
nats consumer info GITHUB_EVENTS_RAW event-dispatcher
nats consumer info GITHUB_EVENTS_RAW s3-worker
```

`event-dispatcher` and `s3-worker` are the durable consumers of the
services. `sse-*` consumers belong to open SSE connections; the server
removes them a minute after the client leaves.

`NumPending` on `event-dispatcher` is the backlog the dispatcher still has
to fan out; on `s3-worker` it is what is not yet in S3.

## Publishing test messages

The services read `pkg/events.GitHubEvent` JSON; anything else is
terminated by the dispatcher with an error in its log. Post through the
webhook receiver instead, so the message is well-formed and signed:

```bash
WEBHOOK_SECRET=... ./scripts/test-webhook.sh
```

## Purging and deleting

```bash
nats stream purge GITHUB_EVENTS_RAW -f   # drop all messages, keep consumers
nats stream rm GITHUB_EVENTS_RAW -f      # drop the stream; services recreate it
```

`scripts/clear-streams.sh` deletes every stream after a confirmation.

## Context

```bash
nats context add local --server=nats://localhost:4222 --description="Local NATS"
nats context select local
```
