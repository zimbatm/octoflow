# Inspect the stream with the NATS CLI

The `nats` CLI is in the development shell. The commands below need no
terminal interaction, so they also work from scripts.

## Point the CLI at the server

```bash
nats context add local --server=nats://localhost:4222
nats context select local
```

## See what is stored

```bash
nats stream info GITHUB_EVENTS_RAW
nats stream info GITHUB_EVENTS_RAW -j | jq '.state'
nats stream report
```

One stream holds primary events (`github.events.org.>`,
`github.events.global.>`) and the dispatcher's copies under `repo`,
`user`, `pr` and `issue`, so `Messages` counts each event several
times.

## Read messages

`nats stream view` needs a terminal. Subscribe, or fetch through a
throwaway consumer:

```bash
# Live, once per event
nats sub "github.events.org.>"

# The last message on each matching subject
nats sub "github.events.repo.*.push" --last-per-subject

# Five stored messages, oldest first
nats consumer create GITHUB_EVENTS_RAW viewer --deliver=all --ack=none --defaults
nats consumer next GITHUB_EVENTS_RAW viewer --count=5
nats consumer rm GITHUB_EVENTS_RAW viewer -f
```

## Check the services' backlogs

```bash
nats consumer ls GITHUB_EVENTS_RAW
nats consumer info GITHUB_EVENTS_RAW event-dispatcher
nats consumer info GITHUB_EVENTS_RAW s3-worker
```

`Unprocessed Messages` on `event-dispatcher` is what the dispatcher has
not fanned out yet; on `s3-worker`, what is not in S3 yet. `Ack
Pending` on `s3-worker` is normal up to twice the batch size: events
stay unacknowledged until their batch is uploaded. `sse-*` consumers
belong to open SSE connections and disappear a minute after the client
leaves.

## Send a test event

Do not `nats pub` into the stream: the services expect
`pkg/events.GitHubEvent` JSON and terminate anything else. Post a signed
delivery through the receiver instead:

```bash
WEBHOOK_SECRET=... ./scripts/test-webhook.sh
```

## Purge or reset

```bash
nats stream purge GITHUB_EVENTS_RAW -f   # drop the messages, keep the consumers
nats stream rm GITHUB_EVENTS_RAW -f      # drop the stream; the services recreate it on start
```

`scripts/clear-streams.sh` deletes every stream after asking. Deleting
the stream also deletes the durable consumers, so the dispatcher and the
s3 service start from the beginning of the new stream.
