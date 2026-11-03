# Wipe events

`octoflow wipe` deletes selected events from the stream, fan-out copies
included, and from the archive. Use it when a delivery turns out to
carry something that must not stay: a secret pasted in a comment, a
security advisory, a payload from an event type the hook should not
have sent.

The command cannot recall what already left: SSE clients that streamed
the event, consumers that acked it, and copies of the archive made
before the wipe. Tell those downstream yourself.

## 1. Point it at the stream and the archive

The command reads the NATS settings and, when `S3_BUCKET` is set, the
s3 settings. On a NixOS host run it as root with the s3 unit's
environment file sourced, so the archive is wiped in the same run;
without `S3_BUCKET` it wipes the stream and says the archive was
skipped.

## 2. Select the events

Every selector given must match. At least one is required.

| Flag | Selects |
|---|---|
| `--id a,b` | delivery ids, the `X-GitHub-Delivery` header, shown as `id` in the SSE stream and in GitHub's "Recent Deliveries" |
| `--event a,b` | event types |
| `--repo ID` | a repository, by numeric id |
| `--org ID` | an organization, by numeric id |
| `--user ID` | a user involved in the event: sender, author, assignee, reviewer, by numeric id |
| `--pr N`, `--issue N` | a pull request or issue number; combine with `--repo` |
| `--from DATE`, `--to DATE` | UTC days, inclusive, by `received_at`; `--to` needs `--from` |

## 3. Count first

```bash
octoflow wipe --repo 4542716 --pr 450123 --dry-run
```

```
wipe stream: scanned=38120 matched=14 events=2 deleted=0
wipe archive: files=48 rows=38012 matched=2 rewritten=0 removed=0
```

`matched` on the stream counts messages: a primary event and each of
its copies. `events` is the number of distinct deliveries. Without
`--from` the whole archive is read, which takes a while on a large
bucket; with it only the days `--from` to `--to` plus one are read,
because a file is named after its upload time and holds events received
up to a day before.

## 4. Wipe

```bash
octoflow wipe --repo 4542716 --pr 450123
```

```
wipe stream: scanned=38120 matched=14 events=2 deleted=14
wipe archive: files=48 rows=38012 matched=2 rewritten=1 removed=0
```

The stream deletion is JetStream's secure delete: the server overwrites
the message on disk. An archive file that held a selected event is
written back without it, under the same key; a file left without rows
is removed. Both happen in place, so if the bucket keeps versions, the
old version still exists and has to be purged in the bucket.

The command stops at the first error and prints the counts so far;
rerun it to continue, the already deleted events no longer match.

## What stays

- The stream remembers the delivery id for 24 hours, so a GitHub
  "Redeliver" inside that window is refused as a duplicate. After it,
  a redelivery stores the event again.
- `octoflow restore` republishes whatever is still in the archive. Wipe
  the archive in the same run, or before a restore.
- The journal holds no payloads; at `LOG_LEVEL=debug` it has the
  delivery id, event type, repository and sender of every event.
- Metrics count the event type and organization, nothing else.
