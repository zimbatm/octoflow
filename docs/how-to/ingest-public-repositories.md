# Ingest repositories without a webhook

For a repository you do not administer, such as `NixOS/nixpkgs`, the
`public-ingest` service reads the public GitHub Events API and the
hourly GH Archive files and publishes them on the same subjects as the
webhook receiver. Both sources miss events; read
[Public events](../explanation/public-events.md) before relying on
them.

## 1. Choose the repositories

```bash
PUBLIC_REPOS=NixOS/nixpkgs,NixOS/nix
```

Do not list a repository that also delivers webhooks: the two sources
use different message ids, so every event would arrive twice.

## 2. Add a token

Without a token the Events API allows 60 requests per hour, and one
poll can take 3. With a token it allows 5000.

```bash
GITHUB_TOKEN=github_pat_...
```

A fine-grained token with no permissions is enough; public events need
none. When the quota runs out the poller waits for the reset and says
so in its log; it does not fail.

## 3. Decide where GH Archive starts

GH Archive publishes one file per hour, about ten minutes after the
hour. On the first start the importer begins three hours back, because
the stream deduplicates for 24 hours only and older hours would be
stored again next to what the Events API delivered. To backfill:

```bash
GHARCHIVE_START=2026-10-14T00:00:00Z
```

The importer keeps its position in the NATS KV bucket
`octoflow_public_ingest`, so a restart continues where it stopped. Delete
the key `gharchive.next_hour` from that bucket to start again from
`GHARCHIVE_START`.

## 4. Run it

```bash
octoflow public-ingest
```

or on NixOS, `services.octoflow.publicIngest.repos`. The log shows one
line per poll and per imported hour:

```
msg="Events API poll" repo=NixOS/nixpkgs fetched=57 published=12
msg="GH Archive hour imported" hour=2026-10-14T09:00:00Z published=6
```

`octoflow_public_ingest_polls_total{status=...}` counts polls by
outcome: `ok`, `not_modified`, `rate_limited`, `error`.

## 5. Tell the sources apart

Public events carry `metadata.source` (`events-api` or `gharchive`),
`metadata.github_event_id` and `metadata.created_at`, the time GitHub
recorded the event. Their `id` is `gh-event-<github event id>`, the same
from both sources, so the stream drops an event that both deliver within
24 hours.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `PUBLIC_REPOS` | required | comma-separated `owner/name` |
| `GITHUB_TOKEN` | none | raises the Events API quota |
| `EVENTS_API_ENABLED` | `true` | |
| `EVENTS_API_INTERVAL` | `1m` | GitHub's `X-Poll-Interval` wins when longer |
| `GHARCHIVE_ENABLED` | `true` | |
| `GHARCHIVE_URL` | `https://data.gharchive.org` | |
| `GHARCHIVE_START` | three hours ago | first hour when no position is saved |
