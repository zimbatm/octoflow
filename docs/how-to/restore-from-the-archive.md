# Restore events from the archive

`octoflow restore` reads the Parquet files the s3 service wrote and
publishes the events back into the stream, on their primary subjects
and with their original ids. The dispatcher fans them out again as if
they had just arrived. Use it after the stream was lost, or to feed a
new consumer events older than the stream's seven days.

## 1. Point it at the archive and the stream

The command reads the same settings as the s3 service (`S3_BUCKET`,
`S3_ENDPOINT`, `AWS_REGION`, credentials, `S3_KEY_PREFIX`) and the NATS
settings. On a NixOS host, source the s3 unit's environment file; on a
laptop, `.env` already has them.

## 2. Count first

```bash
octoflow restore --from 2026-10-01 --to 2026-10-14 --dry-run
```

```
restore: files=312 rows=48112 matched=48112 in_stream=0 published=0 duplicates=0
```

`--from` and `--to` are UTC dates, inclusive, matched against each
event's `received_at`. The command also reads the day after `--to`,
because a file is named after its upload time and holds events
received before it.

## 3. Narrow it down, if you want

```bash
octoflow restore --from 2026-10-01 --event pull_request,issues --repo 4542716
octoflow restore --from 2026-10-01 --org 487568
```

## 4. Restore

```bash
octoflow restore --from 2026-10-01 --to 2026-10-14
```

```
restore: files=312 rows=48112 matched=48112 in_stream=1204 published=46786 duplicates=122
```

The command reads the oldest event the stream still holds and skips
every archived event received at or after it, counted as `in_stream`:
the stream keeps events for seven days but only remembers their ids for
24 hours, so republishing them would store them twice. Events the
stream dropped since are republished; events inside the 24 hour window
are dropped by the stream and counted as `duplicates`. `--force` turns
the skip off, for a stream that lost part of its range. A dry run does
not connect to NATS and reports `in_stream=0`.

Order within a file is preserved and files are read in name order, so
events come back in the order they were archived. The command stops at
the first error and prints the counts so far; rerun it to continue, the
duplicate window takes care of what was already published.

## 5. Check the consumers

Durable consumers created with `DeliverNew` receive the restored events
like any new ones. The s3 service receives them too and archives them
again; the new files repeat the old ones. That is expected after a
stream loss; for a reprocessing run, stop the s3 service first if you
do not want the archive to grow.

## What comes back

Every field of the event, including `metadata` for public-ingest events.
Fan-out copies are not archived; the dispatcher
recreates them from the restored primary events.
