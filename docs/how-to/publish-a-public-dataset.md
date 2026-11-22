# Publish a public dataset

The s3 service writes about 170 small Parquet files a day for a busy
repository. `octoflow compact` makes one file per day of them, at a key
that does not change. DuckDB, ClickHouse and pandas read that file
directly over HTTPS. This page is for the operator; the reader's side is
in [Query the Parquet archive](query-the-parquet-archive.md#the-daily-files-over-https).

Octoflow never makes a bucket public. You do that step yourself, and
only for the prefix you choose.

## What `compact` writes

One file per UTC day:

```
{S3_DAILY_PREFIX}/{YYYY-MM-DD}.parquet
github-events-daily/2026-11-21.parquet
```

| Property | Value |
|---|---|
| Rows | every event received on that day, by `received_at` |
| Order | `received_at`, then `id` |
| Duplicates | one row per `id`; the earliest stays |
| Columns | the same as a batch file, see [Event format](../reference/event-format.md#parquet-row) |
| Row groups | about 64 MiB of payload each, about 5000 events |
| Source | the batch files uploaded on that day and on the next one |

`S3_DAILY_PREFIX` is `{S3_KEY_PREFIX}-daily` by default. It must not be
inside the batch prefix, and the batch prefix must not be inside it.
`restore` and `wipe` list the batch prefix, and must not get a daily
file. The command refuses to start otherwise.

The batch files stay. `restore` reads only them.

One row per `id` holds inside one day. GitHub can redeliver an event on
a later day; that day then has a row with the same `id`.

## 1. Write the first files

The command reads the same settings as the s3 service. It does not need
NATS.

```bash
octoflow compact --from 2026-11-01 --dry-run
octoflow compact --from 2026-11-01
```

```
compact 2026-11-20: files=171 rows=164810 events=164795 duplicates=15 daily=written key=github-events-daily/2026-11-20.parquet
compact 2026-11-21: files=168 rows=165230 events=165230 duplicates=0 daily=written key=github-events-daily/2026-11-21.parquet
```

| Flag | Selects |
|---|---|
| none | yesterday, in UTC |
| `--day DATE` | one day; today is allowed and gives a partial file |
| `--from DATE`, `--to DATE` | a range, inclusive; `--to` is yesterday by default |
| `--dry-run` | reads and counts, writes nothing |

`files` counts the batch files read. `rows` counts their rows of that
day. `events` counts the rows written. `daily` is `written`, `removed`,
`none` or `dry-run`.

A run for a day that has a file writes the file again. The result
depends only on the batch files: the same batch files give the same
bytes. A late batch or a wipe gives the new state. A day without events
writes nothing; if a daily file was there, the command removes it.

The command does not hold a day in memory. A day with 2.2 GB of payloads
in 170 batch files took 13 seconds and 650 MB against a local store.
With `GOMEMLIMIT=400MiB` it took 49 seconds and 250 MB. The need follows
the size of the batch files, not the size of the day.

## 2. Run it every day

On NixOS:

```nix
services.octoflow.s3.daily.enable = true;
```

This adds `octoflow-compact.timer`, at 03:00 UTC, and
`octoflow-compact.service`, which compacts yesterday. See the
[NixOS module](../reference/nixos-module.md). On other systems, run
`octoflow compact` from cron after 01:00 UTC: the s3 service uploads the
last events of a day up to `S3_BATCH_TIMEOUT` after midnight.

The timer compacts one day. After a stop of more than a day, run
`octoflow compact --from DATE` for the days it did not do.

## 3. Make the daily prefix readable

Readers need `GET` with `Range` on the daily files, and nothing else.
There are two ways.

**Public read on the daily prefix.** Stores with bucket policies (AWS S3
and most compatible stores) take a policy for one prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::octoflow/github-events-daily/*"
  }]
}
```

Do not allow `s3:ListBucket` to everyone unless you want the batch keys
to be visible too. Without it a reader cannot list the days, so publish
the first day of the dataset next to the URL.

**A CDN or a proxy in front.** Use this when the store has no policy
for a prefix (Cloudflare R2 makes a whole bucket public), or when you
want your own domain. Forward only the daily prefix. Check these points:

- `Range` requests pass. DuckDB and ClickHouse read the footer and then
  the columns they need.
- The cache time is short, or you purge the cache after a wipe and after
  a new compaction of an old day. A daily file can change.
- CORS allows `GET` and the `Range` header if browsers read the files.

The batch prefix holds the same events as the daily prefix. Both are
public GitHub activity. Keep the batch prefix private only if you want
to: it is not a secret, but nobody needs it to read the dataset.

## 4. Know what a wipe does

`octoflow wipe` removes the selected events from the stream, from the
batch files and from the daily files, in that order. See
[Wipe events](wipe-events.md). For a public dataset:

- Run the wipe with the `S3_KEY_PREFIX` and `S3_DAILY_PREFIX` of the
  compaction. With other values it does not find the daily files, and
  `wipe daily: files=0` tells you so.
- A daily file with a selected event is written again without it, from
  its own rows. A file left without rows is removed.
- The command reads the file again after it wrote it. If a selected
  event is still there, the command fails with exit status 1. This
  occurs when a compaction ran at the same time. Run the wipe again.
- Copies that readers already downloaded stay with them. A CDN keeps
  its copy until you purge it. A bucket with versions keeps the old
  version until you delete it.
