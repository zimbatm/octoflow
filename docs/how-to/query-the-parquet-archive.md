# Query the Parquet archive

The s3 service writes one Parquet file per batch under
`{S3_KEY_PREFIX}/{year}/{month}/{day}/{unix nanoseconds}.parquet`,
zstd-compressed, one row per primary event. The columns are listed in
[Event format](../reference/event-format.md#parquet-row).

With `octoflow compact` there is also one file per day, under
`{S3_DAILY_PREFIX}/{YYYY-MM-DD}.parquet`, with the same columns. It is
the file to read when the operator publishes the archive: see
[The daily files over HTTPS](#the-daily-files-over-https).

## The batch files with DuckDB

```sql
INSTALL httpfs; LOAD httpfs;

-- AWS: credentials from the environment or the shared credentials file.
-- R2 or rustfs: set the endpoint.
SET s3_endpoint = '<account>.r2.cloudflarestorage.com';
SET s3_access_key_id = '...';
SET s3_secret_access_key = '...';
SET s3_url_style = 'path';

-- Everything from October 2026
SELECT event, action, repository, count(*) AS n
FROM read_parquet('s3://octoflow/github-events/2026/10/*/*.parquet')
GROUP BY ALL
ORDER BY n DESC;
```

The payload is a JSON string. Pull fields out of it with the JSON
functions:

```sql
SELECT received_at, sender,
       json_extract_string(payload, '$.ref') AS ref,
       json_array_length(json_extract(payload, '$.commits')) AS commits
FROM read_parquet('s3://octoflow/github-events/2026/10/*/*.parquet')
WHERE event = 'push'
ORDER BY received_at DESC
LIMIT 20;
```

`received_at` is a timestamp in UTC. For the local rustfs store from
`make run`, read the files directly:

```sql
SELECT * FROM read_parquet('.data/s3/octoflow/github-events/*/*/*/*.parquet');
```

## The daily files over HTTPS

When the operator [publishes a dataset](publish-a-public-dataset.md),
there is one file per UTC day at a fixed URL:

```
https://data.example.org/github-events-daily/2026-11-21.parquet
```

`https://data.example.org/github-events-daily` is a placeholder. Use the
base URL that the operator of your instance gives. A file holds every
event received on that day, sorted by `received_at`, one row per `id`.
The file for a day appears early on the next day, in UTC. An old file
can change: the operator can remove events from it.

A plain HTTPS server has no listing, so `*` does not work in the URL.
Name each day.

### With DuckDB

DuckDB reads only the columns and row groups that a query needs. The
first query below does not download the payloads.

```sql
INSTALL httpfs; LOAD httpfs;

-- Events per type for a day
SELECT event, count(*) AS n
FROM read_parquet('https://data.example.org/github-events-daily/2026-11-21.parquet')
GROUP BY event
ORDER BY n DESC;

-- The pull requests opened on a day
SELECT received_at, repository,
       json_extract_string(payload, '$.pull_request.number') AS number,
       json_extract_string(payload, '$.pull_request.user.login') AS author,
       json_extract_string(payload, '$.pull_request.title') AS title
FROM read_parquet('https://data.example.org/github-events-daily/2026-11-21.parquet')
WHERE event = 'pull_request' AND action = 'opened'
ORDER BY received_at;

-- The failed workflow runs of a day
SELECT received_at, repository,
       json_extract_string(payload, '$.workflow_run.name') AS workflow,
       json_extract_string(payload, '$.workflow_run.head_branch') AS branch,
       json_extract_string(payload, '$.workflow_run.html_url') AS url
FROM read_parquet('https://data.example.org/github-events-daily/2026-11-21.parquet')
WHERE event = 'workflow_run' AND action = 'completed'
  AND json_extract_string(payload, '$.workflow_run.conclusion') = 'failure'
ORDER BY received_at;
```

For more than one day, give a list:

```sql
SELECT received_at::DATE AS day, count(*) AS n
FROM read_parquet([
  'https://data.example.org/github-events-daily/' || strftime(d, '%Y-%m-%d') || '.parquet'
  FOR d IN range(DATE '2026-11-01', DATE '2026-11-22', INTERVAL 1 DAY)
])
GROUP BY day
ORDER BY day;
```

The end of `range` is not included. A day without a file fails the
query.

### With ClickHouse

`url()` reads one file, or a set of files with `{a,b}` and `{01..30}`
in the URL. `clickhouse local` runs the same queries without a server.

```sql
-- Events per type for a day
SELECT event, count() AS n
FROM url('https://data.example.org/github-events-daily/2026-11-21.parquet', 'Parquet')
GROUP BY event
ORDER BY n DESC;

-- The pull requests opened on a day
SELECT received_at, repository,
       JSONExtractUInt(payload, 'pull_request', 'number') AS number,
       JSONExtractString(payload, 'pull_request', 'user', 'login') AS author,
       JSONExtractString(payload, 'pull_request', 'title') AS title
FROM url('https://data.example.org/github-events-daily/2026-11-21.parquet', 'Parquet')
WHERE event = 'pull_request' AND action = 'opened'
ORDER BY received_at;

-- The failed workflow runs of a day
SELECT received_at, repository,
       JSONExtractString(payload, 'workflow_run', 'name') AS workflow,
       JSONExtractString(payload, 'workflow_run', 'head_branch') AS branch,
       JSONExtractString(payload, 'workflow_run', 'html_url') AS url
FROM url('https://data.example.org/github-events-daily/2026-11-21.parquet', 'Parquet')
WHERE event = 'workflow_run' AND action = 'completed'
  AND JSONExtractString(payload, 'workflow_run', 'conclusion') = 'failure'
ORDER BY received_at;

-- Events per day, 19 to 21 November
SELECT toDate(received_at) AS day, count() AS n
FROM url('https://data.example.org/github-events-daily/2026-11-{19..21}.parquet', 'Parquet')
GROUP BY day
ORDER BY day;
```

`s3()` reads the bucket directly and accepts `*`, but it must list the
bucket. Use it with credentials, or when the operator allows the
listing:

```sql
SELECT toDate(received_at) AS day, count() AS n
FROM s3('https://<endpoint>/<bucket>/github-events-daily/2026-11-*.parquet', '<key id>', '<secret>', 'Parquet')
GROUP BY day
ORDER BY day;
```

ClickHouse reads every daily file. It refuses the batch files that an
Octoflow older than the `compact` command wrote, with `Page size out of
bounds`: their page index is wrong. DuckDB and pyarrow read them all.

### With Python

pandas and pyarrow read the URL through fsspec:
`pip install pandas pyarrow fsspec aiohttp`. `read_parquet` loads the
whole day, payloads included, so a busy day needs a few GB of memory.
Give `columns=[...]` to load less.

```python
import json

import pandas as pd

url = "https://data.example.org/github-events-daily/2026-11-21.parquet"
df = pd.read_parquet(url)

# Events per type for a day
print(df["event"].value_counts())

# The pull requests opened on a day
opened = df[(df["event"] == "pull_request") & (df["action"] == "opened")]
prs = pd.json_normalize(opened["payload"].map(json.loads).tolist())
print(prs[["pull_request.number", "pull_request.user.login", "pull_request.title"]])

# The failed workflow runs of a day
runs = df[(df["event"] == "workflow_run") & (df["action"] == "completed")]
runs = pd.json_normalize(runs["payload"].map(json.loads).tolist())
failed = runs[runs["workflow_run.conclusion"] == "failure"]
print(failed[["workflow_run.name", "workflow_run.head_branch", "workflow_run.html_url"]])
```

With pyarrow alone, for some columns:

```python
import fsspec
import pyarrow.parquet as pq

with fsspec.open(url) as f:
    table = pq.read_table(f, columns=["event", "action", "repository"])
print(table.group_by("event").aggregate([("event", "count")]))
```

## With other tools

Athena, Spark, Polars and pandas read the same layout; partition by the
path's year, month and day. Each file holds at most 1000 events (the
batch size) and at most `S3_BATCH_TIMEOUT` (one hour) of them, so a busy day has many
small files. `octoflow compact` makes one file per day of them, see
[Publish a public dataset](publish-a-public-dataset.md).

## Retention

Octoflow never deletes from the bucket. Set a lifecycle rule on the
bucket for the retention you want.
