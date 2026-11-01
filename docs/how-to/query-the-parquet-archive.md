# Query the Parquet archive

The s3 service writes one Parquet file per batch under
`{S3_KEY_PREFIX}/{year}/{month}/{day}/{unix nanoseconds}.parquet`,
Snappy-compressed, one row per primary event. The columns are listed in
[Event format](../reference/event-format.md#parquet-row).

## With DuckDB

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

## With other tools

Athena, Spark, Polars and pandas read the same layout; partition by the
path's year, month and day. Each file holds at most 1000 events (the
batch size) and at most five minutes of them, so a busy day has many
small files. Compact them if a query engine struggles; Octoflow does not
rewrite files it has uploaded.

## Retention

Octoflow never deletes from the bucket. Set a lifecycle rule on the
bucket for the retention you want.
