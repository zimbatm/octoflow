# Event format

## JSON message

Every message in the stream, and every SSE `data` field, is one
`pkg/events.GitHubEvent`:

```json
{
  "id": "3fa3a85c-6a48-11f0-8793-19d640392600",
  "event": "pull_request",
  "action": "opened",
  "hook_id": "12345",
  "hook_installation": "67890",
  "received_at": "2026-10-14T12:00:00.123456Z",
  "repository": "NixOS/nixpkgs",
  "repository_id": "4542716",
  "organization": "NixOS",
  "organization_id": "487568",
  "sender": "zimbatm",
  "sender_id": "2",
  "payload": { "action": "opened", "number": 450000, "pull_request": { "...": "..." } },
  "metadata": { "source": "events-api", "github_event_id": "53000000001", "created_at": "2026-10-14T11:59:58Z" }
}
```

| Field | Type | Source | Present |
|---|---|---|---|
| `id` | string | `X-GitHub-Delivery`, or `gh-event-<id>` from public-ingest | always |
| `event` | string | `X-GitHub-Event` | always |
| `action` | string | `payload.action` | when the payload has one |
| `hook_id` | string | `X-GitHub-Hook-ID` | webhook deliveries |
| `hook_installation` | string | `X-GitHub-Hook-Installation-Target-ID` | webhook deliveries |
| `received_at` | RFC 3339, UTC | when Octoflow read it | always |
| `repository` | string | `payload.repository.full_name` | when the payload has a repository |
| `repository_id` | string | `payload.repository.id` | same |
| `organization` | string | `payload.organization.login`, else `payload.repository.owner.login` | when either exists |
| `organization_id` | string | `payload.organization.id`, else `payload.repository.owner.id` | same |
| `sender` | string | `payload.sender.login` | when the payload has a sender |
| `sender_id` | string | `payload.sender.id` | same |
| `payload` | object | the GitHub payload, unchanged | always |
| `metadata` | map of strings | public-ingest only: `source` (`events-api` or `gharchive`), `github_event_id`, `created_at` | public-ingest events |

Ids are strings so that 64-bit values survive every JSON parser. Absent
string fields are omitted. Public-ingest payloads gain `repository`,
`sender` and `organization` objects in the webhook shape before parsing,
so the routing fields are the same for both sources.

## Parquet row

The s3 service writes one row per primary event with these columns, in
this order:

| Column | Parquet type | From |
|---|---|---|
| `id` | UTF8 | `id` |
| `event` | UTF8 | `event` |
| `hook_id` | UTF8 | `hook_id` |
| `hook_installation` | UTF8 | `hook_installation` |
| `received_at` | INT64, TIMESTAMP_MILLIS | `received_at` |
| `payload` | UTF8 | `payload`, as JSON text |
| `repository` | UTF8 | `repository` |
| `organization` | UTF8 | `organization` |
| `sender` | UTF8 | `sender` |
| `action` | UTF8 | `action` |
| `organization_id` | UTF8 | `organization_id` |
| `repository_id` | UTF8 | `repository_id` |
| `sender_id` | UTF8 | `sender_id` |

`metadata` is not stored in Parquet. Files are Snappy-compressed with
row groups of 10000 and named
`{S3_KEY_PREFIX}/{YYYY}/{MM}/{DD}/{unix nanoseconds}.parquet`, by the
upload time in UTC.

## SSE framing

```
retry: 5000

event: connected
data: {"status":"connected","stream_id":"repo-111","subjects":"github.events.repo.111.>"}

id: 1842
event: pull_request
data: <the JSON message above on one line>

: heartbeat
```

`id` is the JetStream stream sequence of the message; `event` is the
GitHub event type; `data` is the message. `: heartbeat` comes every 30
seconds while nothing else does. An `event: error` ends the stream.
