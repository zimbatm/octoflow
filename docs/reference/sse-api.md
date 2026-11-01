# SSE API

Served by `octoflow sse` on `HTTP_LISTEN_ADDR` (`:8080`), next to the
webhook and the dashboard, or on `SSE_LISTEN_ADDR` when set. Machine-readable
version: [sse-openapi.yaml](sse-openapi.yaml).

## Routes

All routes answer `GET` with `text/event-stream` and
`Access-Control-Allow-Origin: *`. `OPTIONS` answers 204.

| Route | Subjects read |
|---|---|
| `/events` | `org.>`, `global.>` (every event once) |
| `/events/org/{org_id}` | `org.{org_id}.>` |
| `/events/repo/{repo_id}` | `repo.{repo_id}.>` |
| `/events/user/{user_id}` | `user.{user_id}.*` |
| `/events/pr/{repo_id}/{number}` | `pr.{repo_id}.{number}.>` |
| `/events/issue/{repo_id}/{number}` | `issue.{repo_id}.{number}.>` |
| `/events/type/{event_type}` | `org.*.{event_type}`, `global.{event_type}` |

Path ids must match `[0-9]+`, `{event_type}` must match `[a-z_]+`;
anything else is a 404.

| Route | Answers |
|---|---|
| `/stats` | JSON: `{"total_clients": N}`, the open connections of this process |
| `/health` | `{"status":"healthy"}`; shared by every service on the address |

## Query parameters

| Parameter | Meaning |
|---|---|
| `last=N` | replay the last N events matching the route before new ones; default 50, maximum 1000, `0` for none; ignored when `Last-Event-ID` is sent |
| `type=a,b` | keep only these GitHub event types |
| `action=x,y` | keep only these actions |

## Headers

| Header | Meaning |
|---|---|
| `Last-Event-ID: <sequence>` | resume after this stream sequence; browsers send it on reconnect. An id older than the last 1000 matching events resumes at those 1000 |

## Stream contents

See [Event format, SSE framing](event-format.md#sse-framing). In short:
`retry: 5000`, a `connected` event, then one event per GitHub event
named after its type with the stream sequence as `id`, a `: heartbeat`
comment every 30 seconds, and an `error` event if the server loses
JetStream.

## Behaviour

- At most `SSE_MAX_CLIENTS` (1000) streams per process; beyond that a
  request gets a 503.
- Each connection gets its own JetStream consumer, deleted when the
  connection ends and in any case one minute after it goes idle.
- `last=N` counts only messages that match the route's subjects, found
  by a binary search over the stream; the stream also holds other
  subjects between them.
- Filters apply after replay counting: `last=10&type=push` replays at
  most 10 events of the route and shows the pushes among them.
