# SSE API

Served by `octoflow sse` on `HTTP_LISTEN_ADDR` (`:8080`), next to the
webhook and the dashboard, or on `SSE_LISTEN_ADDR` when set. Machine-readable
version: [sse-openapi.yaml](sse-openapi.yaml).

## Routes

All routes answer `GET` with `text/event-stream` and
`Access-Control-Allow-Origin: *`. `OPTIONS` answers 204.

| Route | Subjects read |
|---|---|
| `/events` | `org.*.>`, `global.>` (every event once) |
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
| `/stats` | JSON: `total_clients`, the open connections of this process; `events_stored`, the events in the stream without the fan-out copies, refreshed every 30 seconds; `stored_since`, the store time of the oldest message. The last two are absent when the stream does not answer |
| `/site` | JSON: `title`, `intro` and `streams` (`label`, `path`) of this deployment, from `SITE_TITLE`, `SITE_INTRO` and `SITE_STREAMS`; the landing page reads it |
| `/health` | `{"status":"healthy"}`; shared by every service on the address |

## Query parameters

| Parameter | Meaning |
|---|---|
| `last=N` | replay the last N events of the route and of `type`, then new ones; default 50, maximum 1000, `0` for none; ignored when `Last-Event-ID` is sent |
| `type=a,b` | read only these GitHub event types; see [Event types](#event-types) |
| `action=x,y` | keep only these actions; a filter after delivery, so `last=N` also counts the events that it drops |

## Headers

| Header | Meaning |
|---|---|
| `Last-Event-ID: <sequence>` | resume after this stream sequence; browsers send it on reconnect. An id older than the last 1000 matching events resumes at those 1000 |

## Stream contents

See [Event format, SSE framing](event-format.md#sse-framing). In short:
`retry: 5000`, a `connected` event, then one unnamed event per GitHub
event with the stream sequence as `id` and the type in the data, a `: heartbeat`
comment every 30 seconds, and an `error` event if the server loses
JetStream.

## Behaviour

- At most `SSE_MAX_CLIENTS` (1000) streams per process; beyond that a
  request gets a 503.
- Each connection gets its own JetStream consumer, deleted when the
  connection ends and in any case one minute after it goes idle.
- `last=N` counts only messages that match the subjects of the
  connection, found by a binary search over the stream; the stream also
  holds other subjects between them.

## Event types

Every subject ends in the event type. `type=a,b` puts each type in the
place of the last wildcard of the route's subjects, so NATS does the
filtering:

| Request | Subjects read |
|---|---|
| `/events/repo/4542716` | `repo.4542716.>` |
| `/events/repo/4542716?type=pull_request,issues` | `repo.4542716.pull_request`, `repo.4542716.issues` |
| `/events?type=push` | `org.*.push`, `global.push` |
| `/events/user/2?type=push` | `user.2.push` |

- `last=N` counts only events of these types: `last=10&type=push`
  replays the last 10 pushes of the route, however many other events
  are between them.
- A type must match `[a-z_]+`, and a list has at most 100 types;
  anything else is a 400 with the reason as text.
- `/events/type/{event_type}` already reads one type. A `type` list
  there must contain that type and changes nothing; a list without it
  is a 400.
- The `connected` event shows the subjects that the connection reads.
