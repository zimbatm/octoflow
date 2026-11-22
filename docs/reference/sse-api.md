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
| `/events/org/{org_id}`, `/events/org/{login}` | `org.{org_id}.>` |
| `/events/repo/{repo_id}`, `/events/repo/{owner}/{name}` | `repo.{repo_id}.>` |
| `/events/user/{user_id}`, `/events/user/{login}` | `user.{user_id}.*` |
| `/events/pr/{repo_id}/{number}`, `/events/pr/{owner}/{name}/{number}` | `pr.{repo_id}.{number}.>` |
| `/events/issue/{repo_id}/{number}`, `/events/issue/{owner}/{name}/{number}` | `issue.{repo_id}.{number}.>` |
| `/events/type/{event_type}` | `org.*.{event_type}`, `global.{event_type}` |

Path ids and numbers must match `[0-9]+`, `{event_type}` must match
`[a-z_]+`; anything else is a 404.

## Names in a route

`{login}`, `{owner}` and `{name}` are GitHub names:
`/events/repo/NixOS/nixpkgs` is `/events/repo/4542716`. The server reads
the id from the [name index](subjects.md#the-name-index) and then serves
the id route, so `stream_id` and `subjects` in the `connected` event
carry the id. Only the id goes into a subject, never the name.

| Rule | Detail |
|---|---|
| Known names | Octoflow knows a name from the first event with that name that the dispatcher routed. It does not ask GitHub. |
| Case | Ignored, as on GitHub: `nixos/NixPkgs` is `NixOS/nixpkgs`. |
| Characters | Letters, digits, `-`, `_` and `.`, at most 100 per name. A name with another character is not in the index. |
| Digits only | `/events/org/{x}` and `/events/user/{x}` read an `{x}` of digits only as an id. A login of digits only has no name route; use its id. |
| Bots | A login such as `dependabot[bot]` is not indexed; use the id. |
| Rename | The new name works from the first event that carries it. The old name stays in the index and points to the same id, until an event gives that name to another id. |
| Delay | The server keeps an answer for one minute. A name that moved to another id is followed after that time at the latest. A new name works at once. |

| Status | Body | When |
|---|---|---|
| 404 | `{"error":"octoflow has no event of this repository; it only knows the names it has seen in an event"}` (or `organization`, `user`) | the name is not in the index, or is not a valid name |
| 503 | `{"error":"the name index does not answer; use the numeric id"}` | the server could not read the index |

The id routes do not use the index.

| Route | Answers |
|---|---|
| `/stats` | JSON: `total_clients`, the open connections of this process; `events_stored`, the events in the stream without the fan-out copies, refreshed every 30 seconds; `stored_since`, the store time of the oldest message. The last two are absent when the stream does not answer |
| `/site` | JSON: `title`, `intro` and `streams` (`label`, `path`) of this deployment, from `SITE_TITLE`, `SITE_INTRO` and `SITE_STREAMS`; the landing page reads it |
| `/health` | `{"status":"healthy"}`; shared by every service on the address |

## Query parameters

| Parameter | Meaning |
|---|---|
| `last=N` | replay the last N events of the route and of `type`, then new ones; default 50, maximum 1000, `0` for none; ignored when `Last-Event-ID` or `since` is sent |
| `since=T` | replay from a time, then new ones: an RFC 3339 time (`2026-11-22T08:00:00Z`), or a duration back from now (`90m`, `36h`, `2d`); not capped; ignored when `Last-Event-ID` is sent; a bad value is a 400 |
| `type=a,b` | read only these GitHub event types; see [Event types](#event-types) |
| `action=x,y` | keep only these actions; a filter after delivery, so `last=N` also counts the events that it drops |
| `payload=none` | send each event without its `payload` field; see [Payload](#payload). Any other value is a 400 |

## Headers

| Header | Meaning |
|---|---|
| `Last-Event-ID: <sequence>` | resume after this stream sequence; browsers send it on reconnect. Not capped: every later event of the connection's subjects follows. An id older than the stream starts at its first message |

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

## Where the replay starts

The first of these that the request has decides:

| Request has | The stream starts |
|---|---|
| `Last-Event-ID` | after that sequence |
| `since` | at the first event stored at or after that time |
| `last=N`, or nothing (`last=50`) | at the last N events |

- `last=N` needs a search, so it has a maximum. `Last-Event-ID` and
  `since` give NATS the start directly and have no maximum: a consumer
  that was away gets every event that it missed.
- A long range is a lot of data. An event is 13 to 30 kB, and a busy
  repository stores 1000 events in 9 minutes. Select the types that
  you use with `type`, and leave the payload out with `payload=none`.
- The stream keeps events for `NATS_STREAM_MAX_AGE`, and the fan-out
  copies for `NATS_STREAM_COPY_MAX_AGE` when set. A start before that
  begins at the oldest event.
- `since` compares with the time at which the stream stored the
  message, which is not always `received_at`: a restore from the
  archive stores old events again.
- Write a time with `Z`, or encode the `+` of an offset as `%2B`.
- A reconnect of `EventSource` sends the same URL and `Last-Event-ID`,
  so `since` and `last` apply to the first connection only.

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

## Payload

An event is 13 to 30 kB, and the GitHub payload is nearly all of it. The
other fields are a few hundred bytes. `payload=none` sends each event
without its `payload` field; every other field of the
[event](event-format.md) stays:

```json
{"id":"3fa3a85c-6a48-11f0-8793-19d640392600","event":"pull_request","received_at":"2026-10-14T12:00:00.123456Z","repository":"NixOS/nixpkgs","organization":"NixOS","sender":"zimbatm","action":"opened","organization_id":"487568","repository_id":"4542716","sender_id":"2"}
```

Use it to count, to list or to route events, and with a long `since`
range. The default sends the stored message unchanged.
