# Stream events to a browser

The SSE server turns a subject into a `text/event-stream` that a
browser's `EventSource` reads. No client library is needed.

## 1. Choose the route

| Route | Events |
|---|---|
| `/events` | every event once |
| `/events/org/{login}` | one organization |
| `/events/repo/{owner}/{name}` | one repository |
| `/events/user/{login}` | everything a user is involved in |
| `/events/pr/{owner}/{name}/{number}` | one pull request |
| `/events/issue/{owner}/{name}/{number}` | one issue |
| `/events/type/{event_type}` | one event type across organizations |

Names are GitHub names in any case, for example
`/events/repo/NixOS/nixpkgs`. Octoflow knows a name once it has seen an
event with it; before that the route answers 404 with a JSON `error`.
Each route also takes the numeric id in place of the name
(`/events/org/487568`, `/events/repo/4542716`, `/events/pr/4542716/123`).
The id never changes; a name can. See
[Names in a route](../reference/sse-api.md#names-in-a-route). Add `?last=N` to replay the last N events first
(default 50, maximum 1000, `0` for none).

Add `?type=push,pull_request` to read only some event types. Most
events of a busy repository are CI events (`workflow_job`, `check_run`,
`workflow_run`, `check_suite`), so name the types that you use. `last=N`
then counts only those types: `?type=pull_request&last=20` replays 20
pull request events. `?action=opened` drops the other actions after
that count, so it can show fewer than N.

## 2. Connect

```javascript
const url = 'https://events.example.org/events/repo/NixOS/nixpkgs?type=pull_request&last=20';
const source = new EventSource(url);

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(`PR #${event.payload.number} ${event.action} by ${event.sender}`);
};

source.addEventListener('connected', (e) => console.log('connected', e.data));
source.onerror = () => console.log('reconnecting');
```

`EventSource` does not show a status code. If the name can be unknown,
call `fetch(url)` first and read the `error` of a 404.

GitHub events carry no event name, so `onmessage` receives every type;
the type is in `event.event`. The `connected` event comes
first; `error` events end the stream when the server lost JetStream.

## 3. Resume after a reconnect

Each event's `id` is the JetStream sequence. `EventSource` reconnects by
itself and sends the last id as `Last-Event-ID`; the server resumes after
it and ignores `?since=` and `?last=` on that request. Nothing to do on
your side.

A program that is not a browser does the same: keep the last `id`, and
send it as `Last-Event-ID` on the next start. The resume has no maximum,
so no event is lost while the stream still holds it.

## 4. Start from a time

`?since=` replays from a time and has no maximum:

| Value | Start |
|---|---|
| `since=90m` | 90 minutes ago; `36h` and `2d` work too |
| `since=2026-11-22T08:00:00Z` | that time, RFC 3339 |

A long range streams a lot of data: an event is 13 to 30 kB. Add
`?type=` to read only the types that you use.

## 5. Leave the payload out

An event is 13 to 30 kB, nearly all of it the GitHub payload. A page
that shows only who did what does not need it:

```javascript
const source = new EventSource('https://events.example.org/events/repo/4542716?since=2d&payload=none');
source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(`${event.received_at} ${event.event} ${event.action || ''} by ${event.sender}`);
};
```

With `?payload=none` an event has no `payload` field and is a few
hundred bytes. All other fields stay.

## 6. Use it from another origin

The server answers `Access-Control-Allow-Origin: *`, so a page on any
origin can connect. Behind nginx, keep buffering off and the read timeout
long; the NixOS module sets this.

## 7. Try it from the shell

```bash
curl -N 'http://localhost:8080/events?last=3'
curl -N -H 'Last-Event-ID: 42' 'http://localhost:8080/events/org/222'
curl -N 'http://localhost:8080/events/repo/111?since=2h&type=pull_request'
curl -N 'http://localhost:8080/events?last=100&payload=none'
curl http://localhost:8080/stats
```

The dashboard in `web/index.html` is a complete client; serve it with
`octoflow dashboard` or any static file server. It finds the SSE server
from its own origin, or takes `?sse=https://host` in the page URL.
