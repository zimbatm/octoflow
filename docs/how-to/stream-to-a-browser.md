# Stream events to a browser

The SSE server turns a subject into a `text/event-stream` that a
browser's `EventSource` reads. No client library is needed.

## 1. Choose the route

| Route | Events |
|---|---|
| `/events` | every event once |
| `/events/org/{org_id}` | one organization |
| `/events/repo/{repo_id}` | one repository |
| `/events/user/{user_id}` | everything a user is involved in |
| `/events/pr/{repo_id}/{number}` | one pull request |
| `/events/issue/{repo_id}/{number}` | one issue |
| `/events/type/{event_type}` | one event type across organizations |

Ids must be numeric. Add `?type=push,pull_request` and `?action=opened`
to filter, and `?last=N` to replay the last N matching events first
(default 50, maximum 1000, `0` for none).

## 2. Connect

```javascript
const url = 'https://events.example.org/events/repo/4542716?type=pull_request&last=20';
const source = new EventSource(url);

source.addEventListener('pull_request', (e) => {
  const event = JSON.parse(e.data);
  console.log(`PR #${event.payload.number} ${event.action} by ${event.sender}`);
});

source.addEventListener('connected', (e) => console.log('connected', e.data));
source.onerror = () => console.log('reconnecting');
```

Events are named after the GitHub event type, so `onmessage` never
fires; use `addEventListener` per type. The `connected` event comes
first; `error` events end the stream when the server lost JetStream.

## 3. Resume after a reconnect

Each event's `id` is the JetStream sequence. `EventSource` reconnects by
itself and sends the last id as `Last-Event-ID`; the server resumes after
it and ignores `?last=` on that request. Nothing to do on your side.

## 4. Use it from another origin

The server answers `Access-Control-Allow-Origin: *`, so a page on any
origin can connect. Behind nginx, keep buffering off and the read timeout
long; the NixOS module sets this.

## 5. Try it from the shell

```bash
curl -N 'http://localhost:8081/events?last=3'
curl -N -H 'Last-Event-ID: 42' 'http://localhost:8081/events/org/222'
curl http://localhost:8081/stats
```

The dashboard in `web/index.html` is a complete client; serve it with
`octoflow dashboard` or any static file server. It finds the SSE server
from its own address, or takes `?sse=https://host` in the page URL.
