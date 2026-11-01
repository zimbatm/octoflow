# Write a consumer

A consumer is any JetStream subscriber. This guide builds one in Go with
the `nats.go` client; the same subjects work from any NATS client.

## 1. Pick the subject

Every subject carries numeric GitHub ids, so renames do not break it.
Find the id with `curl -s https://api.github.com/repos/OWNER/REPO | jq .id`
(or `/orgs/NAME`, `/users/LOGIN`).

| You want | Subscribe to |
|---|---|
| everything in an organization, once per event | `github.events.org.{org_id}.>` |
| one repository | `github.events.repo.{repo_id}.>` |
| one event type in every repository | `github.events.repo.*.push` |
| everything a user is involved in | `github.events.user.{user_id}.*` |
| one pull request | `github.events.pr.{repo_id}.{number}.*` |
| all pull requests of a repository | `github.events.pr.{repo_id}.*.*` |
| one issue | `github.events.issue.{repo_id}.{number}.*` |

The full list is in [Subjects](../reference/subjects.md). Do not
subscribe to `github.events.>`: it delivers every event once per copy.

## 2. Subscribe durably

```go
nc, err := nats.Connect(os.Getenv("NATS_URL"))
js, err := nc.JetStream()

sub, err := js.Subscribe("github.events.repo.*.push", handle,
    nats.Durable("my-ci"),   // the server remembers where you are
    nats.DeliverNew(),       // first start: skip history
)
```

The durable name is the consumer's identity. Run two instances with the
same name and NATS shares the messages between them. Replace
`DeliverNew` with `DeliverAll` to process the stream's history first.

## 3. Decode the event

The message is a `pkg/events.GitHubEvent`. The routing fields are
parsed; the GitHub payload is raw JSON. Decode only what you need:

```go
func handle(msg *nats.Msg) {
    var event events.GitHubEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        _ = msg.Term() // not an event; retrying cannot help
        return
    }

    var push struct {
        Ref     string `json:"ref"`
        Commits []struct {
            ID      string `json:"id"`
            Message string `json:"message"`
        } `json:"commits"`
    }
    if err := json.Unmarshal(event.Payload, &push); err != nil {
        _ = msg.Term()
        return
    }

    if push.Ref == "refs/heads/main" {
        if err := triggerBuild(event.RepositoryID, push.Commits); err != nil {
            _ = msg.Nak() // see it again later
            return
        }
    }
    _ = msg.Ack()
}
```

`event.Event`, `event.Action`, `event.Repository`, `event.Sender` and
the `*ID` fields are there for routing and logging. The field list is in
[Event format](../reference/event-format.md).

## 4. Acknowledge correctly

| Call | Meaning |
|---|---|
| `Ack` | done; never deliver again |
| `Nak` | not now; deliver again after the consumer's `AckWait` |
| `Term` | never; the message is bad and retrying cannot help |
| nothing | after `AckWait` (30 s by default) the message comes back |

Octoflow stores each event once per subject, so a consumer that reads
one subject sees each event once, barring redeliveries. Make the
handler idempotent anyway: `event.ID` is the delivery id and does not
change on redelivery.

## 5. Run it against the local setup

Start `make run`, send `WEBHOOK_SECRET=change-me ./scripts/test-webhook.sh`,
and your consumer receives the sample event. The two programs in
[examples/](../../examples/) are complete and built with the module:

```bash
REPO_ID=111 go run ./examples/pr-monitor
ORG_ID=222 go run ./examples/security-monitor
```
