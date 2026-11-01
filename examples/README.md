# Examples

Two small consumers. They build with the rest of the module, so
`go build ./...` and `go vet ./...` keep them honest.

## pr-monitor

Prints the pull request activity of one repository.

```bash
REPO_ID=4542716 NATS_URL=nats://localhost:4222 go run ./examples/pr-monitor
```

## security-monitor

Prints the events of one organization that change who can see or change
what: repositories made public or transferred, collaborators and teams
added or removed, security, secret scanning, code scanning and
dependabot alerts.

```bash
ORG_ID=487568 NATS_URL=nats://localhost:4222 go run ./examples/security-monitor
```

## Writing a consumer

```go
nc, _ := nats.Connect("nats://localhost:4222")
js, _ := nc.JetStream()

_, _ = js.Subscribe("github.events.repo.*.push", func(msg *nats.Msg) {
    var event events.GitHubEvent
    if err := json.Unmarshal(msg.Data, &event); err != nil {
        _ = msg.Term() // not retryable
        return
    }
    // event.Event, event.Action, event.Repository, event.Sender and the
    // numeric ids are parsed; event.Payload is the raw GitHub payload.
    _ = msg.Ack()
}, nats.Durable("my-consumer"), nats.DeliverNew())
```

Subjects to subscribe to, all with numeric ids:

| Subject | Delivers |
|---|---|
| `github.events.org.{org_id}.>` | every event of the organization, once |
| `github.events.global.{type}` | events without an organization |
| `github.events.repo.{repo_id}.{type}` or `.>` | one repository |
| `github.events.user.{user_id}.*` | every event involving a user |
| `github.events.pr.{repo_id}.*.*` | all pull requests of a repository |
| `github.events.pr.{repo_id}.{number}.*` | one pull request |
| `github.events.pr.{repo_id}.{number}.{type}` | one pull request, one event type |
| `github.events.issue.{repo_id}.*.*` and `.{number}.*` | the same for issues |

Every fan-out subject ends in the event type, so a wildcard `*` in that
position selects all types.

The same durable name on several instances makes NATS share the work
between them. Nak a message to see it again, Term it when retrying
cannot help.
