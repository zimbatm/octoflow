# Examples

Two consumers, built with the module so `go build ./...` and
`go vet ./...` keep them honest. [Write a consumer](../docs/how-to/write-a-consumer.md)
walks through the pattern they follow; [Subjects](../docs/reference/subjects.md)
lists what to subscribe to.

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
