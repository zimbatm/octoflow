# Octoflow

Octoflow receives GitHub webhooks for an organization, stores every
event in NATS JetStream, fans it out to repository, user, pull request
and issue subjects, archives it to S3 as Parquet, and streams it to
browsers over Server-Sent Events. Consumers subscribe to the slice they
need with numeric ids that survive renames. A second ingest reads the
public Events API and GH Archive for repositories that cannot get a
webhook, best-effort.

```
GitHub webhooks          Events API + GH Archive
      |                            |
   webhook                   public-ingest
      |                            |
      v                            v
   NATS JetStream, stream GITHUB_EVENTS_RAW
     github.events.org.{org_id}.{type}        github.events.global.{type}
                      |
                  dispatcher
                      |
     github.events.repo.{repo_id}.{type}      github.events.user.{user_id}.{type}
     github.events.pr.{repo_id}.{n}.{type}    github.events.issue.{repo_id}.{n}.{type}
                      |
        s3 (Parquet)   sse (browsers)   your consumers
```

## Quick start

```bash
nix develop
cp .env.example .env        # WEBHOOK_SECRET=change-me, local S3 store
make run                    # NATS, rustfs and every service under hivemind

# in a second shell
WEBHOOK_SECRET=change-me ./scripts/test-webhook.sh
nats sub "github.events.>"
curl -N 'http://localhost:8080/events?last=5'
```

The dashboard is at http://localhost:8080. [Your first
events](docs/tutorials/first-events.md) walks through this in detail.

## Documentation

The [documentation index](docs/README.md) follows Diátaxis:

- **Tutorials**: [Your first events](docs/tutorials/first-events.md)
- **How-to guides**: [configure the GitHub webhook](docs/how-to/configure-github-webhook.md),
  [write a consumer](docs/how-to/write-a-consumer.md),
  [stream to a browser](docs/how-to/stream-to-a-browser.md),
  [deploy on NixOS](docs/how-to/deploy-on-nixos.md),
  [ingest public repositories](docs/how-to/ingest-public-repositories.md),
  [query the Parquet archive](docs/how-to/query-the-parquet-archive.md),
  [restore from the archive](docs/how-to/restore-from-the-archive.md),
  [inspect the stream](docs/how-to/inspect-the-stream.md),
  [troubleshoot](docs/how-to/troubleshoot.md)
- **Reference**: [configuration](docs/reference/configuration.md),
  [command line](docs/reference/cli.md),
  [subjects and the stream](docs/reference/subjects.md),
  [event format](docs/reference/event-format.md),
  [SSE API](docs/reference/sse-api.md),
  [metrics](docs/reference/metrics.md),
  [NixOS module](docs/reference/nixos-module.md),
  [GitHub event types](docs/reference/github-event-types.md)
- **Explanation**: [architecture](docs/explanation/architecture.md),
  [delivery guarantees](docs/explanation/delivery-guarantees.md),
  [public events](docs/explanation/public-events.md)

## Services

One binary, `octoflow <service>`, or `octoflow all` for everything in
one process:

| Service | Does |
|---|---|
| `webhook` | verifies the signature, stores the event, answers GitHub after the ack |
| `dispatcher` | copies each event to its repo, user, pr and issue subjects |
| `s3` | batches events into Parquet files in S3 |
| `sse` | streams any subject to browsers, with replay and resume |
| `dashboard` | serves the demo page in `web/` |
| `public-ingest` | reads the Events API and GH Archive for repositories without a webhook |
| `restore --from DATE` | publishes archived events from S3 back into the stream |

## Development

```bash
make test           # go test ./...; several packages start an embedded NATS server
make lint           # golangci-lint
make build          # bin/octoflow
nix flake check     # package, tests and a NixOS VM test of the module
```

## License

MIT License - see LICENSE file for details
