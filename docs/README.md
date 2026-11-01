# Octoflow documentation

The documentation follows [Diátaxis](https://diataxis.fr/): four kinds of
pages, each with one job.

## Tutorials: learn by doing

- [Your first events](tutorials/first-events.md): run everything locally,
  send a signed delivery, watch it in NATS, over SSE and in S3, and run a
  consumer against it.

## How-to guides: get a task done

- [Configure the GitHub webhook](how-to/configure-github-webhook.md)
- [Write a consumer](how-to/write-a-consumer.md)
- [Stream events to a browser](how-to/stream-to-a-browser.md)
- [Deploy on NixOS](how-to/deploy-on-nixos.md)
- [Ingest repositories without a webhook](how-to/ingest-public-repositories.md)
- [Query the Parquet archive](how-to/query-the-parquet-archive.md)
- [Restore events from the archive](how-to/restore-from-the-archive.md)
- [Inspect the stream with the NATS CLI](how-to/inspect-the-stream.md)
- [Troubleshoot](how-to/troubleshoot.md)

## Reference: look things up

- [Configuration](reference/configuration.md): every environment variable
- [Command line, make targets and scripts](reference/cli.md)
- [Subjects and the stream](reference/subjects.md)
- [Event format](reference/event-format.md): the JSON message and the Parquet row
- [SSE API](reference/sse-api.md) and its [OpenAPI description](reference/sse-openapi.yaml)
- [Metrics](reference/metrics.md)
- [NixOS module](reference/nixos-module.md)
- [GitHub event types](reference/github-event-types.md)

## Explanation: understand the design

- [Architecture](explanation/architecture.md): two stages, numeric ids, one stream
- [Delivery guarantees](explanation/delivery-guarantees.md): what can be lost, duplicated or delayed
- [Public events](explanation/public-events.md): why the Events API and GH Archive are best-effort
