# Your first events

In this tutorial you run Octoflow on your machine, send it a GitHub
delivery, and follow that event through NATS, the SSE server and the S3
archive. At the end you run a consumer that reacts to it. It takes about
fifteen minutes. You need Nix with flakes; everything else comes from the
repository's development shell.

## 1. Enter the development shell

```bash
git clone https://github.com/zimbatm/octoflow
cd octoflow
nix develop
```

The shell has Go, `nats-server`, the `nats` CLI, `rustfs` (a local S3
store) and `hivemind`. Create the environment file:

```bash
cp .env.example .env
```

The example sets `WEBHOOK_SECRET=change-me` and points the s3 service at
the local store. That is all you need here.

## 2. Start everything

Leave the shell from step 1 and open a new one, so that `.env` is loaded,
then:

```bash
nix develop
make run
```

Hivemind starts NATS, rustfs and one process per service. After a few
seconds you see lines like these:

```
webhook    | ... msg="Webhook receiver listening" address=:8080
dispatcher | ... msg="Worker started" ...
sse        | ... msg="SSE server listening" address=:8081
s3         | ... msg="Created stream" name=GITHUB_EVENTS_RAW subjects=[github.events.>]
```

Keep this terminal open. Everything below happens in a second
`nix develop` shell.

## 3. Send a delivery

GitHub signs every delivery with the webhook secret. The test script
does the same:

```bash
WEBHOOK_SECRET=change-me ./scripts/test-webhook.sh
```

Expected output ends with:

```
{"status":"ok","delivery_id":"test-1762012345"}
HTTP Status: 200
```

The 200 means JetStream stored the event. Try it without the secret to
see the receiver refuse it:

```bash
WEBHOOK_SECRET=wrong ./scripts/test-webhook.sh
```

```
Invalid signature
HTTP Status: 401
```

## 4. Find the event in NATS

The event went into the stream under its organization subject. The
dispatcher then copied it to the repository, user and pull request
subjects:

```bash
nats stream info GITHUB_EVENTS_RAW | grep -E 'Messages|Subjects'
nats consumer info GITHUB_EVENTS_RAW event-dispatcher | grep -E 'Pending|Delivered'
```

The stream holds 4 messages for the one delivery: the primary event plus
copies for repo 111, user 2 and pull request 7. Subscribe to the pull
request and send the delivery again:

```bash
nats sub "github.events.pr.111.>" &
WEBHOOK_SECRET=change-me ./scripts/test-webhook.sh
```

The subscription prints the copy on `github.events.pr.111.7.pull_request`.
Stop it with `kill %1`.

## 5. Read it over SSE

The SSE server replays the last events of any route and then streams new
ones:

```bash
curl -N 'http://localhost:8081/events/repo/111?last=5'
```

```
retry: 5000

event: connected
data: {"status":"connected","stream_id":"repo-111","subjects":"github.events.repo.111.>"}

id: 2
event: pull_request
data: {"id":"test-...","event":"pull_request","action":"opened",...}
```

The `id` is the JetStream sequence. A browser sends it back as
`Last-Event-ID` when it reconnects, and the stream resumes after it.
Stop curl with Ctrl-C, then open http://localhost:8082 to see the same
stream in the dashboard.

## 6. See the archive being written

The s3 service batches events and uploads a Parquet file every 1000
events or five minutes, and when it stops. Stop hivemind with Ctrl-C in
the first terminal and look for:

```
s3 | ... msg="Uploaded batch to S3" event_count=2 bucket=octoflow key=github-events/2026/11/01/1762012400000000000.parquet
```

The file is in `.data/s3/octoflow/github-events/<year>/<month>/<day>/`.
[Query the Parquet archive](../how-to/query-the-parquet-archive.md)
shows how to read it with DuckDB.

## 7. Run a consumer

Start `make run` again, then run the example that prints pull request
activity for repository 111:

```bash
REPO_ID=111 go run ./examples/pr-monitor
```

In another shell, send the delivery once more:

```bash
WEBHOOK_SECRET=change-me ./scripts/test-webhook.sh
```

The consumer prints:

```
PR #7 opened: Test PR by @test-user
```

It subscribes to `github.events.pr.111.*.*` with a durable consumer, so
it picks up where it left off after a restart. Its source is 100 lines;
[Write a consumer](../how-to/write-a-consumer.md) walks through them.

## Where to go next

- Point a real organization at your receiver: [Configure the GitHub
  webhook](../how-to/configure-github-webhook.md).
- Put it on a server: [Deploy on NixOS](../how-to/deploy-on-nixos.md).
- Understand what you just saw: [Architecture](../explanation/architecture.md).
