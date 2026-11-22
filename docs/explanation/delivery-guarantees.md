# Delivery guarantees

Octoflow is at-least-once end to end. This page says where the
guarantees come from, where duplicates can appear and what is lost in
which failure.

## From GitHub to the stream

The receiver answers 200 only after JetStream acknowledged the publish.
A 503 means the event is not stored; GitHub marks the delivery failed
and an operator can redeliver it. GitHub does not retry on its own.

While the receiver is down, GitHub cannot deliver; those events show as
failed deliveries in GitHub and can be redelivered for a while. There
is no local buffer by design: a buffer that answers 200 before storing
would lose events on a crash while telling GitHub they were safe.

A redelivery within the 24 hour duplicate window is stored once. After
the window it is stored again; consumers that must not act twice can
key on `id`, the delivery id, which does not change.

## From the stream to the fan-out

The dispatcher acknowledges a primary message only after every copy was
acknowledged by the stream. A failure midway leaves the message
unacknowledged; it is redelivered, the already stored copies are
dropped as duplicates by their `<id>@<subject>` ids, and the missing
ones are written. A message that is not an event at all is terminated,
not retried.

Copies of one event reach the stream in the order the dispatcher
writes them, but copies of different events can interleave: ten workers
pull in parallel. Within one subject the order is the order of
publishing.

### CI events and the head of a pull request

The pr copy of a CI event is best-effort, unlike every other copy. The
dispatcher links a CI event to a pull request through a map from head
commit to pull request number, which it learns from `pull_request`
events. It writes the map before the copies of the `pull_request`
event, so before that event is acknowledged.

| Case | Result |
|---|---|
| CI event dispatched after the `pull_request` event of its head | pr copy |
| CI event dispatched before it: GitHub delivered it first, or another worker was faster | no pr copy; the repo copy and the primary event are there |
| the same commit, later CI events, same dispatcher process | pr copy at once |
| the same commit, later CI events, another dispatcher process | pr copy after at most 10 seconds, the life of a cached miss |
| the map cannot be read or written | no pr copy, a warning in the log, the event is acknowledged |
| the event is redelivered | the same copies; the stream drops them by id inside the duplicate window |

A copy that was missed is not written later. The series
`octoflow_dispatch_ci_events_total{link="none"}` counts the CI events
without a pr copy; most of them are for commits that are not the head
of a pull request.

## From the stream to S3

The s3 service acknowledges a message only after the batch holding it
was uploaded. A crash loses the in-memory batch, nothing more: its
messages come back after `AckWait` (the batch timeout plus two minutes)
and are uploaded in a later batch. A failed upload is logged, the batch
is dropped and the same redelivery mechanism repeats it. Two files can
therefore hold the same event after a failure; `id` identifies it.

Batches close on 1000 events, 100 MB of payload, one hour after their
first event (`S3_BATCH_TIMEOUT`), or
shutdown. A clean shutdown flushes before the NATS connection closes,
so the acknowledgements still go out.

## From the stream to a browser

SSE consumers do not acknowledge. The client's cursor is the stream
sequence in each event's `id`; on reconnect the browser sends it as
`Last-Event-ID` and the server resumes after it. Events that aged out
of the stream in between are gone for that client. A connection that
drops before an event was written repeats that event after the resume:
exactly what `Last-Event-ID` means.

## Public events

The Events API and GH Archive are themselves lossy; see
[Public events](public-events.md). Within what they deliver, the same
rules apply: the publisher retries, the stream deduplicates by
`gh-event-<id>` for 24 hours, and progress is saved only after
publishing, so a crash replays at most one poll or one hour.

## Shutdown

On SIGTERM every service stops taking new work, finishes what it holds,
and returns; SSE streams are closed at once, and other HTTP requests get
30 seconds to finish. The
NATS connection closes last.

GitHub does not retry a delivery that was refused. With the NixOS module
systemd holds the port of the receiver (`octoflow-webhook.socket`), so a
delivery that arrives while the service restarts waits in the socket and
is answered by the new process. GitHub waits 10 seconds for an answer.

## After a loss

The archive is the backup of the stream. `octoflow restore` reads the
Parquet files for a date range and publishes the events again with their
original ids, so the dispatcher recreates the copies and consumers see
them as new. It skips what the stream still holds, so it fills the gap
without duplicating live data. See [Restore events from
the archive](../how-to/restore-from-the-archive.md).
