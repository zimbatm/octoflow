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

## From the stream to S3

The s3 service acknowledges a message only after the batch holding it
was uploaded. A crash loses the in-memory batch, nothing more: its
messages come back after `AckWait` (the batch timeout plus two minutes)
and are uploaded in a later batch. A failed upload is logged, the batch
is dropped and the same redelivery mechanism repeats it. Two files can
therefore hold the same event after a failure; `id` identifies it.

Batches close on 1000 events, 100 MB of payload, five minutes, or
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
and returns; HTTP servers get 30 seconds for in-flight requests. The
NATS connection closes last.

## After a loss

The archive is the backup of the stream. `octoflow restore` reads the
Parquet files for a date range and publishes the events again with their
original ids, so the dispatcher recreates the copies and consumers see
them as new. It skips what the stream still holds, so it fills the gap
without duplicating live data. See [Restore events from
the archive](../how-to/restore-from-the-archive.md).
