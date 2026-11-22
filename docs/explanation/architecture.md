# Architecture

Octoflow exists so that one webhook per organization can feed many
consumers, none of which has to run a public endpoint, parse GitHub's
signatures or keep its own history. This page explains the shape that
follows from that goal and the trade-offs in it.

## One stream, two stages

```
GitHub webhooks          Events API + GH Archive
      |                            |
   webhook                   public-ingest
      |                            |
      v                            v
   github.events.org.{org_id}.{type}   github.events.global.{type}     (primary)
                      |
                  dispatcher
                      |
      repo.{repo_id}.{type}   user.{user_id}.{type}
      pr.{repo_id}.{n}.{type} issue.{repo_id}.{n}.{type}                (fan-out)
                      |
        s3 (Parquet)   sse (browsers)   your consumers
```

The receiver does one thing: verify the signature, publish the event on
its organization subject, and answer GitHub only after JetStream
acknowledged. GitHub expects an answer within ten seconds, so nothing
else happens on that path.

The dispatcher reads the primary subjects and republishes each event
under every entity it concerns. This is the second stage. It runs
asynchronously, can be slow or down without losing deliveries, and can
grow new routing dimensions without touching the receiver. Consumers
subscribe to exactly the slice they want and let JetStream do the
filtering.

Both stages write into one stream. A single stream is one thing to
back up, size and monitor, and it lets the SSE server replay and resume
by one sequence number across every route. The price is that the same
payload is stored once per subject it is routed to: with a repository,
a sender and a pull request author that is four copies. Retention is
seven days by default, so the cost is bounded, and the typeless PR and issue copies
that an earlier design also wrote were dropped because they doubled the
cost for a cosmetic gain. If the stream still grows too large,
`NATS_STREAM_COPY_MAX_AGE` gives the copies a shorter life than the
primary events.

## The dispatcher keeps one map

The fan-out of most events needs only the event. CI events are the
exception: a `check_run`, `check_suite`, `workflow_run`, `workflow_job`
or `status` event names a commit, and its `pull_requests` list is empty
for a pull request from a fork, which is most pull requests of a public
repository. "Is this pull request green?" could not be answered from
the pr subject.

So the dispatcher remembers the head commit of every pull request it
sees, in the KV bucket `octoflow_heads`, and copies a CI event to the
pull requests that have its commit as their head. A bounded cache in
front of the bucket holds hits and misses, because CI events are over
90% of the traffic of a busy repository and come in bursts per commit.

The map is an index, not a record. If the bucket is lost, the
dispatcher creates it again on start and nothing fails: CI events get
no pr copy until the next `pull_request` event of each pull request
teaches its head again, and the copies missed in between are not
written later. Every other copy is unaffected. A restart loses only the
cache.

## Numeric ids in subjects

Organizations, repositories and users get renamed and transferred. A
subscription on `repo.4542716.>` survives all of that; one on a name
would not, and historical data would split in two. The parsed names are
still in every message for display and logging.

A person knows `NixOS/nixpkgs`, not 4542716, and GitHub limits lookups
without a token to 60 per hour per address. Octoflow sees every name
next to its id in the events it routes, so the dispatcher keeps a name
index in the KV bucket `octoflow_names` and the SSE server accepts a
name in a route. The name is resolved once, when the client connects;
the subject still carries the id. The index is a convenience and not a
record: it holds only names that an event carried, it keeps the old name
after a rename, and it can be deleted and left to fill again.

## Deduplication by message id

GitHub can deliver the same event twice, and an operator can press
"Redeliver". The receiver does not keep its own memory of what it saw;
it relies on JetStream's duplicate window, keyed by the delivery id,
which is also what makes "answer 200 only after the ack" safe: a
redelivery after a failed ack is stored once. Fan-out copies get
`<id>@<subject>` as their id so that a dispatcher retry after a partial
fan-out completes it without storing anything twice. Public-ingest uses
`gh-event-<id>` for both of its sources, so an event that the Events API
and GH Archive both deliver is stored once.

## Why every fan-out subject ends in the event type

A subject such as `pr.{repo}.{n}.{type}` lets a consumer pick one pull
request (`pr.{repo}.{n}.*`), all pull requests (`pr.{repo}.*.*`) or one
event type across them (`pr.*.*.pull_request_review`) with wildcards,
from one stored copy. Writing a second, typeless copy only saved the
trailing `.*` and doubled the storage for those events.

## SSE replay

A browser that connects wants the last few events before the live ones,
and after a reconnect wants the ones it missed. The stream sequence is
the natural cursor for both. Finding "the last N events matching this
route" is not a direct lookup, because the stream interleaves every
subject; the server binary-searches the sequence space with temporary
consumers and reads `NumPending`. It costs about twenty JetStream calls
per connection and is exact. A cheaper approximation can replace it if
many dashboards reconnect at once.

## The public status

`/status` shows a visitor the events per minute of the last 24 hours.
The Prometheus counters cannot serve it: they are per process, start
at zero on a restart, and the SSE server that answers is not the
process that receives. The stream cannot serve it cheaply either: a
count per minute is a consumer per minute.

So the counts go to a KV bucket, one key per hour with 60 counts. The
dispatcher counts the events: it sees each primary event one time,
whether the webhook receiver, public-ingest or a restore published it,
and its durable consumer continues after a restart. It counts under
the store time of the message, so a backlog lands in the right
minutes. The webhook receiver counts the deliveries it refuses, because
only it sees them. Both write at most every 5 seconds per key.

The cost: the chart follows the dispatcher. When the dispatcher is
down, the newest minutes stay at zero until it is back. The time of
the last event comes from the stream and shows that events still
arrive.

## One binary

The services share configuration, a NATS connection, stream setup,
signal handling and shutdown. They are subcommands of one binary so
that this is written once, and `octoflow all` runs them in one process
for a small deployment. The NixOS module still runs one unit per
service so that each can be restarted and watched on its own.

## What Octoflow is not

It does not transform or enrich events, does not proxy the GitHub API,
and does not serve as a store of record beyond the Parquet archive.
Consumers that need more than the payload call GitHub themselves.

## Capacity

Measured on 2026-11-01 on a laptop, `octoflow all` and nats-server on
the same machine, 3000 `pull_request` deliveries of 314 bytes sent by
16 parallel curl processes, no S3:

| | |
|---|---|
| Receiver | 1370 deliveries/s, 0.5 ms per request on average |
| Publish to JetStream | 0.4 ms on average, 21000 publishes |
| Dispatcher | caught up within 10 ms of the last delivery |
| Stream growth | 7 messages and 4.8 KB per event: the primary, a repo copy, four user copies, a pr copy |

The curl processes were the bottleneck, not the services. For scale:
the GitHub Events API of a busy repository yields a few events per
second. A delivery of 25 MB takes about 60 ms end to end.

The numbers come from `scripts/load.sh`; rerun it after a change to the
hot path.
