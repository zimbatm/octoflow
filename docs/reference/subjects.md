# Subjects and the stream

All subjects start with `NATS_SUBJECT_PREFIX`, `github.events` by
default, and carry numeric GitHub ids.

## Primary subjects

Written by the webhook receiver and public-ingest, once per event.

| Subject | When |
|---|---|
| `github.events.org.{org_id}.{event_type}` | the payload has an organization or a repository owner |
| `github.events.global.{event_type}` | neither, for example `sponsorship` |

`{event_type}` is the `X-GitHub-Event` header: `push`, `pull_request`,
`issues`, `issue_comment`, `pull_request_review`, and so on.

## Fan-out subjects

Written by the dispatcher, one copy per subject.

| Subject | One copy per |
|---|---|
| `github.events.repo.{repo_id}.{event_type}` | event with a repository |
| `github.events.user.{user_id}.{event_type}` | involved user: sender, PR or issue author, assignees, requested reviewers, comment and review authors, team members; none for the types in `DISPATCH_USER_SKIP_EVENTS` |
| `github.events.pr.{repo_id}.{number}.{event_type}` | `pull_request`, `pull_request_review` and `pull_request_review_comment` event; `check_run`, `check_suite`, `workflow_run`, `workflow_job` and `status` event per pull request that the dispatcher finds for it, see [CI events](#ci-events-on-the-pr-subject) |
| `github.events.issue.{repo_id}.{number}.{event_type}` | `issues` and `issue_comment` event |

## CI events on the pr subject

A CI event names a commit, not a pull request. The dispatcher finds the
pull requests in this order:

| Source | Used when |
|---|---|
| `pull_requests` in `check_run`, `check_suite` or `workflow_run` | the list is not empty; GitHub leaves it empty for a pull request from a fork. An entry whose `base.repo.id` is another repository is ignored |
| the bucket `octoflow_heads`, by head commit | the list is empty or absent |

The head commit is `head_sha` of `check_run`, `check_suite`,
`workflow_run` and `workflow_job`, and `sha` of `status`.

| Bucket `octoflow_heads` | |
|---|---|
| Key | `{repo_id}.{head_sha}` |
| Value | pull request numbers, comma-separated, for example `42,57`; at most 16 |
| Written by | the dispatcher, on every `pull_request` event of any action, from `pull_request.head.sha` and `pull_request.number`, before the copies of that event |
| Expiry | `NATS_STREAM_MAX_AGE` after the last write of the key, 30 days when the stream has no max age; applied to an existing bucket on start. A `pull_request` event rewrites a key older than half of that |
| Storage | file, `NATS_STREAM_REPLICAS` replicas |

Each dispatcher process keeps the last 10000 answers in memory: a hit
for 10 minutes, a miss for 10 seconds. A burst of CI events on one
commit costs one read of the bucket.

Limits:

- A CI event dispatched before the first `pull_request` event of its
  head commit gets no pr copy, and none is added later. See [Delivery
  guarantees](../explanation/delivery-guarantees.md#ci-events-and-the-head-of-a-pull-request).
- The link is by commit only. A push to a branch whose tip is also the
  head of a pull request gives CI events that land on that pull request.
- A CI event whose head commit is not the head of the pull request is
  not linked. A workflow started by `pull_request_target` runs on the
  base branch and reports that commit.
- When the bucket does not answer, the event is dispatched without the
  pr copy and the dispatcher logs a warning.

Every fan-out subject ends in the event type. Useful wildcards:

| Pattern | Delivers |
|---|---|
| `github.events.org.{org_id}.>` | everything in an organization, once |
| `github.events.repo.{repo_id}.>` | everything in a repository |
| `github.events.repo.*.push` | one event type in every repository |
| `github.events.user.{user_id}.*` | everything a user is involved in |
| `github.events.pr.{repo_id}.{number}.*` | one pull request |
| `github.events.pr.{repo_id}.*.*` | all pull requests of a repository |
| `github.events.pr.*.*.pull_request_review` | every review everywhere |
| `github.events.issue.{repo_id}.*.*` | all issues of a repository |

`github.events.>` matches primary events and every copy, so it delivers
an event once per subject it was routed to.

## The stream

| Setting | Value |
|---|---|
| Name | `GITHUB_EVENTS_RAW` (`NATS_STREAM_NAME`) |
| Subjects | `github.events.>` |
| Storage | file |
| Replicas | `NATS_STREAM_REPLICAS`, default 1 |
| Retention | limits policy, discard old, max age `NATS_STREAM_MAX_AGE`, default 7 days; copies `NATS_STREAM_COPY_MAX_AGE` when set |
| Duplicate window | 24 hours, or the max age when that is shorter |
| Max consumers | unlimited |

Every service calls `EnsureStream` on start: it creates the stream or
updates these settings on the existing one.

## Message ids

| Message | Id |
|---|---|
| webhook delivery | `X-GitHub-Delivery` |
| public-ingest event | `gh-event-<GitHub event id>` from both sources |
| fan-out copy | `<primary id>@<subject>` |

The stream drops a message whose id it has seen within the duplicate
window.

## Consumers

| Durable name | Owner | Filter | Ack |
|---|---|---|---|
| `event-dispatcher` | dispatcher | primary subjects | explicit, 1000 pending |
| `s3-worker` | s3 service | primary subjects | explicit; wait = batch timeout + 2 min, 2000 pending |
| `sse-<nanos>-<n>` | one per SSE connection | the route's subjects | none; removed one minute after the client leaves |

Progress of public-ingest is not a consumer but the KV bucket
`octoflow_public_ingest`, keys `events-api.<repo_id>` and
`gharchive.next_hour`. The dispatcher keeps the heads of pull requests
in the KV bucket `octoflow_heads`, see [CI events](#ci-events-on-the-pr-subject).

## The name index

The KV bucket `octoflow_names` maps GitHub names to the ids in the
subjects. The dispatcher writes it; the SSE server reads it for the
[routes that take a name](sse-api.md#names-in-a-route).

| Key | Value | From the event fields |
|---|---|---|
| `org.{login}` | organization id | `organization`, `organization_id` |
| `repo.{owner}.{name}` | repository id | `repository`, `repository_id` |
| `user.{login}` | user id | `sender`, `sender_id` |

| Rule | Detail |
|---|---|
| Case | Keys are lower case; GitHub names are case-insensitive. |
| Characters | A name has letters, digits, `-`, `_` and `.`, at most 100 per part. An event name with another character, such as `dependabot[bot]`, gets no key. |
| `.` in a name | Written `=`, because `.` separates the parts of a key: `numtide/.github` is `repo.numtide.=github`. |
| Value | The id in decimal digits. A reader must ignore any other value. |
| Organization | As in the primary subject: the owner of the repository when the payload has no organization. A user that owns a repository thus also has an `org.` key. |
| User | Only the sender of an event. Other involved users get a user subject but no key. |
| Writes | One per new or changed pair for each dispatcher process, which remembers what it wrote. A restart writes each pair again once. A failed write is logged at most once per minute and tried again with the next event that carries the pair; it does not delay the event. |
| Rename | The new name gets a key with the same id. The old key stays. If another id later takes the old name, the next event with that name changes the key. |
| Expiry | None. The bucket keeps one value per key. Delete a key with `nats kv del octoflow_names <key>`; the dispatcher writes it again only after a restart or a change. |

```bash
nats kv get octoflow_names repo.nixos.nixpkgs --raw   # 4542716
nats kv ls octoflow_names
```

## Status counts

The KV bucket `octoflow_status` holds the counts behind
[`GET /status`](sse-api.md#status). The first writer creates it.

| Setting | Value |
|---|---|
| Storage | file |
| Replicas | `NATS_STREAM_REPLICAS` |
| Entry TTL | 48 hours |
| History | 1 |

| Key | Writer | Value |
|---|---|---|
| `events.<YYYY-MM-DD>T<HH>` | dispatcher | JSON array of 60 counts, one per minute of that UTC hour: primary events by store time |
| `refused.<YYYY-MM-DD>T<HH>` | webhook receiver | the same shape: refused deliveries by request time |

A writer collects its counts in memory and writes each hour it touched
at most every 5 seconds, so the write rate does not follow the event
rate. Each write is a compare-and-set on the key revision: two writers
of one key, for example two dispatchers, add up. The SSE server only
reads, 50 keys per answer, and never creates the bucket.

## Filling the buckets again

The name index, the heads of pull requests and the status counts are
indexes of the stream, not records. `octoflow reindex` reads every
primary event of the stream once and writes them again:

```bash
octoflow reindex --dry-run   # reindex: events=198773 heads=4120 minutes=1438 raised=0
octoflow reindex
```

| Bucket | What reindex writes | What it cannot |
|---|---|---|
| `octoflow_names` | each name of an organization, repository and sender in the stream | a name whose events are older than the stream keeps |
| `octoflow_heads` | the head commit of each stored `pull_request` event | the pr copies of CI events that were dispatched before; only later CI events get the link |
| `octoflow_status` | the events of each minute of the last 24 hours, by store time | the refused deliveries: the receiver does not store them |

Use it after a bucket was deleted, and once after an upgrade from a
version that did not keep these buckets. It publishes nothing, so it
can run beside the dispatcher. For the counts it writes a minute only
when the stream has more events than the bucket says, and it leaves the
newest two minutes to the dispatcher, so no event is counted twice and
a second run changes nothing.
