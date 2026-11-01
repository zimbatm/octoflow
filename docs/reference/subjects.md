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
| `github.events.user.{user_id}.{event_type}` | involved user: sender, PR or issue author, assignees, requested reviewers, comment and review authors, team members |
| `github.events.pr.{repo_id}.{number}.{event_type}` | `pull_request`, `pull_request_review` and `pull_request_review_comment` event |
| `github.events.issue.{repo_id}.{number}.{event_type}` | `issues` and `issue_comment` event |

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
| Retention | limits policy, discard old, max age 7 days |
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
`gharchive.next_hour`.
