# Public events

Octoflow has two ingests. The webhook receiver is complete: GitHub sends
every subscribed event of the organization, and a failed delivery can
be redelivered. `public-ingest` exists for repositories where no
webhook can be installed, such as `NixOS/nixpkgs`, and it is not
complete. This page explains what that means and why.

## The two sources

The [Events API](https://docs.github.com/en/rest/activity/events)
returns a repository's recent public events. GitHub documents it as
"not built to serve real-time use cases": it keeps only the newest 300
events for at most 30 days, and delays go from 30 seconds to 6 hours.

[GH Archive](https://www.gharchive.org/) records the same public
timeline into one file per hour, published about ten minutes after the
hour. It is built from the Events API, so it has the same gaps, and
some hours are missing for good.

## What was measured

On 2026-10-14, for `NixOS/nixpkgs`:

| Source | Found | Compared with |
|---|---|---|
| Events API | 277 events in 24 hours | 1537 issues and pull requests changed in those 24 hours |
| GH Archive | 6 to 7 events per hour | the same |

Most activity never appears. The numbers are a snapshot; the ratio is
the point.

## What the payloads contain

Since 2025-10-07 the Events API trims payloads
([changelog](https://github.blog/changelog/2025-08-08-upcoming-changes-to-github-events-api-payloads/)).
A `pull_request` event carries `id`, `number`, `url`, `base` and `head`
of the pull request, without title, state or author. A `push` event has
no commit list. A consumer that needs more fetches the object from the
API.

The Events API covers 16 event types. Webhook-only types such as
`check_run`, `status` or `workflow_run` never arrive this way.

## How the events are rewritten

Public events are published on the same subjects, in the same shape, so
that one consumer serves both ingests:

- The type name is converted: `PullRequestReviewCommentEvent` becomes
  `pull_request_review_comment`.
- The payload gains the `repository`, `sender` and `organization`
  objects that webhooks carry, so the routing ids are the same.
- `id` is `gh-event-<GitHub event id>` from both sources, so the stream
  drops an event that both deliver within 24 hours.
- `metadata.source` says which source delivered it,
  `metadata.created_at` when GitHub recorded it; `received_at` is when
  Octoflow read it.

## How to use them

Treat a public event as a hint that something changed, not as a record
of everything that changed. Poll or fetch from the API for the full
state when it matters. Do not list a repository in `PUBLIC_REPOS` that
also delivers webhooks: the two sources use different ids, so every
event would arrive twice.

## Why keep it

For nixpkgs there is no alternative, and a partial live feed still
drives useful tooling: reacting to a new pull request minutes after it
opens, or watching a label change, works most of the time. The
limitations are stated here so that nothing downstream assumes more.
