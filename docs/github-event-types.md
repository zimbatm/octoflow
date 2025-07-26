# GitHub Webhook Event Types Reference

This document lists all GitHub webhook event types and their main objects/timestamps.

## Repository Events

### push
- **Main object**: `head_commit`
- **Timestamp fields**: `head_commit.timestamp`, `commits[].timestamp`
- **Key fields**: `ref`, `before`, `after`, `commits[]`

### pull_request
- **Main object**: `pull_request`
- **Timestamp fields**: `pull_request.created_at`, `pull_request.updated_at`
- **Actions**: `opened`, `closed`, `reopened`, `synchronize`, `labeled`, `unlabeled`, `assigned`, `unassigned`, `review_requested`, `ready_for_review`, `converted_to_draft`, `locked`, `unlocked`

### issues
- **Main object**: `issue`
- **Timestamp fields**: `issue.created_at`, `issue.updated_at`
- **Actions**: `opened`, `closed`, `reopened`, `edited`, `deleted`, `pinned`, `unpinned`, `assigned`, `unassigned`, `labeled`, `unlabeled`, `locked`, `unlocked`, `transferred`, `milestoned`, `demilestoned`

### issue_comment
- **Main object**: `comment`, `issue`
- **Timestamp fields**: `comment.created_at`, `comment.updated_at`
- **Actions**: `created`, `edited`, `deleted`

### commit_comment
- **Main object**: `comment`
- **Timestamp fields**: `comment.created_at`, `comment.updated_at`
- **Actions**: `created`

### create
- **Main object**: None (repository/branch/tag creation)
- **Timestamp fields**: Repository timestamps
- **Key fields**: `ref`, `ref_type` (branch/tag)

### delete
- **Main object**: None (branch/tag deletion)
- **Timestamp fields**: Repository timestamps
- **Key fields**: `ref`, `ref_type` (branch/tag)

### fork
- **Main object**: `forkee` (the new fork)
- **Timestamp fields**: `forkee.created_at`

### star (watch)
- **Main object**: `repository`
- **Timestamp fields**: `starred_at` (if available)
- **Actions**: `created`, `deleted`

### release
- **Main object**: `release`
- **Timestamp fields**: `release.created_at`, `release.published_at`
- **Actions**: `published`, `unpublished`, `created`, `edited`, `deleted`, `prereleased`, `released`

## Code Review Events

### pull_request_review
- **Main object**: `review`, `pull_request`
- **Timestamp fields**: `review.submitted_at`
- **Actions**: `submitted`, `edited`, `dismissed`

### pull_request_review_comment
- **Main object**: `comment`, `pull_request`
- **Timestamp fields**: `comment.created_at`, `comment.updated_at`
- **Actions**: `created`, `edited`, `deleted`

### check_run
- **Main object**: `check_run`
- **Timestamp fields**: `check_run.started_at`, `check_run.completed_at`
- **Actions**: `created`, `completed`, `rerequested`, `requested_action`

### check_suite
- **Main object**: `check_suite`
- **Timestamp fields**: `check_suite.created_at`, `check_suite.updated_at`
- **Actions**: `completed`, `requested`, `rerequested`

### status
- **Main object**: `commit`
- **Timestamp fields**: `created_at`, `updated_at`
- **States**: `pending`, `success`, `failure`, `error`

## Workflow Events

### workflow_run
- **Main object**: `workflow_run`
- **Timestamp fields**: `workflow_run.created_at`, `workflow_run.updated_at`
- **Actions**: `requested`, `completed`, `in_progress`

### workflow_dispatch
- **Main object**: `workflow`
- **Timestamp fields**: Event timestamp
- **Key fields**: `inputs`, `ref`

### workflow_job
- **Main object**: `workflow_job`
- **Timestamp fields**: `workflow_job.started_at`, `workflow_job.completed_at`
- **Actions**: `queued`, `in_progress`, `completed`, `waiting`

## Organization Events

### organization
- **Main object**: `organization`
- **Timestamp fields**: Event timestamp
- **Actions**: `deleted`, `renamed`, `member_added`, `member_removed`, `member_invited`

### team
- **Main object**: `team`
- **Timestamp fields**: `team.created_at`
- **Actions**: `created`, `deleted`, `edited`, `added_to_repository`, `removed_from_repository`

### member
- **Main object**: `member`
- **Timestamp fields**: Event timestamp
- **Actions**: `added`, `removed`, `edited`

### membership
- **Main object**: `team`, `member`
- **Timestamp fields**: Event timestamp
- **Actions**: `added`, `removed`

## Project Events

### project
- **Main object**: `project`
- **Timestamp fields**: `project.created_at`, `project.updated_at`
- **Actions**: `created`, `edited`, `closed`, `reopened`, `deleted`

### project_card
- **Main object**: `project_card`
- **Timestamp fields**: `project_card.created_at`, `project_card.updated_at`
- **Actions**: `created`, `moved`, `converted`, `edited`, `deleted`

### project_column
- **Main object**: `project_column`
- **Timestamp fields**: `project_column.created_at`, `project_column.updated_at`
- **Actions**: `created`, `edited`, `moved`, `deleted`

### milestone
- **Main object**: `milestone`
- **Timestamp fields**: `milestone.created_at`, `milestone.updated_at`
- **Actions**: `created`, `closed`, `opened`, `edited`, `deleted`

### label
- **Main object**: `label`
- **Timestamp fields**: Event timestamp
- **Actions**: `created`, `edited`, `deleted`

## Security Events

### security_advisory
- **Main object**: `security_advisory`
- **Timestamp fields**: `security_advisory.published_at`, `security_advisory.updated_at`
- **Actions**: `published`, `updated`, `performed`

### code_scanning_alert
- **Main object**: `alert`
- **Timestamp fields**: `alert.created_at`
- **Actions**: `created`, `reopened`, `closed`, `fixed`, `appeared_in_branch`, `closed_by_user`

### secret_scanning_alert
- **Main object**: `alert`
- **Timestamp fields**: `alert.created_at`
- **Actions**: `created`, `resolved`, `reopened`

### dependabot_alert
- **Main object**: `alert`
- **Timestamp fields**: `alert.created_at`
- **Actions**: `created`, `resolved`, `dismissed`, `reopened`

### branch_protection_rule
- **Main object**: `rule`
- **Timestamp fields**: Event timestamp
- **Actions**: `created`, `edited`, `deleted`

## Discussion Events

### discussion
- **Main object**: `discussion`
- **Timestamp fields**: `discussion.created_at`, `discussion.updated_at`
- **Actions**: `created`, `edited`, `deleted`, `pinned`, `unpinned`, `locked`, `unlocked`, `transferred`, `category_changed`, `answered`, `unanswered`, `labeled`, `unlabeled`

### discussion_comment
- **Main object**: `comment`, `discussion`
- **Timestamp fields**: `comment.created_at`, `comment.updated_at`
- **Actions**: `created`, `edited`, `deleted`

## Other Events

### gollum (Wiki)
- **Main object**: `pages[]`
- **Timestamp fields**: Event timestamp
- **Key fields**: `pages[].action` (created/edited)

### page_build
- **Main object**: `build`
- **Timestamp fields**: `build.created_at`
- **Status**: `built`, `errored`

### public
- **Main object**: `repository`
- **Timestamp fields**: Event timestamp
- **Note**: Triggered when repo visibility changes to public

### sponsorship
- **Main object**: `sponsorship`
- **Timestamp fields**: `sponsorship.created_at`
- **Actions**: `created`, `cancelled`, `edited`, `tier_changed`, `pending_cancellation`, `pending_tier_change`

### repository_dispatch
- **Main object**: Custom payload
- **Timestamp fields**: Event timestamp
- **Key fields**: `action`, `client_payload`

### deployment
- **Main object**: `deployment`
- **Timestamp fields**: `deployment.created_at`, `deployment.updated_at`
- **Actions**: `created`

### deployment_status
- **Main object**: `deployment_status`, `deployment`
- **Timestamp fields**: `deployment_status.created_at`
- **States**: `error`, `failure`, `inactive`, `in_progress`, `queued`, `pending`, `success`

### registry_package
- **Main object**: `package`
- **Timestamp fields**: `package.created_at`, `package.updated_at`
- **Actions**: `published`, `updated`

## Notes

This reference provides a comprehensive list of all GitHub webhook event types and their main objects. Each event type has specific fields and payloads that can be used for filtering and processing.