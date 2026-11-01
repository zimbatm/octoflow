#!/usr/bin/env bash
#
# Posts a signed sample pull_request delivery to a receiver.
# The payload carries numeric ids (org 222, repo 111, PR 7, sender 2), so
# the dispatcher routes it to the repo, user and pr subjects.
#
#   WEBHOOK_SECRET=change-me ./scripts/test-webhook.sh
#   WEBHOOK_URL=https://events.example.org/webhook WEBHOOK_SECRET=... ./scripts/test-webhook.sh
#
set -euo pipefail

WEBHOOK_URL="${WEBHOOK_URL:-http://localhost:8080/webhook}"
if [ -z "${WEBHOOK_SECRET:-}" ]; then
    echo "WEBHOOK_SECRET is required: the receiver only accepts signed deliveries" >&2
    exit 1
fi

PAYLOAD='{
  "action": "opened",
  "number": 7,
  "pull_request": {
    "id": 1,
    "number": 7,
    "state": "open",
    "title": "Test PR",
    "user": {"id": 2, "login": "test-user"},
    "created_at": "2026-10-14T12:00:00Z",
    "merged": false
  },
  "repository": {
    "id": 111,
    "name": "test-repo",
    "full_name": "test-org/test-repo",
    "owner": {"id": 222, "login": "test-org"}
  },
  "organization": {"id": 222, "login": "test-org"},
  "sender": {"id": 2, "login": "test-user"}
}'

SIGNATURE=$(printf '%s' "$PAYLOAD" | openssl dgst -sha256 -hmac "$WEBHOOK_SECRET" | sed 's/^.* //')

echo "Sending test webhook to $WEBHOOK_URL"
curl -sS -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-GitHub-Event: pull_request" \
  -H "X-GitHub-Delivery: test-$(date +%s)" \
  -H "X-GitHub-Hook-ID: 12345" \
  -H "X-Hub-Signature-256: sha256=$SIGNATURE" \
  --data-binary "$PAYLOAD" \
  -w "\nHTTP Status: %{http_code}\n"
