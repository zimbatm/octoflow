#!/usr/bin/env bash
# Test script to send a sample GitHub webhook event

set -euo pipefail

# Default values
WEBHOOK_URL="${WEBHOOK_URL:-http://localhost:8080/webhook}"
WEBHOOK_SECRET="${WEBHOOK_SECRET:-}"  # Optional - if empty, no signature will be sent

# Sample GitHub webhook payload
PAYLOAD='{
  "action": "opened",
  "pull_request": {
    "id": 1,
    "number": 1,
    "state": "open",
    "title": "Test PR",
    "created_at": "2025-01-26T12:00:00Z",
    "updated_at": "2025-01-26T12:00:00Z",
    "merged": false
  },
  "repository": {
    "full_name": "test-org/test-repo",
    "owner": {
      "login": "test-org"
    }
  },
  "sender": {
    "login": "test-user"
  }
}'

# Generate signature if secret is provided
if [ -n "$WEBHOOK_SECRET" ]; then
  SIGNATURE=$(echo -n "$PAYLOAD" | openssl dgst -sha256 -hmac "$WEBHOOK_SECRET" | sed 's/^.* //')
  SIGNATURE_HEADER="-H \"X-Hub-Signature-256: sha256=$SIGNATURE\""
else
  SIGNATURE_HEADER=""
  echo "WARNING: No webhook secret configured - sending without signature"
fi

# Send webhook
echo "Sending test webhook to $WEBHOOK_URL"
if [ -n "$WEBHOOK_SECRET" ]; then
  curl -X POST "$WEBHOOK_URL" \
    -H "Content-Type: application/json" \
    -H "X-GitHub-Event: pull_request" \
    -H "X-GitHub-Delivery: test-$(date +%s)" \
    -H "X-Hub-Signature-256: sha256=$SIGNATURE" \
    -H "X-GitHub-Hook-ID: 12345" \
    -d "$PAYLOAD" \
    -w "\nHTTP Status: %{http_code}\n"
else
  curl -X POST "$WEBHOOK_URL" \
    -H "Content-Type: application/json" \
    -H "X-GitHub-Event: pull_request" \
    -H "X-GitHub-Delivery: test-$(date +%s)" \
    -H "X-GitHub-Hook-ID: 12345" \
    -d "$PAYLOAD" \
    -w "\nHTTP Status: %{http_code}\n"
fi