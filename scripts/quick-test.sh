#!/usr/bin/env bash
set -euo pipefail

echo "Quick test of the GitHub Organization Events System"
echo ""

# Make sure webhook secret is not required for this test
unset WEBHOOK_SECRET

# Send a test webhook
echo "Sending test webhook..."
curl -X POST http://localhost:8080/webhook \
  -H "Content-Type: application/json" \
  -H "X-GitHub-Event: pull_request" \
  -H "X-GitHub-Delivery: test-$(date +%s)" \
  -H "X-GitHub-Hook-ID: 12345" \
  -d '{
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
  }' \
  -w "\nHTTP Status: %{http_code}\n" || echo "Failed to send webhook"

echo ""
echo "Check the hivemind output to see if the event was processed!"