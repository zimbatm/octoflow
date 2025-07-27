#!/usr/bin/env bash
set -euo pipefail

echo "Testing SSE connection..."

# Test basic connection
echo "1. Testing basic /events endpoint:"
curl -i -N \
  -H "Accept: text/event-stream" \
  -H "Cache-Control: no-cache" \
  http://localhost:8081/events 2>&1 | head -20

echo -e "\n2. Testing /stats endpoint:"
curl -i http://localhost:8081/stats

echo -e "\n3. Testing with event type filter:"
curl -i -N \
  -H "Accept: text/event-stream" \
  -H "Cache-Control: no-cache" \
  "http://localhost:8081/events?type=push" 2>&1 | head -20