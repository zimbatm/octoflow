#!/usr/bin/env bash
set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting GitHub Organization Events System Test${NC}"
echo ""

# Start services in background
echo "Starting services..."
hivemind &
HIVEMIND_PID=$!

# Wait for services to start
echo "Waiting for services to be ready..."
sleep 10

# Check if services are running
if ! curl -s http://localhost:8080/health > /dev/null; then
    echo -e "${RED}Webhook receiver is not responding${NC}"
    kill $HIVEMIND_PID 2>/dev/null || true
    exit 1
fi

echo -e "${GREEN}✓ Webhook receiver is healthy${NC}"


# Send test webhook
echo ""
echo "Sending test webhook..."
./scripts/test-webhook.sh

# Wait a bit for processing
sleep 3

# Check dashboard
echo ""
echo -e "${GREEN}Services are running!${NC}"
echo ""
echo "You can now:"
echo "  - View the dashboard at http://localhost:8080"
echo "  - Check NATS monitoring at http://localhost:8222"
echo "  - Send webhooks to http://localhost:8080/webhook"
echo ""
echo "Press Ctrl+C to stop all services"

# Wait for user to stop
wait $HIVEMIND_PID