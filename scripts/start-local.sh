#!/usr/bin/env bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}GitHub Organization Events System - Local Development${NC}"
echo ""

# Check for required environment variables
missing_vars=()

if [ -z "${S3_BUCKET:-}" ]; then
    missing_vars+=("S3_BUCKET")
fi

# Warn if webhook secret is not set
if [ -z "${WEBHOOK_SECRET:-}" ]; then
    echo -e "${YELLOW}Warning: WEBHOOK_SECRET not set. Webhook signature validation is disabled.${NC}"
    echo -e "${YELLOW}This is fine for development but NOT recommended for production.${NC}"
    echo ""
fi

# If using AWS S3 (not rustfs/LocalStack)
if [ -z "${S3_ENDPOINT:-}" ]; then
    if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
        echo -e "${YELLOW}Warning: AWS credentials not set. Will use default credential chain.${NC}"
    fi
fi

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo -e "${RED}Error: Missing required environment variables:${NC}"
    for var in "${missing_vars[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "Please set these variables or create a .env file from .env.example"
    exit 1
fi

# Check if NATS is already running
if lsof -i :4222 >/dev/null 2>&1; then
    echo -e "${YELLOW}Warning: NATS appears to be already running on port 4222${NC}"
    echo "Please stop it before running hivemind, or it will conflict."
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Create necessary directories
mkdir -p logs

echo -e "${GREEN}Starting all services with hivemind...${NC}"
echo ""
echo "Services:"
echo "  - NATS Server (port 4222, monitoring on 8222)"
echo "  - Webhook Receiver (port 8080)"
echo "  - S3 Worker (background)"
echo "  - SSE Server (port 8081)"
echo "  - Dashboard (port 8082)"
echo ""
echo "Endpoints:"
echo "  - Webhook:   http://localhost:8080/webhook"
echo "  - Health:    http://localhost:8080/health"
echo "  - SSE:       http://localhost:8081/events"
echo "  - Dashboard: http://localhost:8082"
echo "  - NATS Mon:  http://localhost:8222"
echo ""

# Start hivemind
exec hivemind