#!/usr/bin/env bash
# Script to clear all NATS streams

set -euo pipefail

echo "NATS Stream Cleanup Script"
echo "========================="
echo ""
echo "This script will DELETE ALL NATS streams in your system."
echo "This action cannot be undone!"
echo ""

# Check if nats CLI is available
if ! command -v nats &> /dev/null; then
    echo "Error: 'nats' CLI not found. Please install NATS CLI tools."
    exit 1
fi

# Show current streams
echo "Current streams:"
nats stream list
echo ""

read -p "Are you sure you want to delete ALL streams? (yes/no) " -r
echo
if [[ ! $REPLY =~ ^yes$ ]]; then
    echo "Aborted."
    exit 0
fi

# Get list of all streams
streams=$(nats stream list -n 2>/dev/null | grep -v "No Streams defined" || true)

if [ -z "$streams" ]; then
    echo "No streams found to delete."
    exit 0
fi

# Delete each stream
echo "Deleting streams..."
while IFS= read -r stream; do
    if [ -n "$stream" ]; then
        echo "Deleting stream: $stream"
        nats stream rm "$stream" -f
    fi
done <<< "$streams"

echo ""
echo "All streams have been deleted."
echo ""
echo "Final state:"
nats stream list