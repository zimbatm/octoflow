#!/usr/bin/env bash
# Manual migration script for NATS streams
# This script migrates from the old stream structure to the new raw/ordered streams

set -euo pipefail

echo "NATS Stream Migration Script"
echo "============================"
echo ""
echo "This script will migrate your NATS streams to the new structure:"
echo "  - GITHUB_EVENTS_RAW with subjects: github.events.raw.>"
echo "  - GITHUB_EVENTS_ORDERED with subjects: github.events.ordered.>"
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

# Check if old streams exist
if nats stream info GITHUB_EVENTS &> /dev/null; then
    echo "Found legacy stream GITHUB_EVENTS"
    read -p "Delete legacy stream GITHUB_EVENTS? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        nats stream rm GITHUB_EVENTS -f
        echo "Deleted GITHUB_EVENTS"
    fi
fi

if nats stream info GITHUB_EVENTS_RAW &> /dev/null; then
    echo ""
    echo "Found existing GITHUB_EVENTS_RAW stream"
    echo "Current configuration:"
    nats stream info GITHUB_EVENTS_RAW | grep -E "Subjects:|Storage:|Retention:|Replicas:"
    echo ""
    read -p "Delete and recreate GITHUB_EVENTS_RAW with new subjects? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        nats stream rm GITHUB_EVENTS_RAW -f
        echo "Deleted GITHUB_EVENTS_RAW"
        
        # Create new raw stream
        cat <<EOF | nats stream add GITHUB_EVENTS_RAW
github.events.raw.>
file
limits
168h0m0s
0
0
1
no
no
yes
EOF
        echo "Created GITHUB_EVENTS_RAW with subjects: github.events.raw.>"
    fi
else
    echo ""
    echo "GITHUB_EVENTS_RAW not found, it will be created automatically on service start"
fi

if nats stream info GITHUB_EVENTS_ORDERED &> /dev/null; then
    echo ""
    echo "Found existing GITHUB_EVENTS_ORDERED stream"
    echo "Current configuration:"
    nats stream info GITHUB_EVENTS_ORDERED | grep -E "Subjects:|Storage:|Retention:|Replicas:"
    echo ""
    read -p "Delete and recreate GITHUB_EVENTS_ORDERED? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        nats stream rm GITHUB_EVENTS_ORDERED -f
        echo "Deleted GITHUB_EVENTS_ORDERED"
        
        # Create new ordered stream
        cat <<EOF | nats stream add GITHUB_EVENTS_ORDERED
github.events.ordered.>
file
limits
168h0m0s
0
0
1
no
no
yes
EOF
        echo "Created GITHUB_EVENTS_ORDERED with subjects: github.events.ordered.>"
    fi
else
    echo ""
    echo "GITHUB_EVENTS_ORDERED not found, it will be created automatically on service start"
fi

echo ""
echo "Migration complete!"
echo ""
echo "Final stream configuration:"
nats stream list
echo ""
echo "You can now start the services with 'make run'"