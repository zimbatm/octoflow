#!/usr/bin/env bash
#
# wait-for.sh - Wait for a TCP service to be available
#
# Usage: wait-for.sh [host:]port command [args...]
#
# Examples:
#   wait-for.sh 8080 echo "Port 8080 is ready"
#   wait-for.sh localhost:4222 nats pub test "hello"
#   wait-for.sh nats:4222 ./my-service
#
# This is a pure bash implementation using /dev/tcp
#

set -e

# Default timeout in seconds
TIMEOUT=${WAIT_FOR_TIMEOUT:-30}
INTERVAL=${WAIT_FOR_INTERVAL:-1}

# Parse arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 [host:]port command [args...]"
    echo "Environment variables:"
    echo "  WAIT_FOR_TIMEOUT - timeout in seconds (default: 30)"
    echo "  WAIT_FOR_INTERVAL - check interval in seconds (default: 1)"
    exit 1
fi

# Parse host:port
ADDR=$1
shift

# Extract host and port
if [[ "$ADDR" == *:* ]]; then
    HOST=${ADDR%:*}
    PORT=${ADDR#*:}
else
    HOST="localhost"
    PORT=$ADDR
fi

# Validate port number
if ! [[ "$PORT" =~ ^[0-9]+$ ]]; then
    echo "Error: Invalid port number: $PORT"
    exit 1
fi

# Function to check if port is open using /dev/tcp
check_port() {
    local host=$1
    local port=$2
    
    # Try to open a connection to the port
    # Using a subshell with timeout to prevent hanging
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
        exec 3<&-  # Close the file descriptor
        exec 3>&-
        return 0
    else
        return 1
    fi
}

echo "Waiting for $HOST:$PORT to be available..."

# Wait for the service
elapsed=0
while ! check_port "$HOST" "$PORT"; do
    if [[ $elapsed -ge "$TIMEOUT" ]]; then
        echo "Timeout: $HOST:$PORT did not become available within $TIMEOUT seconds"
        exit 1
    fi
    
    sleep "$INTERVAL"
    elapsed=$((elapsed + INTERVAL))
    
    # Show progress every few seconds
    if [ $((elapsed % 5)) -eq 0 ]; then
        echo "Still waiting for $HOST:$PORT... ($elapsed/$TIMEOUT seconds)"
    fi
done

echo "$HOST:$PORT is available!"

# Execute the command
exec "$@"
