#!/usr/bin/env bash
# wait-for.sh [host:]port command...: runs the command once the port accepts
# connections, gives up after WAIT_FOR_TIMEOUT seconds (30).
set -eu
addr=$1; shift
host=${addr%:*}; port=${addr##*:}
[ "$host" = "$port" ] && host=localhost
for ((i = 0; i < ${WAIT_FOR_TIMEOUT:-30}; i++)); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
        exec "$@"
    fi
    sleep 1
done
echo "wait-for: $host:$port did not open" >&2
exit 1
