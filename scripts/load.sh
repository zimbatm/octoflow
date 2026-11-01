#!/usr/bin/env bash
# Rough local throughput: N deliveries through the receiver, and the time
# until the dispatcher fanned them all out. Needs the devshell. Usage:
#   scripts/load.sh [deliveries] [parallel]
set -u
N=${1:-3000}; P=${2:-16}
cd "$(dirname "$0")/.." || exit 1
D=$(mktemp -d)
trap 'rm -rf "$D"' EXIT
rm -rf $D/js; mkdir -p $D/js
printf "port: 4399\nmax_payload: 33554432\njetstream { store_dir: %s/js }\n" $D > $D/nats.conf
nats-server -c $D/nats.conf > $D/nats.log 2>&1 &
NATS=$!
until nc -z 127.0.0.1 4399 2>/dev/null; do sleep 0.2; done
go build -o $D/octoflow ./cmd/octoflow || exit 1
WEBHOOK_ALLOW_UNSIGNED=true NATS_URL=nats://127.0.0.1:4399 HTTP_LISTEN_ADDR=127.0.0.1:28480 METRICS_LISTEN_ADDR=127.0.0.1:29480 WEB_DIR=./web LOG_LEVEL=warn $D/octoflow all > $D/octoflow.log 2>&1 &
OF=$!
until curl -sf http://127.0.0.1:28480/health >/dev/null; do kill -0 $OF 2>/dev/null || { echo "octoflow died"; cat $D/octoflow.log; kill $NATS; exit 1; }; sleep 0.2; done
cat > $D/payload.json <<'JSON'
{"action":"opened","pull_request":{"number":7,"user":{"id":2},"assignees":[{"id":3},{"id":4}],"requested_reviewers":[{"id":5}],"title":"load test"},"repository":{"id":111,"full_name":"numtide/x","owner":{"login":"numtide","id":222}},"organization":{"login":"numtide","id":222},"sender":{"login":"zimbatm","id":2}}
JSON
metric() { curl -s http://127.0.0.1:29480/metrics | grep "^$1" | awk '{s+=$NF} END{print s+0}'; }
echo "deliveries=$N parallel=$P payload=$(wc -c < $D/payload.json) bytes"
t0=$(date +%s.%N)
seq $N | xargs -P $P -I{} curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:28480/webhook -H 'Content-Type: application/json' -H 'X-GitHub-Event: pull_request' -H 'X-GitHub-Delivery: load-{}' --data-binary @$D/payload.json > $D/codes.txt
t1=$(date +%s.%N)
sort $D/codes.txt | uniq -c | tr '\n' ' '; echo
while [ "$(metric 'octoflow_dispatched_events_total{status="ok"}')" -lt "$N" ]; do sleep 0.1; done
t2=$(date +%s.%N)
awk -v n=$N -v a=$t0 -v b=$t1 -v c=$t2 'BEGIN{printf "receiver: %.1f deliveries/s (%.2fs)\ndispatcher caught up %.2fs after the last delivery; end to end %.1f events/s\n", n/(b-a), b-a, c-b, n/(c-a)}'
echo "stream: $(nats -s nats://127.0.0.1:4399 stream info GITHUB_EVENTS_RAW 2>/dev/null | grep -E 'Messages:|Bytes:' | tr -s ' ' | tr '\n' ' ')"
curl -s http://127.0.0.1:29480/metrics | grep -E 'octoflow_webhook_request_duration_seconds_sum|octoflow_webhook_request_duration_seconds_count|octoflow_nats_publish_duration_seconds_(sum|count)' | awk '{printf "%s %s\n", $1, $2}'
kill $OF; wait $OF 2>/dev/null; kill $NATS
grep -c 'level=ERROR' $D/octoflow.log | sed 's/^/errors in log: /'
