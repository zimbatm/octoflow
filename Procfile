# Start NATS server with JetStream
nats: nats-server -js -m 8222

# Wait for NATS to be ready, then start webhook receiver
webhook: ./scripts/wait-for.sh 4222 gorefresh ./cmd/webhook-receiver

# Wait for NATS to be ready, then start reorder worker
reorder: ./scripts/wait-for.sh 4222 gorefresh ./cmd/reorder-worker

# Wait for NATS to be ready, then start S3 worker
s3-worker: ./scripts/wait-for.sh 4222 gorefresh ./cmd/s3-worker

# Wait for NATS to be ready, then start SSE server
sse: ./scripts/wait-for.sh 4222 gorefresh ./cmd/sse-server

# Start the dashboard web server
dashboard: WEB_DIR=./web gorefresh ./cmd/dashboard

# SSH tunnels for exposing webhook to internet (uncomment one):
tunnel-localhost: ./scripts/wait-for.sh 8080 ssh -R 80:localhost:8080 localhost.run
#tunnel-pinggy: ./scripts/wait-for.sh 8080 ssh -R 80:localhost:8080 a.pinggy.io
#tunnel-serveo: ./scripts/wait-for.sh 8080 ssh -R 80:localhost:8080 serveo.net
