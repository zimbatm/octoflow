# Start NATS server with JetStream
nats: nats-server -js -m 8222

# Wait for NATS to be ready, then start webhook receiver
webhook: ./scripts/wait-for.sh 4222 go run ./cmd/webhook-receiver

# Wait for NATS to be ready, then start event dispatcher
dispatcher: ./scripts/wait-for.sh 4222 go run ./cmd/event-dispatcher

# Wait for NATS to be ready, then start S3 worker
# s3-worker: ./scripts/wait-for.sh 4222 go run ./cmd/s3-worker  # Disabled - needs Parquet schema fixes

# Wait for NATS to be ready, then start SSE server
sse: ./scripts/wait-for.sh 4222 go run ./cmd/sse-server

# Start the dashboard web server
dashboard: WEB_DIR=./web go run ./cmd/dashboard

# SSH tunnels for exposing webhook to internet
tunnel-cloudflare: cloudflared tunnel run --url http://localhost:8080 6429956a-91d2-40ce-a576-ba3974523d41
