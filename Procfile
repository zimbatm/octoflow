# Start NATS server with JetStream
nats: nats-server -js -m 8222

# Local S3 store; the s3-worker creates the bucket on start
s3: mkdir -p .data/s3 && RUSTFS_ACCESS_KEY=$AWS_ACCESS_KEY_ID RUSTFS_SECRET_KEY=$AWS_SECRET_ACCESS_KEY exec rustfs server --address 127.0.0.1:9000 .data/s3

# Wait for NATS to be ready, then start webhook receiver
webhook: ./scripts/wait-for.sh 4222 go run ./cmd/webhook-receiver

# Wait for NATS to be ready, then start event dispatcher
dispatcher: ./scripts/wait-for.sh 4222 go run ./cmd/event-dispatcher

# Wait for NATS and rustfs, then start S3 worker
s3-worker: ./scripts/wait-for.sh 4222 ./scripts/wait-for.sh 9000 sh -c "./scripts/create-bucket.sh && exec go run ./cmd/s3-worker"

# Wait for NATS to be ready, then start SSE server
sse: ./scripts/wait-for.sh 4222 go run ./cmd/sse-server

# Start the dashboard web server
dashboard: WEB_DIR=./web go run ./cmd/dashboard

# SSH tunnels for exposing webhook to internet
tunnel-cloudflare: cloudflared tunnel run --url http://localhost:8080 6429956a-91d2-40ce-a576-ba3974523d41
