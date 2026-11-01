# Start NATS server with JetStream
nats: nats-server -c nats.conf

# Local S3 store; the s3 service creates the bucket on start
s3-store: mkdir -p .data/s3 && RUSTFS_ACCESS_KEY=$AWS_ACCESS_KEY_ID RUSTFS_SECRET_KEY=$AWS_SECRET_ACCESS_KEY exec rustfs server --address 127.0.0.1:9000 .data/s3

# Each service waits for NATS; the binary is the same for all of them
webhook: ./scripts/wait-for.sh 4222 go run ./cmd/octoflow webhook
dispatcher: ./scripts/wait-for.sh 4222 go run ./cmd/octoflow dispatcher
s3: ./scripts/wait-for.sh 4222 ./scripts/wait-for.sh 9000 sh -c "./scripts/create-bucket.sh && exec go run ./cmd/octoflow s3"
sse: ./scripts/wait-for.sh 4222 go run ./cmd/octoflow sse
dashboard: go run ./cmd/octoflow dashboard
