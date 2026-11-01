# NATS with JetStream; nats.conf raises max_payload for 25 MB deliveries
nats: nats-server -c nats.conf

# Local S3 store; create-bucket.sh makes the bucket once it is up
s3-store: mkdir -p .data/s3 && RUSTFS_ACCESS_KEY=$AWS_ACCESS_KEY_ID RUSTFS_SECRET_KEY=$AWS_SECRET_ACCESS_KEY exec rustfs server --address 127.0.0.1:9000 .data/s3

# Every service in one process on :8080; s3 runs because .env sets S3_BUCKET
octoflow: ./scripts/wait-for.sh 4222 ./scripts/wait-for.sh 9000 sh -c "./scripts/create-bucket.sh && exec go run ./cmd/octoflow all"
