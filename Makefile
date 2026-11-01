.PHONY: all build test test-coverage lint fmt run clean nats-server \
	run-webhook run-dispatcher run-s3-worker run-sse run-dashboard run-public-ingest

SERVICES = webhook-receiver event-dispatcher s3-worker sse-server dashboard public-ingest

all: build

build:
	for s in $(SERVICES); do go build -o bin/$$s ./cmd/$$s || exit 1; done
	go build ./...

test:
	go test ./...

test-coverage:
	go test -coverprofile=coverage.txt -covermode=atomic ./...

lint:
	golangci-lint run

fmt:
	gofmt -w .
	go mod tidy

# all services under hivemind, see Procfile
run:
	./scripts/start-local.sh

nats-server:
	nats-server -js

run-webhook:
	go run ./cmd/webhook-receiver
run-dispatcher:
	go run ./cmd/event-dispatcher
run-s3-worker:
	go run ./cmd/s3-worker
run-sse:
	go run ./cmd/sse-server
run-dashboard:
	WEB_DIR=./web go run ./cmd/dashboard
run-public-ingest:
	go run ./cmd/public-ingest

clean:
	go clean
	rm -rf bin coverage.txt
