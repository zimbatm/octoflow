.PHONY: all build test test-race test-coverage lint fmt run clean nats-server

all: build

build:
	go build -o bin/octoflow ./cmd/octoflow
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

test-coverage:
	go test -coverprofile=coverage.txt -covermode=atomic ./...

lint:
	golangci-lint run

fmt:
	gofmt -w .
	go mod tidy

# all services as separate processes under hivemind, see Procfile
run:
	./scripts/start-local.sh

nats-server:
	nats-server -c nats.conf

clean:
	go clean
	rm -rf bin coverage.txt
