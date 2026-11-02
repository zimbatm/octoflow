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

# nats, rustfs and octoflow all under hivemind, see Procfile; needs .env
run:
	@test -f .env || { echo "copy .env.example to .env first"; exit 1; }
	hivemind

nats-server:
	nats-server -c nats.conf

clean:
	go clean
	rm -rf bin coverage.txt
