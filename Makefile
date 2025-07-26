.PHONY: all build test clean fmt lint run-webhook run-s3-worker run-sse dev run-local run

# Build variables
GOBASE=$(shell pwd)
GOBIN=$(GOBASE)/bin
GOFILES=$(shell find . -name "*.go" -type f -not -path "./vendor/*")
GOCMD=go

# Build all services
all: build

build: webhook-receiver event-dispatcher s3-worker sse-server dashboard

webhook-receiver:
	$(GOCMD) build -o $(GOBIN)/webhook-receiver ./cmd/webhook-receiver

event-dispatcher:
	$(GOCMD) build -o $(GOBIN)/event-dispatcher ./cmd/event-dispatcher

s3-worker:
	$(GOCMD) build -o $(GOBIN)/s3-worker ./cmd/s3-worker

sse-server:
	$(GOCMD) build -o $(GOBIN)/sse-server ./cmd/sse-server

dashboard:
	$(GOCMD) build -o $(GOBIN)/dashboard ./cmd/dashboard


# Run services
run-webhook:
	$(GOCMD) run ./cmd/webhook-receiver

run-dispatcher:
	$(GOCMD) run ./cmd/event-dispatcher

run-s3-worker:
	$(GOCMD) run ./cmd/s3-worker

run-sse:
	$(GOCMD) run ./cmd/sse-server


run-dashboard:
	WEB_DIR=./web $(GOCMD) run ./cmd/dashboard


# Development
dev:
	nix develop

# Testing
test:
	$(GOCMD) test -v ./...

test-coverage:
	$(GOCMD) test -v -coverprofile=coverage.txt -covermode=atomic ./...

# Formatting and linting
fmt:
	$(GOCMD) fmt ./...
	$(GOCMD) mod tidy

lint:
	golangci-lint run

# Clean build artifacts
clean:
	$(GOCMD) clean
	rm -rf $(GOBIN)

# Dependencies
deps:
	$(GOCMD) mod download
	$(GOCMD) mod tidy

# NATS local development
nats-server:
	nats-server -js

# Generate mocks (if using mockgen)
mocks:
	$(GOCMD) generate ./...

# Run all services locally with hivemind
run:
	./scripts/start-local.sh

# Alias for backwards compatibility
run-local: run
