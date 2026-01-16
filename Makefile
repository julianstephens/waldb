.PHONY: build test lint fmt clean help install

# Variables
BINARY_NAME=waldb
BUILD_DIR=bin
CMD_DIR=./cmd/waldb
VERSION?=dev
COMMIT?=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE?=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)"

# Default target
all: build

## build: Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	@go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)
	@echo "Built $(BUILD_DIR)/$(BINARY_NAME)"

## install: Install the binary to $GOPATH/bin
install:
	@echo "Installing $(BINARY_NAME)..."
	@go install $(LDFLAGS) $(CMD_DIR)
	@echo "Installed $(BINARY_NAME) to $(shell go env GOPATH)/bin"

## test: Run all tests
test:
	@echo "Running tests..."
	@go test -v ./...

## test-race: Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	@go test -race -v ./...

## test-coverage: Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@go test -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

## fmt: Format code
fmt:
	@echo "Formatting code..."
	@golangci-lint fmt
	@echo "Code formatted"

## lint: Run linters
lint:
	@echo "Running golangci-lint..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run --timeout=5m; \
	elif [ -x "$(shell go env GOPATH)/bin/golangci-lint" ]; then \
		$(shell go env GOPATH)/bin/golangci-lint run --timeout=5m; \
	else \
		echo "golangci-lint not found. Install it with:"; \
		echo "  curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b \$$(go env GOPATH)/bin v1.62.2"; \
		exit 1; \
	fi
	@echo "Linting complete"

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.out coverage.html
	@echo "Cleaned"

## help: Show this help message
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'
