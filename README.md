# waldb

A Write-Ahead Log (WAL) based key-value database written in Go. It provides durability and atomicity for key-value operations using a simple and efficient design.

## Features

- Write-ahead logging for durability
- Simple key-value operations
- Batch operations support
- Database snapshots
- Health checks and repair utilities

## Documentation

- [Design Document](docs/DESIGN.md) - Architecture and design decisions

## Installation

### From Source

```bash
git clone https://github.com/julianstephens/waldb.git
cd waldb
go build -o waldb ./cmd/waldb
```

### From Release

Download pre-built binaries from the [releases page](https://github.com/julianstephens/waldb/releases).

## Usage

```bash
# Initialize a new database
waldb init /path/to/db

# Store a key-value pair
waldb put mykey myvalue

# Retrieve a value
waldb get mykey

# Delete a key
waldb del mykey

# Show database statistics
waldb stats

# Check database health
waldb doctor

# Repair a database
waldb repair /path/to/db
```

For detailed command help:

```bash
waldb --help
waldb <command> --help
```

## Development

### Prerequisites

- Go 1.25 or later
- golangci-lint (for linting)

### Building

```bash
go build -o waldb ./cmd/waldb
```

### Testing

Run all tests:

```bash
go test ./...
```

Run unit tests:

```bash
go test ./internal/waldb/... ./cmd/...
```

Run E2E tests:

```bash
go test ./internal/tests/e2e/...
```

Run tests with race detection:

```bash
go test -race ./...
```

Run tests with coverage:

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Linting

Format code:

```bash
golangci-lint fmt
```

Run golangci-lint:

```bash
golangci-lint run
```

### Using Make (Optional)

A Makefile is provided for convenience:

```bash
make build    # Build the binary
make test     # Run tests
make lint     # Run linters
make fmt      # Format code
make clean    # Clean build artifacts
```

## Project Structure

```
waldb/
├── cmd/
│   └── waldb/          # Main CLI application
├── internal/
│   ├── waldb/          # Core database implementation
│   ├── cli/            # Command handlers
│   ├── testutil/       # Test helpers and mocks
│   └── tests/e2e/      # End-To-End tests
├── docs/               # Documentation
├── .github/
│   └── workflows/      # CI/CD workflows
├── go.mod
├── go.sum
├── Makefile
├── LICENSE
└── README.md
```

## License

MIT License - see [LICENSE](LICENSE) for details.
