# waldb

A Write-Ahead Log (WAL) based key-value database written in Go.

## Features

- Write-ahead logging for durability
- Simple key-value operations
- Batch operations support
- Database snapshots
- Health checks and repair utilities

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

- Go 1.23 or later
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
gofmt -w .
```

Run go vet:
```bash
go vet ./...
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
│   └── cli/            # Command handlers
├── docs/               # Documentation
├── scripts/            # Build and utility scripts
├── .github/
│   └── workflows/      # CI/CD workflows
├── go.mod
├── go.sum
├── Makefile
├── LICENSE
└── README.md
```

## CI/CD

### Pull Request Workflow

Every pull request triggers:
- Code formatting checks (`gofmt`)
- Static analysis (`go vet`, `golangci-lint`)
- Unit tests with race detection
- Code coverage reporting

See `.github/workflows/pr.yml` for details.

### Release Workflow

Releases are automated via Git tags:

1. Create and push a semantic version tag:
   ```bash
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

2. GitHub Actions will automatically:
   - Build binaries for multiple platforms (Linux, macOS, Windows)
   - Create a GitHub release
   - Upload build artifacts

Supported platforms:
- Linux (amd64, arm64)
- macOS (amd64, arm64)
- Windows (amd64)

See `.github/workflows/release.yml` for details.

## Contributing

Contributions are welcome! Please ensure:
- Code is properly formatted (`gofmt`)
- All tests pass (`go test ./...`)
- Linters pass (`golangci-lint run`)

## License

MIT License - see [LICENSE](LICENSE) for details.

## Documentation

- [Design Document](docs/design.md) - Architecture and design decisions
- [Testing Strategy](docs/testing.md) - Testing approach and guidelines

