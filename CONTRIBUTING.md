# Contributing to ojs-backend-kafka

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing.

## Getting Started

### Prerequisites

- Go 1.22+
- Docker (for Kafka + Redis)

### Local Development Setup

```bash
# Clone the repository
git clone https://github.com/openjobspec/ojs-backend-kafka.git
cd ojs-backend-kafka

# Start dependencies
docker compose -f docker/docker-compose.yml up kafka redis -d

# Build
make build

# Run tests
make test

# Run linter
make lint
```

## Development Workflow

1. Fork the repository
2. Create a feature branch from `main`: `git checkout -b feat/my-feature`
3. Make your changes
4. Run tests and linter: `make test && make lint`
5. Commit using [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation changes
   - `refactor:` for code refactoring
   - `test:` for test additions/changes
   - `chore:` for maintenance tasks
6. Push to your fork and open a Pull Request

## Code Guidelines

- Follow existing code patterns and conventions
- Add tests for new functionality
- Use `log/slog` for structured logging (not `log.Printf`)
- Use Redis Lua scripts for multi-step atomic operations (see `internal/state/scripts.go`)
- Handle errors explicitly; do not silently ignore them

## Architecture

The project uses a three-layer architecture:

- **`internal/api/`** -- HTTP handlers (chi router)
- **`internal/core/`** -- Business logic interfaces and types
- **`internal/kafka/`** -- Kafka-specific backend implementation
- **`internal/state/`** -- Redis state store with atomic Lua scripts
- **`internal/scheduler/`** -- Background job promotion and reaping

See the [README](README.md) for the full architecture diagram.

## Testing

```bash
make test           # Run all unit tests with race detector
make lint           # Run go vet + golangci-lint
```

## Reporting Issues

- Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.yml) for bugs
- Use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.yml) for new ideas

## License

By contributing, you agree that your contributions will be licensed under the Apache-2.0 License.
