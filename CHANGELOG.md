# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-09-02

### Changed
- Upgraded gRPC, OpenTelemetry, Chi, and Go networking dependencies to patched
  releases and raised the minimum Go version to 1.25.
- Migrated native linting to pinned golangci-lint v2 and restored clean-cache
  standalone module compilation.
- Corrected workflow cancellation/effect fencing, unique replacement, cron
  deduplication, and Redis transition atomicity.
- Aligned release dependencies and hardened standalone CI, release artifacts,
  checksums, and provenance for the coordinated 0.5.0 train.

## [0.4.1] - 2026-04-21

### Fixed
- Closed a scheduler context cancellation leak.
- Propagated previously ignored conversion, JSON marshaling, and Redis errors.

## [0.4.0] - 2026-04-20

### Added
- Initial Kafka-backed OpenJobSpec server with hybrid architecture (Kafka for durability, Redis for state).
- Snappy compression and leader acknowledgment for Kafka messages.
- Custom partitioner for queue-based topic routing.
- Full OJS conformance support (levels 0–4).
- Docker Compose setup with Kafka 3.7 and Redis 7.
- Project governance files (`CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`).
