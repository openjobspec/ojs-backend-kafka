# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial Kafka-backed OpenJobSpec server with hybrid architecture (Kafka for durability, Redis for state).
- Snappy compression and leader acknowledgment for Kafka messages.
- Custom partitioner for queue-based topic routing.
- Full OJS conformance support (levels 0–4).
- Docker Compose setup with Kafka 3.7 and Redis 7.
- Project governance files (`CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`).
