# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0](https://github.com/openjobspec/ojs-backend-kafka/compare/v0.1.0...v0.2.0) (2026-02-28)


### Features

* add consumer group rebalance handling ([e2eb93c](https://github.com/openjobspec/ojs-backend-kafka/commit/e2eb93c56b8a6fb526c064838462e1c94c9d2828))
* add health check readiness probe ([242253c](https://github.com/openjobspec/ojs-backend-kafka/commit/242253c777843f97e45181a0dcbf29887dd500d0))
* implement graceful shutdown with drain timeout ([7699d21](https://github.com/openjobspec/ojs-backend-kafka/commit/7699d213b2f696906fde7ad591922e7b1ec4cfe1))


### Bug Fixes

* correct offset commit on graceful shutdown ([f11e827](https://github.com/openjobspec/ojs-backend-kafka/commit/f11e82718b820eaef56b84c99d058e76b58e1581))
* correct retry delay calculation for exponential backoff ([7334e0a](https://github.com/openjobspec/ojs-backend-kafka/commit/7334e0a686d9d36001e5c80f5895f204507d15ad))

## [Unreleased]

### Added
- Initial Kafka-backed OpenJobSpec server with hybrid architecture (Kafka for durability, Redis for state).
- Snappy compression and leader acknowledgment for Kafka messages.
- Custom partitioner for queue-based topic routing.
- Full OJS conformance support (levels 0–4).
- Docker Compose setup with Kafka 3.7 and Redis 7.
- Project governance files (`CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`).
