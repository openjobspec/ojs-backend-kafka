# ojs-backend-kafka

[![CI](https://github.com/openjobspec/ojs-backend-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/openjobspec/ojs-backend-kafka/actions/workflows/ci.yml)
![Conformance](https://github.com/openjobspec/ojs-backend-kafka/raw/main/.github/badges/conformance.svg)
[![Security](https://github.com/openjobspec/ojs-backend-kafka/actions/workflows/security.yml/badge.svg)](https://github.com/openjobspec/ojs-backend-kafka/actions/workflows/security.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/openjobspec/ojs-backend-kafka)](https://goreportcard.com/report/github.com/openjobspec/ojs-backend-kafka)
[![Go Version](https://img.shields.io/github/go-mod/go-version/openjobspec/ojs-backend-kafka)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

An [Open Job Spec (OJS)](https://github.com/openjobspec/openjobspec) backend implementation using **Apache Kafka** for transport and event streaming, with a **Redis** sidecar state store for per-job lifecycle tracking.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    OJS HTTP API                         │
│  POST /ojs/v1/jobs  POST /ojs/v1/workers/fetch  ...    │
└──────────────────────┬──────────────────────────────────┘
                       │
              ┌────────▼────────┐
              │  KafkaBackend   │  implements core.Backend
              │  (orchestrator) │
              └──┬──────────┬───┘
                 │          │
    ┌────────────▼──┐  ┌───▼────────────┐
    │  State Store  │  │ Kafka Producer │
    │   (Redis)     │  │  (franz-go)    │
    │               │  │                │
    │ • Job state   │  │ • Queue topics │
    │ • Queues      │  │ • Events topic │
    │ • Visibility  │  │ • DLQ topics   │
    │ • Workflows   │  │ • Retry topics │
    │ • Cron        │  │                │
    └───────────────┘  └────────────────┘
```

### Why a hybrid architecture?

Kafka is a **log**, not a **queue**. OJS requires per-job acknowledgment, per-job state tracking, and an 8-state lifecycle—none of which Kafka supports natively. The solution:

- **State store (Redis)** handles all per-job lifecycle, queue ordering, visibility timeouts, unique jobs, workflows, and cron scheduling. This is what the HTTP API reads and writes for correctness and low latency.
- **Kafka** provides durable event streaming, horizontal scalability, and replay capability. Every job enqueue, completion, failure, and cancellation is published to Kafka topics.

### OJS-to-Kafka concept mapping

| OJS Concept | Kafka Mapping |
|---|---|
| Queue | Topic `ojs.queue.{name}` |
| Job enqueue | Produce to queue topic + state store write |
| Job fetch | Read from state store (low-latency, per-job semantics) |
| Job ack | State store update + lifecycle event to `ojs.events` |
| Job nack | State store update + retry/DLQ topic |
| Scheduled jobs | State store scheduled set, promoted by background scheduler |
| Dead letter | State store DLQ set + `ojs.dead.{queue}` topic |
| Events | Dedicated `ojs.events` topic |
| Priority | Score-based ordering in state store available queue |

### Topic naming convention

```
ojs.queue.{name}              -- main job topic per OJS queue
ojs.queue.{name}.retry        -- retry topic per queue
ojs.dead.{name}               -- dead letter topic per queue
ojs.scheduled                 -- delayed jobs (informational)
ojs.events                    -- lifecycle events
```

## Trade-offs vs Redis/Postgres backends

### Strengths
- **Horizontal scalability**: Kafka partitions scale linearly with consumers
- **Durability**: Kafka replication provides fault tolerance beyond what Redis offers
- **Replay capability**: Reprocess historical jobs by resetting consumer offsets
- **Natural event streaming**: All lifecycle events available as a Kafka stream
- **Massive throughput**: 50,000+ jobs/second per partition
- **Decoupled consumers**: External services can consume job events independently

### Weaknesses
- **Higher latency**: Kafka produce adds ~5-10ms vs pure Redis
- **Operational complexity**: Requires Kafka cluster + Redis (two systems to manage)
- **No transactional enqueue**: Cannot atomically enqueue with application database writes
- **External state store required**: Kafka alone cannot track per-job lifecycle
- **Overkill for small workloads**: If throughput < 10,000 jobs/second, use Redis or Postgres backend

## Quick start

### Prerequisites

- Go 1.22+
- Docker (for Kafka + Redis)

### Run with Docker Compose

```bash
make docker-up       # Starts Kafka (KRaft) + Redis + OJS server
curl http://localhost:8080/ojs/manifest
make docker-down     # Stop everything
```

### Run locally

```bash
# Start Kafka and Redis (Docker)
docker compose -f docker/docker-compose.yml up kafka redis -d

# Build and run
make run
# or: KAFKA_BROKERS=localhost:9092 REDIS_URL=redis://localhost:6379 go run ./cmd/ojs-server
```

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `OJS_PORT` | `8080` | HTTP server port |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL (state store) |
| `OJS_KAFKA_USE_QUEUE_KEY` | `false` | Use queue name as partition key (instead of job type) |
| `OJS_KAFKA_EVENTS_ENABLED` | `true` | Publish lifecycle events to `ojs.events` topic |

## Build & test

```bash
make build          # Build server binary to bin/ojs-server
make test           # go test ./... -race -cover
make lint           # go vet ./...
make run            # Build and run (needs Kafka + Redis)
make docker-up      # Start server + Kafka + Redis via Docker Compose
make docker-down    # Stop Docker Compose
```

### Development with Hot Reload

```bash
make dev            # Local hot reload (requires air)
make docker-dev     # Docker Compose with hot reload
```

## Conformance

```bash
make conformance              # Run all conformance levels
make conformance-level-0      # Run specific level (0-4)
```

### Conformance support

| Level | Description | Status |
|---|---|---|
| 0 | Core (push, fetch, ack, nack, info, cancel) | Full support |
| 1 | Visibility timeout, heartbeat, dead letter | Full support |
| 2 | Scheduled jobs, cron | Full support |
| 3 | Workflows (chain, group, batch) | Full support |
| 4 | Unique jobs, rate limiting, priority | Full support |

## Project structure

```
ojs-backend-kafka/
├── cmd/ojs-server/main.go           # Server entrypoint
├── internal/
│   ├── api/                          # HTTP handlers (shared with Redis backend)
│   ├── core/                         # Core interfaces & types (shared)
│   ├── kafka/
│   │   ├── backend.go                # KafkaBackend implementing core.Backend
│   │   ├── producer.go               # Kafka message production
│   │   ├── consumer.go               # Kafka consumer (for external consumption)
│   │   ├── headers.go                # OJS attribute → Kafka header mapping
│   │   ├── codec.go                  # Job serialization/deserialization
│   │   └── partitioner.go            # Topic naming & partition key logic
│   ├── state/
│   │   ├── store.go                  # State store interface
│   │   └── redis.go                  # Redis-backed state store
│   ├── scheduler/
│   │   └── scheduler.go              # Background tasks (promote, reap, cron)
│   └── server/
│       ├── config.go                 # Environment-based configuration
│       └── server.go                 # HTTP router setup
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml            # Kafka (KRaft) + Redis + OJS server
├── Makefile
└── .github/workflows/
    ├── ci.yml
    └── conformance.yml
```

## Performance targets

| Metric | Target |
|---|---|
| Enqueue p99 | < 10ms (Kafka produce with acks=1) |
| Dequeue p99 | < 100ms (state store read) |
| Throughput | 50,000+ jobs/second per partition |
| Connected workers | Up to 10,000 (Kafka scales horizontally) |

## Observability

### OpenTelemetry

The server supports distributed tracing via OpenTelemetry. Set the following environment variable to enable:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

Traces are exported in OTLP format over gRPC. Compatible with Jaeger, Zipkin, Grafana Tempo, and any OTLP-compatible collector.

You can also use the legacy env vars `OJS_OTEL_ENABLED=true` and `OJS_OTEL_ENDPOINT` for explicit control.

## Production Deployment Notes

- **Rate limiting**: This server does not enforce request rate limits. Place a reverse proxy (e.g., Nginx, Envoy, or a cloud load balancer) in front of the server to add rate limiting in production.
- **Authentication**: Set `OJS_API_KEY` to require Bearer token auth on all endpoints. For local-only testing, set `OJS_ALLOW_INSECURE_NO_AUTH=true`.
- **TLS**: Terminate TLS at a reverse proxy or load balancer rather than at the application level.

## License

Apache-2.0


