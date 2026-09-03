# Certification Remediation Audit

**Repository:** `ojs-backend-kafka`  
**Branch:** `refactor/clean-code-srp`  
**Scope:** this repository only; all changes remain unstaged  
**Public compatibility:** no route, protobuf, schema, topic/header, or OJS wire-format changes; no sibling-repository edits

## Deep-review findings resolved

| # | Finding | Resolution |
|---|---------|------------|
| 1 | Workflow cancellation fencing | `AtomicCancelWorkflow` now atomically changes a running workflow to `cancelled`, collects and retains every stable effect job ID, deletes pending/leased effects, and removes the workflow from the global pending index. Claim, create, and completion scripts fence cancelled workflows. `CancelWorkflow` cancels tracked jobs plus returned stable IDs, including a job created immediately before cancellation but not yet appended. Atomic fetch/promotion and state-checked requeue prevent cancelled effect jobs from being resurrected. |
| 2 | Workflow effects with unique `ignore` | The backend rejects uniqueness on every chain/group/batch job and workflow callback before saving or enqueueing. gRPC conversion returns a precise `InvalidArgument` error for unique workflow steps. Legacy stored definitions are also rejected during effect reconstruction. Effect dispatch never accepts a different returned ID. |
| 3 | Unique replacement ordering | Replacement uses compare-and-replace Lua: the current claim must still equal the caller's observed predecessor; active/pending unsafe predecessors are rejected with the existing ID; available/scheduled/retryable predecessors are type-checked, cancelled, and removed from all indexes before the replacement is created and claimed. Concurrent replacers produce at most one replacement. Public `Cancel` now uses an atomic cancellation script and propagates all store failures. |
| 4 | Expired workflow-effect leases | Stable effect jobs use `AtomicCreateWorkflowEffectJob`, which checks workflow state and current lease ownership and creates only when the job hash is absent. Existing fetched or terminal jobs are reconciled without hash/index rewrites. Stale owners cannot create, append, or complete; only the current owner can complete the effect. |
| 5 | Cron outage deduplication | Cron occurrence IDs are deterministic UUIDv7 values: the 48-bit timestamp comes from the scheduled occurrence and the remaining 74 bits from a stable SHA-256 hash of cron name plus occurrence. Marker expiry or restart therefore reuses the same ID. `AtomicPushIfAbsent` reconciles that ID without resetting an existing job. Marker retention remains bounded at 24 hours and successful cursor persistence deletes the marker. |
| 6 | Lua WRONGTYPE atomicity | The global workflow pending-index type is validated before any mutation in workflow advance, completion, and cancellation/revocation scripts. Related job creation, cancellation, fetch, promotion, replacement, and requeue scripts preflight key types/state before transitions. WRONGTYPE tests prove counters, results, advanced markers, effects, job lists, claims, and indexes are not partially mutated. |

## Tests added or strengthened

- Workflow cancellation before effect claim, while leased, and racing a created-but-unappended effect; all paths leave no live post-cancel effect job.
- Two-drainer expired-lease stress with active/fetched and terminal stable jobs; exactly one creation and append, with no state reset.
- Workflow uniqueness rejection for chain, group, batch, callbacks, and gRPC chain/group conversion.
- Unique replacement for available, scheduled, retryable, and active predecessors; preflight failure and concurrent replacement coverage.
- Atomic public job cancellation failure propagation and cancelled-job fetch/promotion/requeue fencing.
- Cron UUIDv7 determinism, validity, timestamp version bits, name/occurrence uniqueness, and simulated restart after a greater-than-24-hour marker outage.
- Real-Redis pending-index WRONGTYPE tests for workflow advance, completion, and cancellation with no partial mutations.
- Generic and workflow-specific create-if-absent tests proving fetched/terminal jobs are never reset or re-indexed.

## Final validation evidence

| Gate | Result |
|---|---|
| Repository-wide `gofmt -l` | clean |
| `git diff --check` | pass |
| `GOWORK=off go build ./...` | pass |
| `GOWORK=off go vet ./...` | pass |
| `GOWORK=off make build` | pass |
| `GOWORK=off make lint` | pass |
| Uncached full race/coverage: `go test ./... -race -cover -count=1` | pass |
| Coverage | API 13.2%, gRPC 42.7%, Kafka 54.0%, state 67.7% |
| `GOWORK=off make test` | pass |
| Backend race stress (`-count=50`) | pass |
| Redis atomic/concurrency stress (`-count=5`) | pass |
| Redis integration/testcontainers | pass (`redis:7-alpine`) |
| Workflow conformance (`level-3-workflows`) | **14/14 pass**, Level 3 Orchestration **CONFORMANT** |

The final workflow conformance run used isolated `redis:7-alpine` and
`docker.redpanda.com/redpandadata/redpanda:v24.2.7` containers with the freshly
built server. The server and both containers were stopped and removed afterward.

## Known pre-existing conditions

- Conformance levels 0-2 retain the prior baseline's cross-repository job-type validation mismatch for hyphenated types. The shared validation contract lives in `ojs-go-backend-common` and was intentionally not changed here.
- Generic `coreErrorToGRPC` remains string-based outside the workflow conversion errors.
- DAG joins/forks and multiple dependent chains remain unsupported; independent groups and one linear chain are supported.
- The existing `KafkaBackend` breadth and in-memory checkpoint architecture are outside this correctness remediation.

## Remaining blockers

None.
