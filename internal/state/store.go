package state

import (
	"context"
	"encoding/json"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// Store defines the interface for job state persistence.
// The state store tracks per-job lifecycle, which Kafka alone cannot do.
// Kafka handles delivery and ordering; the state store handles lifecycle tracking.
type Store interface {
	// Job state operations
	SaveJob(ctx context.Context, job *core.Job) error
	GetJob(ctx context.Context, jobID string) (*core.Job, error)
	UpdateJob(ctx context.Context, jobID string, updates map[string]any) error
	DeleteJobFields(ctx context.Context, jobID string, fields ...string) error

	// Queue operations
	AddToAvailable(ctx context.Context, queue string, jobID string, score float64) error
	PopFromAvailable(ctx context.Context, queue string) (string, error)
	RemoveFromAvailable(ctx context.Context, queue string, jobID string) error
	AddToActive(ctx context.Context, queue string, jobID string) error
	RemoveFromActive(ctx context.Context, queue string, jobID string) error
	GetActiveJobs(ctx context.Context, queue string) ([]string, error)
	RegisterQueue(ctx context.Context, queue string) error
	GetAllQueues(ctx context.Context) ([]string, error)
	IsQueuePaused(ctx context.Context, queue string) (bool, error)
	PauseQueue(ctx context.Context, queue string) error
	ResumeQueue(ctx context.Context, queue string) error
	GetAvailableCount(ctx context.Context, queue string) (int64, error)
	GetActiveCount(ctx context.Context, queue string) (int64, error)
	IncrCompletedCount(ctx context.Context, queue string) error
	GetCompletedCount(ctx context.Context, queue string) (int64, error)

	// Rate limiting
	SetRateLimit(ctx context.Context, queue string, maxPerSecond int) error
	CheckRateLimit(ctx context.Context, queue string) (bool, error)
	RecordFetch(ctx context.Context, queue string) error

	// Scheduled jobs
	AddToScheduled(ctx context.Context, jobID string, scheduledAtMs int64) error
	RemoveFromScheduled(ctx context.Context, jobID string) error
	GetDueScheduled(ctx context.Context, nowMs int64) ([]string, error)

	// Retry jobs
	AddToRetry(ctx context.Context, jobID string, retryAtMs int64) error
	RemoveFromRetry(ctx context.Context, jobID string) error
	GetDueRetries(ctx context.Context, nowMs int64) ([]string, error)

	// Dead letter queue
	AddToDead(ctx context.Context, jobID string, nowMs int64) error
	RemoveFromDead(ctx context.Context, jobID string) error
	GetDeadJobs(ctx context.Context, offset, limit int) ([]string, int64, error)
	IsInDead(ctx context.Context, jobID string) (bool, error)

	// Visibility timeout
	SetVisibility(ctx context.Context, jobID string, deadline string) error
	GetVisibility(ctx context.Context, jobID string) (string, error)
	DeleteVisibility(ctx context.Context, jobID string) error

	// Unique jobs
	GetUniqueJobID(ctx context.Context, fingerprint string) (string, error)
	SetUniqueJobID(ctx context.Context, fingerprint string, jobID string, ttlMs int64) error

	// Workers
	RegisterWorker(ctx context.Context, workerID string, data map[string]any) error
	GetWorkerData(ctx context.Context, workerID string, field string) (string, error)
	SetWorkerData(ctx context.Context, workerID string, field string, value string) error

	// Cron
	SaveCron(ctx context.Context, name string, data []byte) error
	GetCron(ctx context.Context, name string) ([]byte, error)
	DeleteCron(ctx context.Context, name string) error
	GetAllCronNames(ctx context.Context) ([]string, error)
	AcquireCronLock(ctx context.Context, key string, ttlMs int64) (bool, error)
	SetCronInstance(ctx context.Context, name string, jobID string) error
	GetCronInstance(ctx context.Context, name string) (string, error)

	// Workflows
	SaveWorkflow(ctx context.Context, id string, data map[string]any) error
	GetWorkflow(ctx context.Context, id string) (map[string]string, error)
	UpdateWorkflow(ctx context.Context, id string, updates map[string]any) error
	AppendWorkflowJob(ctx context.Context, workflowID string, jobID string) error
	GetWorkflowJobs(ctx context.Context, workflowID string) ([]string, error)
	SetWorkflowResult(ctx context.Context, workflowID string, step int, result json.RawMessage) error
	GetWorkflowResults(ctx context.Context, workflowID string) (map[string]string, error)

	// Atomic operations (Lua scripts for crash safety)
	AtomicPush(ctx context.Context, job *core.Job, score float64, scheduled bool) error
	AtomicFetch(ctx context.Context, queue string, visDeadline string) (string, error)
	AtomicAck(ctx context.Context, jobID string, queue string, completedAt string, result string) error
	AtomicNackDiscard(ctx context.Context, jobID string, queue string, completedAt string, errJSON string, histJSON string, attempt string, addToDead bool, nowMs int64) error
	AtomicNackRetry(ctx context.Context, jobID string, queue string, errJSON string, histJSON string, attempt string, retryDelayMs string, retryAtMs int64) error

	// Health
	Ping(ctx context.Context) error

	// Admin
	ListJobs(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error)
	ListWorkers(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error)

	// Close
	Close() error
}
