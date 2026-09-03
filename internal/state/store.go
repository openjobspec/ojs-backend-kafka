package state

import (
	"context"
	"encoding/json"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

const (
	CronOccurrenceAcquired = "acquired"
	CronOccurrenceBusy     = "busy"
	CronOccurrenceFired    = "fired"
)

// Unique claim outcomes returned by ClaimUniqueJob.
const (
	// UniqueClaimClaimed means the new job was created and now owns the claim.
	UniqueClaimClaimed = "claimed"
	// UniqueClaimRejected means a relevant job already owns the claim and the
	// reject policy applies; the new job was not created.
	UniqueClaimRejected = "rejected"
	// UniqueClaimIgnored means a relevant job already owns the claim and the
	// ignore policy applies; the existing job should be returned unchanged.
	UniqueClaimIgnored = "ignored"
)

// UniqueClaimResult reports the outcome of an atomic unique-job claim.
type UniqueClaimResult struct {
	// Outcome is one of UniqueClaimClaimed, UniqueClaimRejected, UniqueClaimIgnored.
	Outcome string
	// ExistingID is the job that already owned the claim (reject/ignore), or the
	// job that was atomically cancelled before a replacement was created.
	ExistingID string
	// ExistingState is the state observed for ExistingID when the operation
	// could not proceed.
	ExistingState string
}

// JobCancelResult reports the outcome of an atomic job cancellation.
type JobCancelResult struct {
	Cancelled     bool
	PreviousState string
}

// CronOccurrenceClaim describes ownership of one scheduled cron occurrence.
type CronOccurrenceClaim struct {
	Status string
	JobID  string
}

// WorkflowAdvanceInput carries the data for one atomic workflow advancement,
// including the preassigned job IDs used to durably record dispatch effects.
type WorkflowAdvanceInput struct {
	WorkflowID  string
	JobID       string
	Step        int
	Result      json.RawMessage
	Failed      bool
	CompletedAt string

	// Preassigned, stable job IDs for the dispatch effects the advancement may
	// record. Only the effect that is actually activated uses its ID; the atomic
	// script stores the chosen ID so a retry reuses the same stable identity.
	NextChainJobID  string
	OnCompleteJobID string
	OnSuccessJobID  string
	OnFailureJobID  string
}

// WorkflowAdvanceResult describes the atomic outcome of advancing a workflow.
type WorkflowAdvanceResult struct {
	Applied       bool
	WorkflowType  string
	State         string
	Completed     int
	Failed        int
	Total         int
	TerminalOwner bool
	EnqueueNext   bool
	NextStep      int
	// HasPendingEffects is true when the workflow has undispatched effects in its
	// durable outbox (including ones recorded by a prior, possibly crashed,
	// advancement) that a drain must still process.
	HasPendingEffects bool
}

// Workflow effect claim statuses returned by ClaimWorkflowEffect.
const (
	WorkflowEffectClaimed = "claimed"
	WorkflowEffectBusy    = "busy"
	WorkflowEffectDone    = "done"
	WorkflowEffectGone    = "gone"
	WorkflowEffectFenced  = "fenced"
)

// WorkflowEffectClaim reports the outcome of leasing a workflow dispatch effect.
type WorkflowEffectClaim struct {
	Status string
	JobID  string
}

// Workflow effect job creation outcomes returned by
// AtomicCreateWorkflowEffectJob.
const (
	WorkflowEffectJobCreated  = "created"
	WorkflowEffectJobExisting = "existing"
	WorkflowEffectJobFenced   = "fenced"
	WorkflowEffectJobNotOwner = "not_owner"
	WorkflowEffectJobGone     = "gone"
)

// WorkflowCancelResult reports the atomic workflow cancellation outcome and
// the stable IDs of every revoked dispatch effect.
type WorkflowCancelResult struct {
	Applied      bool
	State        string
	EffectJobIDs []string
}

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
	// ClaimUniqueJob atomically claims the fingerprint and creates the job in a
	// single Redis transaction, applying the conflict policy against the current
	// claim's relevance so no dangling claim or duplicate can result from a race.
	ClaimUniqueJob(ctx context.Context, fingerprint string, job *core.Job, score float64, scheduled bool, ttlMs int64, conflict string, relevantStates []string) (*UniqueClaimResult, error)
	// ReplaceUniqueJob compares the current fingerprint owner with expectedID,
	// atomically cancels an eligible predecessor, then creates the replacement.
	ReplaceUniqueJob(ctx context.Context, fingerprint string, expectedID string, job *core.Job, score float64, scheduled bool, ttlMs int64, relevantStates []string, cancelledAt string) (*UniqueClaimResult, error)

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
	ClaimCronOccurrence(ctx context.Context, name string, occurrenceMs int64, owner string, jobID string, nowMs int64, leaseMs int64) (*CronOccurrenceClaim, error)
	CompleteCronOccurrence(ctx context.Context, name string, occurrenceMs int64, owner string, jobID string) error
	ReleaseCronOccurrence(ctx context.Context, name string, occurrenceMs int64, owner string, jobID string) error
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
	AtomicCancelWorkflow(ctx context.Context, workflowID string, completedAt string) (*WorkflowCancelResult, error)

	// Workflow dispatch outbox (durable effects for crash-safe follow-up enqueues)
	GetWorkflowEffects(ctx context.Context, workflowID string) (map[string]string, error)
	ClaimWorkflowEffect(ctx context.Context, workflowID string, effectID string, owner string, nowMs int64, leaseMs int64) (*WorkflowEffectClaim, error)
	AtomicCreateWorkflowEffectJob(ctx context.Context, workflowID string, effectID string, owner string, job *core.Job, score float64, scheduled bool) (string, error)
	CompleteWorkflowEffect(ctx context.Context, workflowID string, effectID string, owner string, jobID string, appendJob bool) (bool, error)
	ReleaseWorkflowEffect(ctx context.Context, workflowID string, effectID string, owner string) error
	GetWorkflowsWithPendingEffects(ctx context.Context) ([]string, error)

	// Atomic operations (Lua scripts for crash safety)
	AtomicPush(ctx context.Context, job *core.Job, score float64, scheduled bool) error
	AtomicPushIfAbsent(ctx context.Context, job *core.Job, score float64, scheduled bool) (bool, error)
	AtomicCancelJob(ctx context.Context, jobID string, cancelledAt string) (*JobCancelResult, error)
	AtomicFetch(ctx context.Context, queue string, visDeadline string, startedAt string, workerID string) (string, error)
	AtomicPromote(ctx context.Context, jobID string, queue string, fromState string, enqueuedAt string, score float64) (bool, error)
	AtomicAck(ctx context.Context, jobID string, queue string, completedAt string, result string) error
	AtomicRequeue(ctx context.Context, jobID string, queue string, enqueuedAt string, score float64) error
	AtomicNackDiscard(ctx context.Context, jobID string, queue string, completedAt string, errJSON string, histJSON string, attempt string, addToDead bool, nowMs int64) error
	AtomicNackRetry(ctx context.Context, jobID string, queue string, errJSON string, histJSON string, attempt string, retryDelayMs string, retryAtMs int64) error
	AtomicAdvanceWorkflow(ctx context.Context, input WorkflowAdvanceInput) (*WorkflowAdvanceResult, error)

	// Health
	Ping(ctx context.Context) error

	// Admin
	ListJobs(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error)
	ListWorkers(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error)

	// Close
	Close() error
}
