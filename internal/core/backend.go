package core

import (
	"context"
	"encoding/json"
)

// JobManager handles core job lifecycle operations.
type JobManager interface {
	Push(ctx context.Context, job *Job) (*Job, error)
	PushBatch(ctx context.Context, jobs []*Job) ([]*Job, error)
	Info(ctx context.Context, jobID string) (*Job, error)
	Cancel(ctx context.Context, jobID string) (*Job, error)
}

// WorkerManager handles worker-side operations (fetch, ack, nack, heartbeat).
type WorkerManager interface {
	Fetch(ctx context.Context, queues []string, count int, workerID string, visibilityTimeoutMs int) ([]*Job, error)
	Ack(ctx context.Context, jobID string, result []byte) (*AckResponse, error)
	Nack(ctx context.Context, jobID string, jobErr *JobError, requeue bool) (*NackResponse, error)
	Heartbeat(ctx context.Context, workerID string, activeJobs []string, visibilityTimeoutMs int) (*HeartbeatResponse, error)
	SetWorkerState(ctx context.Context, workerID string, state string) error
}

// QueueManager handles queue-level operations.
type QueueManager interface {
	ListQueues(ctx context.Context) ([]QueueInfo, error)
	QueueStats(ctx context.Context, name string) (*QueueStats, error)
	PauseQueue(ctx context.Context, name string) error
	ResumeQueue(ctx context.Context, name string) error
}

// DeadLetterManager handles dead letter queue operations.
type DeadLetterManager interface {
	ListDeadLetter(ctx context.Context, limit, offset int) ([]*Job, int, error)
	RetryDeadLetter(ctx context.Context, jobID string) (*Job, error)
	DeleteDeadLetter(ctx context.Context, jobID string) error
}

// CronManager handles cron job operations.
type CronManager interface {
	RegisterCron(ctx context.Context, cron *CronJob) (*CronJob, error)
	ListCron(ctx context.Context) ([]*CronJob, error)
	DeleteCron(ctx context.Context, name string) (*CronJob, error)
}

// WorkflowManager handles workflow operations.
type WorkflowManager interface {
	CreateWorkflow(ctx context.Context, req *WorkflowRequest) (*Workflow, error)
	GetWorkflow(ctx context.Context, id string) (*Workflow, error)
	CancelWorkflow(ctx context.Context, id string) (*Workflow, error)
	AdvanceWorkflow(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error
}

// Backend defines the full interface for OJS backend implementations,
// composing all role-specific interfaces.
type Backend interface {
	JobManager
	WorkerManager
	QueueManager
	DeadLetterManager
	CronManager
	WorkflowManager

	// Health returns the health status.
	Health(ctx context.Context) (*HealthResponse, error)

	// Close closes the backend connection.
	Close() error
}

// AckResponse represents the response after acknowledging a job.
type AckResponse struct {
	Acknowledged bool   `json:"acknowledged"`
	JobID        string `json:"job_id"`
	State        string `json:"state"`
	CompletedAt  string `json:"completed_at"`
	Job          *Job   `json:"job,omitempty"`
}

// NackResponse represents the response after failing a job.
type NackResponse struct {
	JobID         string `json:"job_id"`
	State         string `json:"state"`
	Attempt       int    `json:"attempt"`
	MaxAttempts   int    `json:"max_attempts"`
	NextAttemptAt string `json:"next_attempt_at,omitempty"`
	DiscardedAt   string `json:"discarded_at,omitempty"`
	Job           *Job   `json:"job,omitempty"`
}

// QueueInfo represents basic queue metadata.
type QueueInfo struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at,omitempty"`
}

// QueueStats represents detailed queue statistics.
type QueueStats struct {
	Queue     string `json:"queue"`
	Status    string `json:"status"`
	Stats     Stats  `json:"stats"`
}

// Stats holds queue statistics counts.
type Stats struct {
	Available int `json:"available"`
	Active    int `json:"active"`
	Completed int `json:"completed"`
	Scheduled int `json:"scheduled"`
	Retryable int `json:"retryable"`
	Dead      int `json:"dead"`
}

// HealthResponse represents the health check response.
type HealthResponse struct {
	Status        string        `json:"status"`
	Version       string        `json:"version"`
	UptimeSeconds int64         `json:"uptime_seconds"`
	Backend       BackendHealth `json:"backend"`
}

// BackendHealth represents backend-specific health info.
type BackendHealth struct {
	Type      string `json:"type"`
	Status    string `json:"status"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
	Error     string `json:"error,omitempty"`
}

// HeartbeatResponse represents the heartbeat response.
type HeartbeatResponse struct {
	State        string   `json:"state"`
	Directive    string   `json:"directive"`
	JobsExtended []string `json:"jobs_extended,omitempty"`
	ServerTime   string   `json:"server_time"`
}

// CronJobTemplate represents the job template for a cron job.
type CronJobTemplate struct {
	Type    string          `json:"type"`
	Args    json.RawMessage `json:"args,omitempty"`
	Options *EnqueueOptions `json:"options,omitempty"`
}

// CronJob represents a registered cron job.
type CronJob struct {
	Name          string          `json:"name"`
	Expression    string          `json:"expression"`
	Timezone      string          `json:"timezone,omitempty"`
	OverlapPolicy string          `json:"overlap_policy,omitempty"`
	Enabled       bool            `json:"enabled"`
	JobTemplate   *CronJobTemplate `json:"job_template,omitempty"`
	CreatedAt     string          `json:"created_at,omitempty"`
	NextRunAt     string          `json:"next_run_at,omitempty"`
	LastRunAt     string          `json:"last_run_at,omitempty"`

	// Legacy flat fields (kept for internal use)
	JobType    string          `json:"-"`
	Args       json.RawMessage `json:"-"`
	Queue      string          `json:"-"`
	Meta       json.RawMessage `json:"-"`
	Schedule   string          `json:"-"`
}

// WorkflowRequest represents the request body for creating a workflow.
type WorkflowRequest struct {
	Type      string               `json:"type"` // chain, group, batch
	Name      string               `json:"name,omitempty"`
	Steps     []WorkflowJobRequest `json:"steps,omitempty"` // for chain
	Jobs      []WorkflowJobRequest `json:"jobs,omitempty"`  // for group/batch
	Callbacks *WorkflowCallbacks   `json:"callbacks,omitempty"`
}

// WorkflowJobRequest represents a single job within a workflow request.
type WorkflowJobRequest struct {
	Name    string          `json:"name"`
	Type    string          `json:"type"`
	Args    json.RawMessage `json:"args"`
	Options *EnqueueOptions `json:"options,omitempty"`
}

// Workflow represents the stored/returned workflow state.
type Workflow struct {
	ID             string             `json:"id"`
	Name           string             `json:"name,omitempty"`
	Type           string             `json:"type"` // chain, group, batch
	State          string             `json:"state"`
	StepsTotal     *int               `json:"steps_total,omitempty"`
	StepsCompleted *int               `json:"steps_completed,omitempty"`
	JobsTotal      *int               `json:"jobs_total,omitempty"`
	JobsCompleted  *int               `json:"jobs_completed,omitempty"`
	Callbacks      *WorkflowCallbacks `json:"callbacks,omitempty"`
	CreatedAt      string             `json:"created_at"`
	CompletedAt    string             `json:"completed_at,omitempty"`
}

// WorkflowCallbacks defines callback jobs for workflow events.
type WorkflowCallbacks struct {
	OnSuccess  *WorkflowCallback `json:"on_success,omitempty"`
	OnFailure  *WorkflowCallback `json:"on_failure,omitempty"`
	OnComplete *WorkflowCallback `json:"on_complete,omitempty"`
}

// WorkflowCallback defines a callback job.
type WorkflowCallback struct {
	Type    string          `json:"type"`
	Args    json.RawMessage `json:"args,omitempty"`
	Options *EnqueueOptions `json:"options,omitempty"`
}
