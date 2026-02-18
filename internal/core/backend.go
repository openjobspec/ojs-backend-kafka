package core

import (
	common "github.com/openjobspec/ojs-go-backend-common/core"
)

// Type aliases for shared types — allows internal packages to continue using core.X
type Job = common.Job
type OJSError = common.OJSError
type RetryPolicy = common.RetryPolicy
type UniquePolicy = common.UniquePolicy
type EnqueueRequest = common.EnqueueRequest
type EnqueueOptions = common.EnqueueOptions
type FetchRequest = common.FetchRequest
type AckRequest = common.AckRequest
type NackRequest = common.NackRequest
type JobError = common.JobError
type HeartbeatRequest = common.HeartbeatRequest
type BatchEnqueueRequest = common.BatchEnqueueRequest
type RateLimitPolicy = common.RateLimitPolicy

// Type aliases for backend interfaces.
type JobManager = common.JobManager
type WorkerManager = common.WorkerManager
type QueueManager = common.QueueManager
type DeadLetterManager = common.DeadLetterManager
type CronManager = common.CronManager
type WorkflowManager = common.WorkflowManager
type AdminManager = common.AdminManager

// Type aliases for backend response/model types.
type AckResponse = common.AckResponse
type NackResponse = common.NackResponse
type QueueInfo = common.QueueInfo
type QueueStats = common.QueueStats
type Stats = common.Stats
type JobListFilters = common.JobListFilters
type WorkerInfo = common.WorkerInfo
type WorkerSummary = common.WorkerSummary
type HealthResponse = common.HealthResponse
type BackendHealth = common.BackendHealth
type HeartbeatResponse = common.HeartbeatResponse
type CronJobTemplate = common.CronJobTemplate
type CronJob = common.CronJob
type WorkflowRequest = common.WorkflowRequest
type WorkflowJobRequest = common.WorkflowJobRequest
type Workflow = common.Workflow
type WorkflowCallbacks = common.WorkflowCallbacks
type WorkflowCallback = common.WorkflowCallback

// Backend is a type alias for the common Backend interface.
type Backend = common.Backend

// Constant aliases for shared constants.
const (
	StateAvailable = common.StateAvailable
	StateScheduled = common.StateScheduled
	StatePending   = common.StatePending
	StateActive    = common.StateActive
	StateRetryable = common.StateRetryable
	StateCompleted = common.StateCompleted
	StateCancelled = common.StateCancelled
	StateDiscarded = common.StateDiscarded

	ErrCodeInvalidRequest  = common.ErrCodeInvalidRequest
	ErrCodeValidationError = common.ErrCodeValidationError
	ErrCodeNotFound        = common.ErrCodeNotFound
	ErrCodeConflict        = common.ErrCodeConflict
	ErrCodeDuplicate       = common.ErrCodeDuplicate
	ErrCodeInternalError   = common.ErrCodeInternalError
	ErrCodeUnsupported     = common.ErrCodeUnsupported
	ErrCodeQueuePaused     = common.ErrCodeQueuePaused

	OJSVersion                 = common.OJSVersion
	OJSMediaType               = common.OJSMediaType
	TimeFormat                 = common.TimeFormat
	DefaultVisibilityTimeoutMs = common.DefaultVisibilityTimeoutMs
)

// Function aliases for shared functions.
var (
	NewUUIDv7              = common.NewUUIDv7
	IsValidUUIDv7          = common.IsValidUUIDv7
	IsValidUUID            = common.IsValidUUID
	FormatTime             = common.FormatTime
	NowFormatted           = common.NowFormatted
	ParseEnqueueRequest    = common.ParseEnqueueRequest
	ValidateEnqueueRequest = common.ValidateEnqueueRequest
	CalculateBackoff       = common.CalculateBackoff
	DefaultRetryPolicy     = common.DefaultRetryPolicy
	ParseISO8601Duration   = common.ParseISO8601Duration
	FormatISO8601Duration  = common.FormatISO8601Duration
	IsValidTransition      = common.IsValidTransition
	IsTerminalState        = common.IsTerminalState
	IsCancellableState     = common.IsCancellableState
	NewInvalidRequestError = common.NewInvalidRequestError
	NewNotFoundError       = common.NewNotFoundError
	NewConflictError       = common.NewConflictError
	NewValidationError     = common.NewValidationError
	NewInternalError       = common.NewInternalError
)
