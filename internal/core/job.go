package core

import (
	"encoding/json"
	"time"
)

const (
	OJSVersion   = "1.0.0-rc.1"
	OJSMediaType = "application/openjobspec+json"
	TimeFormat   = "2006-01-02T15:04:05.000Z"
)

// FormatTime formats a time as ISO 8601 UTC with millisecond precision.
func FormatTime(t time.Time) string {
	return t.UTC().Format(TimeFormat)
}

// NowFormatted returns the current time formatted as ISO 8601 UTC.
func NowFormatted() string {
	return FormatTime(time.Now())
}

// Job represents a complete OJS job envelope.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	State       string          `json:"state"`
	Queue       string          `json:"queue"`
	Args        json.RawMessage `json:"args"`
	Meta        json.RawMessage `json:"meta,omitempty"`
	Priority    *int            `json:"priority,omitempty"`
	Attempt     int             `json:"attempt"`
	MaxAttempts *int            `json:"max_attempts,omitempty"`
	TimeoutMs   *int            `json:"timeout_ms,omitempty"`
	CreatedAt   string          `json:"created_at"`
	EnqueuedAt  string          `json:"enqueued_at,omitempty"`
	StartedAt   string          `json:"started_at,omitempty"`
	CompletedAt string          `json:"completed_at,omitempty"`
	CancelledAt string          `json:"cancelled_at,omitempty"`
	ScheduledAt string          `json:"scheduled_at,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error        json.RawMessage `json:"error,omitempty"`
	Errors       []json.RawMessage `json:"errors,omitempty"`
	Tags         []string        `json:"tags,omitempty"`
	Retry        *RetryPolicy    `json:"retry,omitempty"`
	Unique       *UniquePolicy   `json:"unique,omitempty"`
	ExpiresAt            string          `json:"expires_at,omitempty"`
	RetryDelayMs         *int64          `json:"retry_delay_ms,omitempty"`
	ParentResults        []json.RawMessage `json:"parent_results,omitempty"`
	VisibilityTimeoutMs  *int              `json:"-"`
	WorkflowID           string            `json:"-"`
	WorkflowStep         int               `json:"-"`
	IsExisting           bool              `json:"-"`
	RateLimit            *RateLimitPolicy  `json:"-"`

	// Unknown fields for forward compatibility
	UnknownFields map[string]json.RawMessage `json:"-"`
}

// Known field names for Job
var knownJobFields = map[string]bool{
	"id": true, "type": true, "state": true, "queue": true,
	"args": true, "meta": true, "priority": true, "attempt": true,
	"max_attempts": true, "timeout_ms": true, "created_at": true,
	"enqueued_at": true, "started_at": true, "completed_at": true,
	"cancelled_at": true, "scheduled_at": true, "result": true,
	"error": true, "errors": true, "tags": true, "retry": true, "unique": true,
	"specversion": true, "options": true, "schema": true,
	"expires_at": true, "retry_delay_ms": true,
}

// MarshalJSON implements custom JSON marshaling to include unknown fields.
func (j Job) MarshalJSON() ([]byte, error) {
	m := make(map[string]any)

	m["id"] = j.ID
	m["type"] = j.Type
	m["state"] = j.State
	m["queue"] = j.Queue
	m["attempt"] = j.Attempt

	if j.Args != nil {
		m["args"] = json.RawMessage(j.Args)
	}
	if j.Meta != nil && len(j.Meta) > 0 {
		m["meta"] = json.RawMessage(j.Meta)
	}
	if j.Priority != nil {
		m["priority"] = *j.Priority
	}
	if j.MaxAttempts != nil {
		m["max_attempts"] = *j.MaxAttempts
	}
	if j.TimeoutMs != nil {
		m["timeout_ms"] = *j.TimeoutMs
	}
	if j.CreatedAt != "" {
		m["created_at"] = j.CreatedAt
	}
	if j.EnqueuedAt != "" {
		m["enqueued_at"] = j.EnqueuedAt
	}
	if j.StartedAt != "" {
		m["started_at"] = j.StartedAt
	}
	if j.CompletedAt != "" {
		m["completed_at"] = j.CompletedAt
	}
	if j.CancelledAt != "" {
		m["cancelled_at"] = j.CancelledAt
	}
	if j.ScheduledAt != "" {
		m["scheduled_at"] = j.ScheduledAt
	}
	if j.Result != nil && len(j.Result) > 0 {
		m["result"] = json.RawMessage(j.Result)
	}
	if j.Error != nil && len(j.Error) > 0 {
		m["error"] = json.RawMessage(j.Error)
	}
	if len(j.Tags) > 0 {
		m["tags"] = j.Tags
	}
	if len(j.Errors) > 0 {
		m["errors"] = j.Errors
	}
	if j.ExpiresAt != "" {
		m["expires_at"] = j.ExpiresAt
	}
	if j.RetryDelayMs != nil {
		m["retry_delay_ms"] = *j.RetryDelayMs
	}
	if len(j.ParentResults) > 0 {
		m["parent_results"] = j.ParentResults
	}

	// Include unknown fields
	for k, v := range j.UnknownFields {
		m[k] = json.RawMessage(v)
	}

	return json.Marshal(m)
}

// EnqueueRequest represents the request body for enqueuing a job.
type EnqueueRequest struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Args    json.RawMessage `json:"args"`
	Meta    json.RawMessage `json:"meta,omitempty"`
	Schema  string          `json:"schema,omitempty"`
	Options *EnqueueOptions `json:"options,omitempty"`

	// HasID indicates whether the "id" field was explicitly present in the JSON body.
	HasID bool `json:"-"`

	// Unknown fields preserved for forward compatibility
	UnknownFields map[string]json.RawMessage `json:"-"`
}

// RateLimitPolicy defines rate limiting for a queue.
type RateLimitPolicy struct {
	MaxPerSecond int `json:"max_per_second,omitempty"`
}

// EnqueueOptions are optional settings for job enqueue.
type EnqueueOptions struct {
	Queue               string           `json:"queue,omitempty"`
	Priority            *int             `json:"priority,omitempty"`
	TimeoutMs           *int             `json:"timeout_ms,omitempty"`
	DelayUntil          string           `json:"delay_until,omitempty"`
	ScheduledAt         string           `json:"scheduled_at,omitempty"`
	ExpiresAt           string           `json:"expires_at,omitempty"`
	Retry               *RetryPolicy     `json:"retry,omitempty"`
	RetryPolicy         *RetryPolicy     `json:"retry_policy,omitempty"`
	Unique              *UniquePolicy    `json:"unique,omitempty"`
	Tags                []string         `json:"tags,omitempty"`
	VisibilityTimeoutMs *int             `json:"visibility_timeout_ms,omitempty"`
	VisibilityTimeout   string           `json:"visibility_timeout,omitempty"`
	Metadata            json.RawMessage  `json:"metadata,omitempty"`
	RateLimit           *RateLimitPolicy `json:"rate_limit,omitempty"`
}

// ParseEnqueueRequest parses raw JSON into an EnqueueRequest, preserving unknown fields.
func ParseEnqueueRequest(data []byte) (*EnqueueRequest, error) {
	var req EnqueueRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	knownRequestFields := map[string]bool{
		"id": true, "type": true, "args": true, "meta": true,
		"schema": true, "options": true,
	}

	if _, hasID := raw["id"]; hasID {
		req.HasID = true
	}

	req.UnknownFields = make(map[string]json.RawMessage)
	for k, v := range raw {
		if !knownRequestFields[k] {
			req.UnknownFields[k] = v
		}
	}

	return &req, nil
}

// FetchRequest represents the request body for fetching jobs.
type FetchRequest struct {
	Queues             []string `json:"queues"`
	Count              int      `json:"count,omitempty"`
	WorkerID           string   `json:"worker_id,omitempty"`
	VisibilityTimeoutMs *int    `json:"visibility_timeout_ms,omitempty"`
}

// AckRequest represents the request body for acknowledging a job.
type AckRequest struct {
	JobID  string          `json:"job_id"`
	Result json.RawMessage `json:"result,omitempty"`
}

// NackRequest represents the request body for failing a job.
type NackRequest struct {
	JobID   string    `json:"job_id"`
	Error   *JobError `json:"error"`
	Requeue bool      `json:"requeue,omitempty"`
}

// JobError represents a structured error from a job handler.
type JobError struct {
	Code      string         `json:"code,omitempty"`
	Type      string         `json:"type,omitempty"`
	Message   string         `json:"message"`
	Retryable *bool          `json:"retryable,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

// HeartbeatRequest represents the request body for worker heartbeat.
type HeartbeatRequest struct {
	WorkerID            string   `json:"worker_id"`
	JobID               string   `json:"job_id,omitempty"`
	ActiveJobs          []string `json:"active_jobs,omitempty"`
	VisibilityTimeoutMs *int     `json:"visibility_timeout_ms,omitempty"`
}

// BatchEnqueueRequest represents the request body for batch enqueue.
type BatchEnqueueRequest struct {
	Jobs []json.RawMessage `json:"jobs"`
}
