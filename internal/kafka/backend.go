package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/robfig/cron/v3"

	ojsotel "github.com/openjobspec/ojs-go-backend-common/otel"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

// KafkaBackend implements core.Backend using Kafka for transport and event
// streaming, with an external state store (Redis) for per-job lifecycle tracking.
//
// Architecture:
//   - State store: handles all per-job state, queue ordering, visibility timeouts
//   - Kafka producer: publishes job messages and lifecycle events to Kafka topics
//   - Kafka consumer: optional, for distributed external consumption and replay
//
// The HTTP API (push/fetch/ack/nack) operates against the state store for
// correctness and low latency. Kafka provides durability, event streaming,
// and horizontal scalability.
type KafkaBackend struct {
	store     state.Store
	producer  *Producer
	startTime time.Time
}

// New creates a new KafkaBackend.
func New(store state.Store, producer *Producer) *KafkaBackend {
	return &KafkaBackend{
		store:     store,
		producer:  producer,
		startTime: time.Now(),
	}
}

func (b *KafkaBackend) Close() error {
	return b.store.Close()
}

// Push enqueues a single job.
func (b *KafkaBackend) Push(ctx context.Context, job *core.Job) (*core.Job, error) {
	ctx, span := ojsotel.StartJobSpan(ctx, "push", job.ID, job.Type, job.Queue)
	defer span.End()

	now := time.Now()

	if job.ID == "" {
		job.ID = core.NewUUIDv7()
	}

	job.CreatedAt = core.FormatTime(now)
	job.Attempt = 0

	// Handle unique jobs
	if job.Unique != nil {
		fingerprint := computeFingerprint(job)
		ttlMs := int64(3600000) // 1 hour default
		if job.Unique.Period != "" {
			if d, err := core.ParseISO8601Duration(job.Unique.Period); err == nil {
				ttlMs = d.Milliseconds()
			}
		}

		conflict := job.Unique.OnConflict
		if conflict == "" {
			conflict = "reject"
		}

		existingID, err := b.store.GetUniqueJobID(ctx, fingerprint)
		if err == nil && existingID != "" {
			existingJob, stateErr := b.store.GetJob(ctx, existingID)
			if stateErr == nil {
				isRelevant := false
				if len(job.Unique.States) > 0 {
					for _, s := range job.Unique.States {
						if s == existingJob.State {
							isRelevant = true
							break
						}
					}
				} else {
					isRelevant = !core.IsTerminalState(existingJob.State)
				}

				if isRelevant {
					switch conflict {
					case "reject":
						return nil, &core.OJSError{
							Code:    core.ErrCodeDuplicate,
							Message: "A job with the same unique key already exists.",
							Details: map[string]any{
								"existing_job_id": existingID,
								"unique_key":      fingerprint,
							},
						}
					case "ignore":
						existingJob.IsExisting = true
						return existingJob, nil
					case "replace":
						b.Cancel(ctx, existingID)
					}
				}
			}
		}

		b.store.SetUniqueJobID(ctx, fingerprint, job.ID, ttlMs)
	}

	// Determine initial state
	if job.ScheduledAt != "" {
		scheduledTime, err := time.Parse(time.RFC3339, job.ScheduledAt)
		if err == nil && scheduledTime.After(now) {
			job.State = core.StateScheduled
			job.EnqueuedAt = core.FormatTime(now)

			if err := b.store.AtomicPush(ctx, job, float64(scheduledTime.UnixMilli()), true); err != nil {
				return nil, fmt.Errorf("atomic push scheduled job: %w", err)
			}

			// Publish to Kafka asynchronously
			b.producer.ProduceJobAsync(ctx, job)
			b.producer.EmitJobEnqueued(ctx, job)

			return job, nil
		}
	}

	job.State = core.StateAvailable
	job.EnqueuedAt = core.FormatTime(now)

	score := computeScore(job.Priority, now)

	if err := b.store.AtomicPush(ctx, job, score, false); err != nil {
		return nil, fmt.Errorf("atomic push job: %w", err)
	}

	if job.RateLimit != nil && job.RateLimit.MaxPerSecond > 0 {
		b.store.SetRateLimit(ctx, job.Queue, job.RateLimit.MaxPerSecond)
	}

	// Publish to Kafka asynchronously
	b.producer.ProduceJobAsync(ctx, job)
	b.producer.EmitJobEnqueued(ctx, job)

	return job, nil
}

// Fetch claims jobs from the specified queues.
func (b *KafkaBackend) Fetch(ctx context.Context, queues []string, count int, workerID string, visibilityTimeoutMs int) ([]*core.Job, error) {
	ctx, span := ojsotel.StartStorageSpan(ctx, "fetch", "kafka")
	defer span.End()

	now := time.Now()
	var jobs []*core.Job

	for _, queue := range queues {
		if len(jobs) >= count {
			break
		}

		paused, _ := b.store.IsQueuePaused(ctx, queue)
		if paused {
			continue
		}

		allowed, _ := b.store.CheckRateLimit(ctx, queue)
		if !allowed {
			continue
		}

		remaining := count - len(jobs)
		for i := 0; i < remaining; i++ {
			// Compute visibility deadline
			effectiveVisTimeout := visibilityTimeoutMs
			if effectiveVisTimeout <= 0 {
				effectiveVisTimeout = core.DefaultVisibilityTimeoutMs
			}
			deadline := core.FormatTime(now.Add(time.Duration(effectiveVisTimeout) * time.Millisecond))

			// Atomically: pop from available, add to active, set visibility
			jobID, err := b.store.AtomicFetch(ctx, queue, deadline)
			if err != nil || jobID == "" {
				break
			}

			job, err := b.store.GetJob(ctx, jobID)
			if err != nil {
				continue
			}

			// Check expiration
			if job.ExpiresAt != "" {
				expTime, err := time.Parse(time.RFC3339, job.ExpiresAt)
				if err == nil && now.After(expTime) {
					b.store.UpdateJob(ctx, jobID, map[string]any{"state": core.StateDiscarded})
					b.store.RemoveFromActive(ctx, queue, jobID)
					b.store.DeleteVisibility(ctx, jobID)
					continue
				}
			}

			// Adjust visibility if job has custom timeout
			if job.VisibilityTimeoutMs != nil && *job.VisibilityTimeoutMs > 0 && visibilityTimeoutMs <= 0 {
				customDeadline := core.FormatTime(now.Add(time.Duration(*job.VisibilityTimeoutMs) * time.Millisecond))
				b.store.SetVisibility(ctx, jobID, customDeadline)
			}

			// Update job state fields
			b.store.UpdateJob(ctx, jobID, map[string]any{
				"state":      core.StateActive,
				"started_at": core.FormatTime(now),
				"worker_id":  workerID,
			})

			b.store.RecordFetch(ctx, queue)

			// Re-fetch the updated job
			updatedJob, err := b.store.GetJob(ctx, jobID)
			if err != nil {
				continue
			}
			jobs = append(jobs, updatedJob)

			b.producer.EmitJobStarted(ctx, updatedJob)
		}
	}

	return jobs, nil
}

// Ack acknowledges a job as completed.
func (b *KafkaBackend) Ack(ctx context.Context, jobID string, result []byte) (*core.AckResponse, error) {
	ctx, span := ojsotel.StartJobSpan(ctx, "ack", jobID, "", "")
	defer span.End()

	job, err := b.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, core.NewNotFoundError("Job", jobID)
	}

	if job.State != core.StateActive {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot acknowledge job not in 'active' state. Current state: '%s'.", job.State),
			map[string]any{
				"job_id":         jobID,
				"current_state":  job.State,
				"expected_state": "active",
			},
		)
	}

	now := core.NowFormatted()
	resultStr := ""
	if result != nil && len(result) > 0 {
		resultStr = string(result)
	}
	if err := b.store.AtomicAck(ctx, jobID, job.Queue, now, resultStr); err != nil {
		return nil, core.NewInternalError(fmt.Sprintf("atomic ack failed: %v", err))
	}

	// Advance workflow
	b.advanceWorkflow(ctx, jobID, core.StateCompleted, result)

	// Emit Kafka events
	b.producer.EmitJobCompleted(ctx, jobID, job.Queue, job.Type, json.RawMessage(result))

	updatedJob, err := b.store.GetJob(ctx, jobID)
	if err != nil {
		slog.Error("failed to fetch job after ack", "job_id", jobID, "error", err)
	}
	return &core.AckResponse{
		Acknowledged: true,
		JobID:        jobID,
		State:        core.StateCompleted,
		CompletedAt:  now,
		Job:          updatedJob,
	}, nil
}

// Nack reports a job failure.
func (b *KafkaBackend) Nack(ctx context.Context, jobID string, jobErr *core.JobError, requeue bool) (*core.NackResponse, error) {
	ctx, span := ojsotel.StartJobSpan(ctx, "nack", jobID, "", "")
	defer span.End()

	job, err := b.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, core.NewNotFoundError("Job", jobID)
	}

	if job.State != core.StateActive {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot fail job not in 'active' state. Current state: '%s'.", job.State),
			map[string]any{
				"job_id":         jobID,
				"current_state":  job.State,
				"expected_state": "active",
			},
		)
	}

	now := time.Now()
	maxAttempts := 3
	if job.MaxAttempts != nil {
		maxAttempts = *job.MaxAttempts
	}

	// Handle requeue
	if requeue {
		score := computeScore(job.Priority, now)
		b.store.UpdateJob(ctx, jobID, map[string]any{
			"state":       core.StateAvailable,
			"started_at":  "",
			"worker_id":   "",
			"enqueued_at": core.FormatTime(now),
		})
		b.store.RemoveFromActive(ctx, job.Queue, jobID)
		b.store.DeleteVisibility(ctx, jobID)
		b.store.AddToAvailable(ctx, job.Queue, jobID, score)

		updatedJob, getErr := b.store.GetJob(ctx, jobID)
		if getErr != nil {
			slog.Error("failed to fetch job after nack requeue", "job_id", jobID, "error", getErr)
		}
		return &core.NackResponse{
			JobID:       jobID,
			State:       core.StateAvailable,
			Attempt:     job.Attempt,
			MaxAttempts: maxAttempts,
			Job:         updatedJob,
		}, nil
	}

	newAttempt := job.Attempt + 1

	// Build error JSON
	var errJSON []byte
	if jobErr != nil {
		errObj := map[string]any{
			"message": jobErr.Message,
			"attempt": job.Attempt,
		}
		if jobErr.Code != "" {
			errObj["type"] = jobErr.Code
		}
		if jobErr.Type != "" {
			errObj["type"] = jobErr.Type
		}
		if jobErr.Retryable != nil {
			errObj["retryable"] = *jobErr.Retryable
		}
		if jobErr.Details != nil {
			errObj["details"] = jobErr.Details
		}
		var marshalErr error
		errJSON, marshalErr = json.Marshal(errObj)
		if marshalErr != nil {
			slog.Error("failed to marshal job error", "job_id", jobID, "error", marshalErr)
			errJSON = []byte(`{"message":"marshal error"}`)
		}
	}

	// Update error history
	var errorHistory []json.RawMessage
	if len(job.Errors) > 0 {
		errorHistory = job.Errors
	}
	if errJSON != nil {
		errorHistory = append(errorHistory, json.RawMessage(errJSON))
	}
	histJSON, marshalErr := json.Marshal(errorHistory)
	if marshalErr != nil {
		slog.Error("failed to marshal error history", "job_id", jobID, "error", marshalErr)
		histJSON = []byte("[]")
	}

	// Check non-retryable
	isNonRetryable := false
	if jobErr != nil && jobErr.Retryable != nil && !*jobErr.Retryable {
		isNonRetryable = true
	}

	if !isNonRetryable && jobErr != nil && job.Retry != nil {
		for _, pattern := range job.Retry.NonRetryableErrors {
			errType := jobErr.Code
			if jobErr.Type != "" {
				errType = jobErr.Type
			}
			if matchesPattern(errType, pattern) || matchesPattern(jobErr.Message, pattern) {
				isNonRetryable = true
				break
			}
		}
	}

	onExhaustion := "discard"
	if job.Retry != nil && job.Retry.OnExhaustion != "" {
		onExhaustion = job.Retry.OnExhaustion
	}

	// Determine next state
	if isNonRetryable || newAttempt >= maxAttempts {
		discardedAt := core.FormatTime(now)
		errStr := ""
		if errJSON != nil {
			errStr = string(errJSON)
		}
		addToDead := onExhaustion == "dead_letter"
		if err := b.store.AtomicNackDiscard(ctx, jobID, job.Queue, discardedAt, errStr, string(histJSON), strconv.Itoa(newAttempt), addToDead, now.UnixMilli()); err != nil {
			return nil, core.NewInternalError(fmt.Sprintf("atomic nack discard failed: %v", err))
		}

		if addToDead {
			b.producer.ProduceToDeadLetter(ctx, job)
		}

		b.advanceWorkflow(ctx, jobID, core.StateDiscarded, nil)
		b.producer.EmitJobDiscarded(ctx, jobID, job.Queue, job.Type)

		updatedJob, getErr := b.store.GetJob(ctx, jobID)
		if getErr != nil {
			slog.Error("failed to fetch job after discard", "job_id", jobID, "error", getErr)
		}
		return &core.NackResponse{
			JobID:       jobID,
			State:       core.StateDiscarded,
			Attempt:     newAttempt,
			MaxAttempts: maxAttempts,
			DiscardedAt: discardedAt,
			Job:         updatedJob,
		}, nil
	}

	// Retry
	backoff := core.CalculateBackoff(job.Retry, newAttempt)
	backoffMs := backoff.Milliseconds()
	nextAttemptAt := now.Add(backoff)

	errStr := ""
	if errJSON != nil {
		errStr = string(errJSON)
	}
	if err := b.store.AtomicNackRetry(ctx, jobID, job.Queue, errStr, string(histJSON), strconv.Itoa(newAttempt), strconv.FormatInt(backoffMs, 10), nextAttemptAt.UnixMilli()); err != nil {
		return nil, core.NewInternalError(fmt.Sprintf("atomic nack retry failed: %v", err))
	}

	b.producer.EmitJobFailed(ctx, jobID, job.Queue, job.Type, newAttempt, json.RawMessage(errJSON))

	updatedJob, getErr := b.store.GetJob(ctx, jobID)
	if getErr != nil {
		slog.Error("failed to fetch job after retry", "job_id", jobID, "error", getErr)
	}
	return &core.NackResponse{
		JobID:         jobID,
		State:         core.StateRetryable,
		Attempt:       newAttempt,
		MaxAttempts:   maxAttempts,
		NextAttemptAt: core.FormatTime(nextAttemptAt),
		Job:           updatedJob,
	}, nil
}

// Info retrieves job details.
func (b *KafkaBackend) Info(ctx context.Context, jobID string) (*core.Job, error) {
	return b.store.GetJob(ctx, jobID)
}

// Cancel cancels a job.
func (b *KafkaBackend) Cancel(ctx context.Context, jobID string) (*core.Job, error) {
	job, err := b.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, core.NewNotFoundError("Job", jobID)
	}

	if core.IsTerminalState(job.State) {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot cancel job in terminal state '%s'.", job.State),
			map[string]any{
				"job_id":        jobID,
				"current_state": job.State,
			},
		)
	}

	now := core.NowFormatted()
	b.store.UpdateJob(ctx, jobID, map[string]any{
		"state":        core.StateCancelled,
		"cancelled_at": now,
	})
	b.store.RemoveFromAvailable(ctx, job.Queue, jobID)
	b.store.RemoveFromActive(ctx, job.Queue, jobID)
	b.store.RemoveFromScheduled(ctx, jobID)
	b.store.RemoveFromRetry(ctx, jobID)
	b.store.DeleteVisibility(ctx, jobID)

	b.producer.EmitJobCancelled(ctx, jobID, job.Queue, job.Type)

	job.State = core.StateCancelled
	job.CancelledAt = now
	return job, nil
}

// ListQueues returns all known queues.
func (b *KafkaBackend) ListQueues(ctx context.Context) ([]core.QueueInfo, error) {
	names, err := b.store.GetAllQueues(ctx)
	if err != nil {
		return nil, err
	}

	sort.Strings(names)
	var queues []core.QueueInfo
	for _, name := range names {
		status := "active"
		if paused, _ := b.store.IsQueuePaused(ctx, name); paused {
			status = "paused"
		}
		queues = append(queues, core.QueueInfo{
			Name:   name,
			Status: status,
		})
	}
	return queues, nil
}

// Health returns the health status.
func (b *KafkaBackend) Health(ctx context.Context) (*core.HealthResponse, error) {
	start := time.Now()
	err := b.store.Ping(ctx)
	latency := time.Since(start).Milliseconds()

	resp := &core.HealthResponse{
		Version:       core.OJSVersion,
		UptimeSeconds: int64(time.Since(b.startTime).Seconds()),
	}

	if err != nil {
		resp.Status = "degraded"
		resp.Backend = core.BackendHealth{
			Type:   "kafka",
			Status: "disconnected",
			Error:  err.Error(),
		}
		return resp, err
	}

	resp.Status = "ok"
	resp.Backend = core.BackendHealth{
		Type:      "kafka",
		Status:    "connected",
		LatencyMs: latency,
	}
	return resp, nil
}

// Heartbeat extends visibility timeout and reports worker state.
func (b *KafkaBackend) Heartbeat(ctx context.Context, workerID string, activeJobs []string, visibilityTimeoutMs int) (*core.HeartbeatResponse, error) {
	now := time.Now()
	extended := make([]string, 0)

	b.store.RegisterWorker(ctx, workerID, map[string]any{
		"last_heartbeat": core.FormatTime(now),
		"active_jobs":    len(activeJobs),
	})

	for _, jobID := range activeJobs {
		job, err := b.store.GetJob(ctx, jobID)
		if err != nil || job.State != core.StateActive {
			continue
		}
		timeout := time.Duration(visibilityTimeoutMs) * time.Millisecond
		b.store.SetVisibility(ctx, jobID, core.FormatTime(now.Add(timeout)))
		extended = append(extended, jobID)
	}

	directive := "continue"
	storedDirective, err := b.store.GetWorkerData(ctx, workerID, "directive")
	if err == nil && storedDirective != "" {
		directive = storedDirective
	}

	if directive == "continue" {
		for _, jobID := range activeJobs {
			job, err := b.store.GetJob(ctx, jobID)
			if err != nil {
				continue
			}
			if job.Meta != nil {
				var metaObj map[string]any
				if json.Unmarshal(job.Meta, &metaObj) == nil {
					if td, ok := metaObj["test_directive"]; ok {
						if tdStr, ok := td.(string); ok && tdStr != "" {
							directive = tdStr
							break
						}
					}
				}
			}
		}
	}

	return &core.HeartbeatResponse{
		State:        "active",
		Directive:    directive,
		JobsExtended: extended,
		ServerTime:   core.FormatTime(now),
	}, nil
}

// ListDeadLetter returns dead letter jobs.
func (b *KafkaBackend) ListDeadLetter(ctx context.Context, limit, offset int) ([]*core.Job, int, error) {
	ids, total, err := b.store.GetDeadJobs(ctx, offset, limit)
	if err != nil {
		return nil, 0, err
	}

	var jobs []*core.Job
	for _, id := range ids {
		job, err := b.store.GetJob(ctx, id)
		if err == nil {
			jobs = append(jobs, job)
		}
	}
	return jobs, int(total), nil
}

// RetryDeadLetter retries a dead letter job.
func (b *KafkaBackend) RetryDeadLetter(ctx context.Context, jobID string) (*core.Job, error) {
	inDead, err := b.store.IsInDead(ctx, jobID)
	if err != nil || !inDead {
		return nil, core.NewNotFoundError("Dead letter job", jobID)
	}

	now := time.Now()
	job, err := b.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}

	b.store.RemoveFromDead(ctx, jobID)
	b.store.UpdateJob(ctx, jobID, map[string]any{
		"state":       core.StateAvailable,
		"attempt":     "0",
		"enqueued_at": core.FormatTime(now),
	})
	b.store.DeleteJobFields(ctx, jobID, "error", "error_history", "completed_at", "retry_delay_ms")

	score := computeScore(nil, now)
	b.store.AddToAvailable(ctx, job.Queue, jobID, score)

	return b.store.GetJob(ctx, jobID)
}

// DeleteDeadLetter removes a job from the dead letter queue.
func (b *KafkaBackend) DeleteDeadLetter(ctx context.Context, jobID string) error {
	return b.store.RemoveFromDead(ctx, jobID)
}

// RegisterCron registers a cron job.
func (b *KafkaBackend) RegisterCron(ctx context.Context, cronJob *core.CronJob) (*core.CronJob, error) {
	expr := cronJob.Expression
	if expr == "" {
		expr = cronJob.Schedule
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	var schedule cron.Schedule
	var err error

	if cronJob.Timezone != "" {
		loc, locErr := time.LoadLocation(cronJob.Timezone)
		if locErr != nil {
			return nil, core.NewInvalidRequestError(
				fmt.Sprintf("Invalid timezone: %s", cronJob.Timezone),
				map[string]any{"timezone": cronJob.Timezone},
			)
		}
		schedule, err = parser.Parse("CRON_TZ=" + loc.String() + " " + expr)
		if err != nil {
			schedule, err = parser.Parse(expr)
		}
	} else {
		schedule, err = parser.Parse(expr)
	}

	if err != nil {
		return nil, core.NewInvalidRequestError(
			fmt.Sprintf("Invalid cron expression: %s", expr),
			map[string]any{"expression": expr, "error": err.Error()},
		)
	}

	now := time.Now()
	cronJob.CreatedAt = core.FormatTime(now)
	cronJob.NextRunAt = core.FormatTime(schedule.Next(now))
	cronJob.Schedule = expr
	cronJob.Expression = expr

	if cronJob.Queue == "" {
		cronJob.Queue = "default"
	}
	if cronJob.OverlapPolicy == "" {
		cronJob.OverlapPolicy = "allow"
	}
	cronJob.Enabled = true

	data, err := json.Marshal(cronJob)
	if err != nil {
		return nil, fmt.Errorf("marshal cron job: %w", err)
	}
	b.store.SaveCron(ctx, cronJob.Name, data)

	return cronJob, nil
}

// ListCron lists all registered cron jobs.
func (b *KafkaBackend) ListCron(ctx context.Context) ([]*core.CronJob, error) {
	names, err := b.store.GetAllCronNames(ctx)
	if err != nil {
		return nil, err
	}

	sort.Strings(names)
	var crons []*core.CronJob
	for _, name := range names {
		data, err := b.store.GetCron(ctx, name)
		if err != nil {
			continue
		}
		var cj core.CronJob
		if err := json.Unmarshal(data, &cj); err == nil {
			crons = append(crons, &cj)
		}
	}
	return crons, nil
}

// DeleteCron removes a cron job.
func (b *KafkaBackend) DeleteCron(ctx context.Context, name string) (*core.CronJob, error) {
	data, err := b.store.GetCron(ctx, name)
	if err != nil {
		return nil, core.NewNotFoundError("Cron job", name)
	}

	var cj core.CronJob
	if err := json.Unmarshal(data, &cj); err != nil {
		return nil, fmt.Errorf("unmarshal cron job: %w", err)
	}

	b.store.DeleteCron(ctx, name)
	return &cj, nil
}

// CreateWorkflow creates and starts a workflow.
func (b *KafkaBackend) CreateWorkflow(ctx context.Context, req *core.WorkflowRequest) (*core.Workflow, error) {
	now := time.Now()
	wfID := core.NewUUIDv7()

	jobs := req.Jobs
	if req.Type == "chain" {
		jobs = req.Steps
	}
	total := len(jobs)

	wf := &core.Workflow{
		ID:        wfID,
		Name:      req.Name,
		Type:      req.Type,
		State:     "running",
		CreatedAt: core.FormatTime(now),
	}

	if req.Type == "chain" {
		wf.StepsTotal = &total
		zero := 0
		wf.StepsCompleted = &zero
	} else {
		wf.JobsTotal = &total
		zero := 0
		wf.JobsCompleted = &zero
	}

	wfHash := map[string]any{
		"id":         wfID,
		"type":       req.Type,
		"name":       req.Name,
		"state":      "running",
		"total":      strconv.Itoa(total),
		"completed":  "0",
		"failed":     "0",
		"created_at": core.FormatTime(now),
	}
	if req.Callbacks != nil {
		cbJSON, err := json.Marshal(req.Callbacks)
		if err != nil {
			return nil, fmt.Errorf("marshal workflow callbacks: %w", err)
		}
		wfHash["callbacks"] = string(cbJSON)
	}
	jobDefs, err := json.Marshal(jobs)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow job definitions: %w", err)
	}
	wfHash["job_defs"] = string(jobDefs)

	b.store.SaveWorkflow(ctx, wfID, wfHash)

	if req.Type == "chain" {
		step := jobs[0]
		queue := "default"
		if step.Options != nil && step.Options.Queue != "" {
			queue = step.Options.Queue
		}

		job := &core.Job{
			Type:         step.Type,
			Args:         step.Args,
			Queue:        queue,
			WorkflowID:   wfID,
			WorkflowStep: 0,
		}
		if step.Options != nil && step.Options.RetryPolicy != nil {
			job.Retry = step.Options.RetryPolicy
			job.MaxAttempts = &step.Options.RetryPolicy.MaxAttempts
		} else if step.Options != nil && step.Options.Retry != nil {
			job.Retry = step.Options.Retry
			job.MaxAttempts = &step.Options.Retry.MaxAttempts
		}

		created, err := b.Push(ctx, job)
		if err != nil {
			return nil, err
		}
		b.store.AppendWorkflowJob(ctx, wfID, created.ID)
	} else {
		for i, step := range jobs {
			queue := "default"
			if step.Options != nil && step.Options.Queue != "" {
				queue = step.Options.Queue
			}

			job := &core.Job{
				Type:         step.Type,
				Args:         step.Args,
				Queue:        queue,
				WorkflowID:   wfID,
				WorkflowStep: i,
			}
			if step.Options != nil && step.Options.RetryPolicy != nil {
				job.Retry = step.Options.RetryPolicy
				job.MaxAttempts = &step.Options.RetryPolicy.MaxAttempts
			} else if step.Options != nil && step.Options.Retry != nil {
				job.Retry = step.Options.Retry
				job.MaxAttempts = &step.Options.Retry.MaxAttempts
			}

			created, err := b.Push(ctx, job)
			if err != nil {
				return nil, err
			}
			b.store.AppendWorkflowJob(ctx, wfID, created.ID)
		}
	}

	return wf, nil
}

// GetWorkflow retrieves a workflow by ID.
func (b *KafkaBackend) GetWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	data, err := b.store.GetWorkflow(ctx, id)
	if err != nil {
		return nil, err
	}

	wf := &core.Workflow{
		ID:        data["id"],
		Name:      data["name"],
		Type:      data["type"],
		State:     data["state"],
		CreatedAt: data["created_at"],
	}
	if v, ok := data["completed_at"]; ok && v != "" {
		wf.CompletedAt = v
	}

	total, _ := strconv.Atoi(data["total"])
	completed, _ := strconv.Atoi(data["completed"])

	if wf.Type == "chain" {
		wf.StepsTotal = &total
		wf.StepsCompleted = &completed
	} else {
		wf.JobsTotal = &total
		wf.JobsCompleted = &completed
	}

	return wf, nil
}

// CancelWorkflow cancels a workflow and its active/pending jobs.
func (b *KafkaBackend) CancelWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	wf, err := b.GetWorkflow(ctx, id)
	if err != nil {
		return nil, err
	}

	if wf.State == "completed" || wf.State == "failed" || wf.State == "cancelled" {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot cancel workflow in state '%s'.", wf.State),
			nil,
		)
	}

	jobIDs, _ := b.store.GetWorkflowJobs(ctx, id)
	for _, jobID := range jobIDs {
		job, err := b.store.GetJob(ctx, jobID)
		if err == nil && !core.IsTerminalState(job.State) {
			b.Cancel(ctx, jobID)
		}
	}

	wf.State = "cancelled"
	wf.CompletedAt = core.NowFormatted()

	b.store.UpdateWorkflow(ctx, id, map[string]any{
		"state":        "cancelled",
		"completed_at": wf.CompletedAt,
	})

	return wf, nil
}

// AdvanceWorkflow advances workflow state after job completion or failure.
func (b *KafkaBackend) AdvanceWorkflow(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error {
	data, err := b.store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return nil
	}

	wfType := data["type"]
	state := data["state"]
	if state != "running" {
		return nil
	}

	total, _ := strconv.Atoi(data["total"])
	completed, _ := strconv.Atoi(data["completed"])
	failedCount, _ := strconv.Atoi(data["failed"])

	// Get workflow step from job
	job, _ := b.store.GetJob(ctx, jobID)
	stepIdx := 0
	if job != nil {
		stepIdx = job.WorkflowStep
	}

	if result != nil && len(result) > 0 {
		b.store.SetWorkflowResult(ctx, workflowID, stepIdx, result)
	}

	if failed {
		failedCount++
	} else {
		completed++
	}
	totalFinished := completed + failedCount

	updates := map[string]any{
		"completed": strconv.Itoa(completed),
		"failed":    strconv.Itoa(failedCount),
	}

	if wfType == "chain" {
		if failed {
			updates["state"] = "failed"
			updates["completed_at"] = core.NowFormatted()
			b.store.UpdateWorkflow(ctx, workflowID, updates)
			return nil
		}

		if totalFinished >= total {
			updates["state"] = "completed"
			updates["completed_at"] = core.NowFormatted()
			b.store.UpdateWorkflow(ctx, workflowID, updates)
			return nil
		}

		b.store.UpdateWorkflow(ctx, workflowID, updates)
		return b.enqueueChainStep(ctx, workflowID, data, stepIdx+1)
	}

	b.store.UpdateWorkflow(ctx, workflowID, updates)

	if totalFinished >= total {
		finalState := "completed"
		if failedCount > 0 {
			finalState = "failed"
		}
		b.store.UpdateWorkflow(ctx, workflowID, map[string]any{
			"state":        finalState,
			"completed_at": core.NowFormatted(),
		})

		if wfType == "batch" {
			b.fireBatchCallbacks(ctx, workflowID, data, failedCount > 0)
		}
	}

	return nil
}

func (b *KafkaBackend) enqueueChainStep(ctx context.Context, workflowID string, wfData map[string]string, stepIdx int) error {
	var jobDefs []core.WorkflowJobRequest
	if err := json.Unmarshal([]byte(wfData["job_defs"]), &jobDefs); err != nil {
		return err
	}

	if stepIdx >= len(jobDefs) {
		return nil
	}

	step := jobDefs[stepIdx]
	queue := "default"
	if step.Options != nil && step.Options.Queue != "" {
		queue = step.Options.Queue
	}

	var parentResults []json.RawMessage
	resultsData, _ := b.store.GetWorkflowResults(ctx, workflowID)
	for i := 0; i < stepIdx; i++ {
		if r, ok := resultsData[strconv.Itoa(i)]; ok {
			parentResults = append(parentResults, json.RawMessage(r))
		}
	}

	job := &core.Job{
		Type:          step.Type,
		Args:          step.Args,
		Queue:         queue,
		WorkflowID:    workflowID,
		WorkflowStep:  stepIdx,
		ParentResults: parentResults,
	}
	if step.Options != nil && step.Options.RetryPolicy != nil {
		job.Retry = step.Options.RetryPolicy
		job.MaxAttempts = &step.Options.RetryPolicy.MaxAttempts
	} else if step.Options != nil && step.Options.Retry != nil {
		job.Retry = step.Options.Retry
		job.MaxAttempts = &step.Options.Retry.MaxAttempts
	}

	created, err := b.Push(ctx, job)
	if err != nil {
		return err
	}

	b.store.AppendWorkflowJob(ctx, workflowID, created.ID)
	return nil
}

func (b *KafkaBackend) fireBatchCallbacks(ctx context.Context, workflowID string, wfData map[string]string, hasFailure bool) {
	cbStr, ok := wfData["callbacks"]
	if !ok || cbStr == "" {
		return
	}

	var callbacks core.WorkflowCallbacks
	if err := json.Unmarshal([]byte(cbStr), &callbacks); err != nil {
		return
	}

	if callbacks.OnComplete != nil {
		b.fireCallback(ctx, callbacks.OnComplete)
	}
	if !hasFailure && callbacks.OnSuccess != nil {
		b.fireCallback(ctx, callbacks.OnSuccess)
	}
	if hasFailure && callbacks.OnFailure != nil {
		b.fireCallback(ctx, callbacks.OnFailure)
	}
}

func (b *KafkaBackend) fireCallback(ctx context.Context, cb *core.WorkflowCallback) {
	queue := "default"
	if cb.Options != nil && cb.Options.Queue != "" {
		queue = cb.Options.Queue
	}
	b.Push(ctx, &core.Job{
		Type:  cb.Type,
		Args:  cb.Args,
		Queue: queue,
	})
}

// PushBatch atomically enqueues multiple jobs.
func (b *KafkaBackend) PushBatch(ctx context.Context, jobs []*core.Job) ([]*core.Job, error) {
	// Pre-validate all jobs before any writes to avoid partial batch failures
	for _, job := range jobs {
		if err := core.ValidateEnqueueRequest(&core.EnqueueRequest{
			Type: job.Type,
			Args: job.Args,
		}); err != nil {
			return nil, err
		}
	}

	now := time.Now()

	for _, job := range jobs {
		if job.ID == "" {
			job.ID = core.NewUUIDv7()
		}
		job.State = core.StateAvailable
		job.Attempt = 0
		job.CreatedAt = core.FormatTime(now)
		job.EnqueuedAt = core.FormatTime(now)

		score := computeScore(job.Priority, now)

		if err := b.store.SaveJob(ctx, job); err != nil {
			return nil, fmt.Errorf("batch save job: %w", err)
		}
		if err := b.store.AddToAvailable(ctx, job.Queue, job.ID, score); err != nil {
			return nil, fmt.Errorf("batch add to available: %w", err)
		}
		b.store.RegisterQueue(ctx, job.Queue)

		b.producer.ProduceJobAsync(ctx, job)
	}

	return jobs, nil
}

// QueueStats returns statistics for a queue.
func (b *KafkaBackend) QueueStats(ctx context.Context, name string) (*core.QueueStats, error) {
	available, _ := b.store.GetAvailableCount(ctx, name)
	active, _ := b.store.GetActiveCount(ctx, name)
	completed, _ := b.store.GetCompletedCount(ctx, name)

	status := "active"
	if paused, _ := b.store.IsQueuePaused(ctx, name); paused {
		status = "paused"
	}

	return &core.QueueStats{
		Queue:  name,
		Status: status,
		Stats: core.Stats{
			Available: int(available),
			Active:    int(active),
			Completed: int(completed),
		},
	}, nil
}

// PauseQueue pauses a queue.
func (b *KafkaBackend) PauseQueue(ctx context.Context, name string) error {
	return b.store.PauseQueue(ctx, name)
}

// ResumeQueue resumes a queue.
func (b *KafkaBackend) ResumeQueue(ctx context.Context, name string) error {
	return b.store.ResumeQueue(ctx, name)
}

// SetWorkerState sets a directive for a worker.
func (b *KafkaBackend) SetWorkerState(ctx context.Context, workerID string, directive string) error {
	return b.store.SetWorkerData(ctx, workerID, "directive", directive)
}

// advanceWorkflow advances a workflow when a job completes or fails.
func (b *KafkaBackend) advanceWorkflow(ctx context.Context, jobID, jobState string, result []byte) {
	job, err := b.store.GetJob(ctx, jobID)
	if err != nil || job.WorkflowID == "" {
		return
	}

	failed := jobState == core.StateDiscarded || jobState == core.StateCancelled
	b.AdvanceWorkflow(ctx, job.WorkflowID, jobID, json.RawMessage(result), failed)
}

// --- Scheduler-callable methods ---

// PromoteScheduled moves due scheduled jobs to their available queues.
func (b *KafkaBackend) PromoteScheduled(ctx context.Context) error {
	now := time.Now()
	ids, err := b.store.GetDueScheduled(ctx, now.UnixMilli())
	if err != nil {
		return err
	}

	for _, jobID := range ids {
		job, err := b.store.GetJob(ctx, jobID)
		if err != nil {
			b.store.RemoveFromScheduled(ctx, jobID)
			continue
		}

		score := computeScore(job.Priority, now)
		b.store.RemoveFromScheduled(ctx, jobID)
		b.store.UpdateJob(ctx, jobID, map[string]any{
			"state":       core.StateAvailable,
			"enqueued_at": core.FormatTime(now),
		})
		b.store.AddToAvailable(ctx, job.Queue, jobID, score)
	}
	return nil
}

// PromoteRetries moves due retry jobs to their available queues.
func (b *KafkaBackend) PromoteRetries(ctx context.Context) error {
	now := time.Now()
	ids, err := b.store.GetDueRetries(ctx, now.UnixMilli())
	if err != nil {
		return err
	}

	for _, jobID := range ids {
		job, err := b.store.GetJob(ctx, jobID)
		if err != nil {
			b.store.RemoveFromRetry(ctx, jobID)
			continue
		}

		score := computeScore(job.Priority, now)
		b.store.RemoveFromRetry(ctx, jobID)
		b.store.UpdateJob(ctx, jobID, map[string]any{
			"state":       core.StateAvailable,
			"enqueued_at": core.FormatTime(now),
		})
		b.store.AddToAvailable(ctx, job.Queue, jobID, score)
	}
	return nil
}

// RequeueStalled finds and requeues jobs that exceeded their visibility timeout.
func (b *KafkaBackend) RequeueStalled(ctx context.Context) error {
	queues, err := b.store.GetAllQueues(ctx)
	if err != nil {
		return err
	}

	now := time.Now()

	for _, queue := range queues {
		activeJobs, err := b.store.GetActiveJobs(ctx, queue)
		if err != nil {
			continue
		}

		for _, jobID := range activeJobs {
			visDeadline, err := b.store.GetVisibility(ctx, jobID)
			if err != nil {
				continue
			}

			deadline, err := time.Parse(core.TimeFormat, visDeadline)
			if err != nil {
				continue
			}

			if now.After(deadline) {
				job, err := b.store.GetJob(ctx, jobID)
				if err != nil {
					continue
				}

				score := computeScore(job.Priority, now)
				b.store.RemoveFromActive(ctx, queue, jobID)
				b.store.UpdateJob(ctx, jobID, map[string]any{
					"state":       core.StateAvailable,
					"started_at":  "",
					"worker_id":   "",
					"enqueued_at": core.FormatTime(now),
				})
				b.store.AddToAvailable(ctx, queue, jobID, score)
				b.store.DeleteVisibility(ctx, jobID)
			}
		}
	}
	return nil
}

// FireCronJobs checks cron schedules and fires due jobs.
func (b *KafkaBackend) FireCronJobs(ctx context.Context) error {
	names, err := b.store.GetAllCronNames(ctx)
	if err != nil {
		return err
	}

	now := time.Now()
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	for _, name := range names {
		data, err := b.store.GetCron(ctx, name)
		if err != nil {
			continue
		}

		var cronJob core.CronJob
		if err := json.Unmarshal(data, &cronJob); err != nil {
			continue
		}

		if cronJob.NextRunAt == "" {
			continue
		}

		nextRun, err := time.Parse(core.TimeFormat, cronJob.NextRunAt)
		if err != nil {
			continue
		}

		if now.Before(nextRun) {
			continue
		}

		lockKey := fmt.Sprintf("ojs:cron_lock:%s:%d", name, nextRun.Unix())
		acquired, err := b.store.AcquireCronLock(ctx, lockKey, 60000)
		if err != nil || !acquired {
			continue
		}

		var jobType string
		var args json.RawMessage
		var queue string
		if cronJob.JobTemplate != nil {
			jobType = cronJob.JobTemplate.Type
			args = cronJob.JobTemplate.Args
			if cronJob.JobTemplate.Options != nil {
				queue = cronJob.JobTemplate.Options.Queue
			}
		}
		if queue == "" {
			queue = "default"
		}

		// Check overlap policy
		if cronJob.OverlapPolicy == "skip" {
			instanceJobID, err := b.store.GetCronInstance(ctx, name)
			skipFire := false
			if err == nil && instanceJobID != "" {
				instanceJob, err := b.store.GetJob(ctx, instanceJobID)
				if err == nil && !core.IsTerminalState(instanceJob.State) {
					skipFire = true
				}
			}
			if skipFire {
				expr := cronJob.Expression
				if expr == "" {
					expr = cronJob.Schedule
				}
				schedule, err := parser.Parse(expr)
				if err == nil {
					cronJob.LastRunAt = core.FormatTime(now)
					cronJob.NextRunAt = core.FormatTime(schedule.Next(now))
					cronData, marshalErr := json.Marshal(cronJob)
					if marshalErr != nil {
						slog.Error("failed to marshal cron job", "cron", name, "error", marshalErr)
					} else {
						b.store.SaveCron(ctx, name, cronData)
					}
				}
				continue
			}
		}

		cronVisTimeout := 600000
		job := &core.Job{
			Type:                jobType,
			Args:                args,
			Queue:               queue,
			VisibilityTimeoutMs: &cronVisTimeout,
		}

		created, pushErr := b.Push(ctx, job)
		if pushErr != nil {
			continue
		}

		if cronJob.OverlapPolicy == "skip" {
			b.store.SetCronInstance(ctx, name, created.ID)
		}

		expr := cronJob.Expression
		if expr == "" {
			expr = cronJob.Schedule
		}
		schedule, err := parser.Parse(expr)
		if err == nil {
			cronJob.LastRunAt = core.FormatTime(now)
			cronJob.NextRunAt = core.FormatTime(schedule.Next(now))
			cronData, marshalErr := json.Marshal(cronJob)
			if marshalErr != nil {
				slog.Error("failed to marshal cron job", "cron", name, "error", marshalErr)
			} else {
				b.store.SaveCron(ctx, name, cronData)
			}
		}
	}
	return nil
}

// --- Helper functions ---

func computeScore(priority *int, enqueueTime time.Time) float64 {
	p := 0
	if priority != nil {
		p = *priority
	}
	return float64(100-p)*1e15 + float64(enqueueTime.UnixMilli())
}

func computeFingerprint(job *core.Job) string {
	h := sha256.New()
	keys := job.Unique.Keys
	if len(keys) == 0 {
		keys = []string{"type", "args"}
	}
	sort.Strings(keys)
	for _, key := range keys {
		switch key {
		case "type":
			h.Write([]byte("type:"))
			h.Write([]byte(job.Type))
		case "args":
			h.Write([]byte("args:"))
			if job.Args != nil {
				h.Write(job.Args)
			}
		case "queue":
			h.Write([]byte("queue:"))
			h.Write([]byte(job.Queue))
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func matchesPattern(s, pattern string) bool {
	// Convert glob-like pattern (with * wildcards) to safe regex.
	// All other regex metacharacters are escaped to prevent injection.
	escaped := regexp.QuoteMeta(pattern)
	// Restore * wildcards: QuoteMeta turns * into \*, convert back to .*
	safePattern := "^" + regexp.MustCompile(`\\\*`).ReplaceAllString(escaped, ".*") + "$"
	re, err := regexp.Compile(safePattern)
	if err != nil {
		return s == pattern
	}
	return re.MatchString(s)
}
