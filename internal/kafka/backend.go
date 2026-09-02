package kafka

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
	cpStore   *checkpointStore
	pushJobFn func(context.Context, *core.Job) (*core.Job, error)
	nowFn     func() time.Time
}

// workflowEffectLeaseMs bounds how long a single drainer may hold a workflow
// dispatch effect before another drainer may reclaim it after a crash.
const workflowEffectLeaseMs = 30_000

// New creates a new KafkaBackend.
func New(store state.Store, producer *Producer) *KafkaBackend {
	return &KafkaBackend{
		store:     store,
		producer:  producer,
		startTime: time.Now(),
		cpStore:   newCheckpointStore(),
		nowFn:     time.Now,
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
	score, scheduled := prepareJobForPush(job, now)

	// Persist. Unique jobs claim their fingerprint atomically with creation so a
	// crash or a race can never leave a dangling claim or two live duplicates.
	if job.Unique != nil {
		existing, claimed, err := b.pushUniqueJob(ctx, job, score, scheduled)
		if err != nil {
			return nil, err
		}
		if !claimed {
			return existing, nil
		}
	} else if err := b.store.AtomicPush(ctx, job, score, scheduled); err != nil {
		return nil, fmt.Errorf("atomic push job: %w", err)
	}

	if !scheduled && job.RateLimit != nil && job.RateLimit.MaxPerSecond > 0 {
		b.store.SetRateLimit(ctx, job.Queue, job.RateLimit.MaxPerSecond)
	}

	// Publish to Kafka asynchronously
	b.publishEnqueuedJob(ctx, job)

	return job, nil
}

// pushUniqueJob atomically claims the job's unique fingerprint and creates the
// job in one Redis transaction. It returns:
//
//   - (nil, true, nil)      the new job was claimed and created; the caller
//     should finish enqueueing (emit events) and return the new job.
//   - (existing, false, nil) the ignore policy applied; return existing as-is.
//   - (nil, false, err)      the reject policy applied or a failure occurred.
//
// For the replace policy the predecessor is compare-and-cancelled before the
// replacement is created in the same Lua script. Active or otherwise unsafe
// predecessors are rejected, and concurrent replacers that observed the same
// predecessor cannot both succeed.
func (b *KafkaBackend) pushUniqueJob(ctx context.Context, job *core.Job, score float64, scheduled bool) (*core.Job, bool, error) {
	fingerprint := computeFingerprint(job)
	ttlMs := int64(0)
	if job.Unique.Period != "" {
		if d, err := core.ParseISO8601Duration(job.Unique.Period); err == nil {
			ttlMs = d.Milliseconds()
		}
	}

	conflict := job.Unique.OnConflict
	if conflict == "" {
		conflict = "reject"
	}

	var result *state.UniqueClaimResult
	var err error
	if conflict == "replace" {
		expectedID, getErr := b.store.GetUniqueJobID(ctx, fingerprint)
		if getErr != nil {
			return nil, false, fmt.Errorf("get unique predecessor: %w", getErr)
		}
		result, err = b.store.ReplaceUniqueJob(
			ctx,
			fingerprint,
			expectedID,
			job,
			score,
			scheduled,
			ttlMs,
			job.Unique.States,
			core.NowFormatted(),
		)
	} else {
		result, err = b.store.ClaimUniqueJob(ctx, fingerprint, job, score, scheduled, ttlMs, conflict, job.Unique.States)
	}
	if err != nil {
		return nil, false, fmt.Errorf("claim unique job: %w", err)
	}

	switch result.Outcome {
	case state.UniqueClaimRejected:
		if conflict == "replace" && result.ExistingState == core.StateActive {
			return nil, false, core.NewConflictError(
				"Cannot replace an active unique job.",
				map[string]any{
					"existing_job_id": result.ExistingID,
					"current_state":   result.ExistingState,
					"unique_key":      fingerprint,
				},
			)
		}
		return nil, false, &core.OJSError{
			Code:    core.ErrCodeDuplicate,
			Message: "A job with the same unique key already exists.",
			Details: map[string]any{
				"existing_job_id": result.ExistingID,
				"unique_key":      fingerprint,
			},
		}
	case state.UniqueClaimIgnored:
		existingJob, getErr := b.store.GetJob(ctx, result.ExistingID)
		if getErr != nil {
			return nil, false, fmt.Errorf("load ignored unique job %q: %w", result.ExistingID, getErr)
		}
		existingJob.IsExisting = true
		return existingJob, false, nil
	case state.UniqueClaimClaimed:
		if result.ExistingID != "" {
			replaced, getErr := b.store.GetJob(ctx, result.ExistingID)
			if getErr == nil && replaced != nil {
				b.emitJobCancelled(ctx, replaced)
			}
		}
		return nil, true, nil
	default:
		return nil, false, fmt.Errorf("unexpected unique claim outcome %q", result.Outcome)
	}
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
			startedAt := core.FormatTime(now)

			// Atomically: validate available state, transition to active, move
			// queue indexes, and set visibility.
			jobID, err := b.store.AtomicFetch(ctx, queue, deadline, startedAt, workerID)
			if err != nil || jobID == "" {
				break
			}

			job, err := b.store.GetJob(ctx, jobID)
			if err != nil {
				continue
			}
			if job.State != core.StateActive {
				// A cancellation may have won immediately after the atomic
				// fetch. Never return or mutate that now-cancelled job.
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
		if job.State == core.StateCompleted {
			recovered, recoveryErr := b.advanceWorkflowJob(
				ctx,
				job.WorkflowID,
				job,
				job.Result,
				false,
			)
			if recoveryErr != nil {
				return nil, core.NewInternalError(
					fmt.Sprintf("job completed but workflow advancement recovery failed: %v", recoveryErr),
				)
			}
			if recovered {
				return &core.AckResponse{
					Acknowledged: true,
					ID:           jobID,
					State:        core.StateCompleted,
					CompletedAt:  job.CompletedAt,
					Job:          job,
				}, nil
			}
		}
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
	if len(result) > 0 {
		resultStr = string(result)
	}
	if err := b.store.AtomicAck(ctx, jobID, job.Queue, now, resultStr); err != nil {
		return nil, core.NewInternalError(fmt.Sprintf("atomic ack failed: %v", err))
	}

	job.State = core.StateCompleted
	job.CompletedAt = now
	if resultStr != "" {
		job.Result = append(json.RawMessage(nil), result...)
	}
	_, workflowErr := b.advanceWorkflowJob(
		ctx,
		job.WorkflowID,
		job,
		json.RawMessage(result),
		false,
	)

	// Emit Kafka events
	b.producer.EmitJobCompleted(ctx, jobID, job.Queue, job.Type, json.RawMessage(result))
	if workflowErr != nil {
		return nil, core.NewInternalError(
			fmt.Sprintf("job completed but workflow advancement failed: %v", workflowErr),
		)
	}

	updatedJob, err := b.store.GetJob(ctx, jobID)
	if err != nil {
		slog.Error("failed to fetch job after ack", "job_id", jobID, "error", err)
	}
	return &core.AckResponse{
		Acknowledged: true,
		ID:           jobID,
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
		if !requeue && job.State == core.StateDiscarded {
			recovered, recoveryErr := b.advanceWorkflowJob(
				ctx,
				job.WorkflowID,
				job,
				nil,
				true,
			)
			if recoveryErr != nil {
				return nil, core.NewInternalError(
					fmt.Sprintf("job discarded but workflow advancement recovery failed: %v", recoveryErr),
				)
			}
			if recovered {
				return &core.NackResponse{
					ID:          jobID,
					State:       core.StateDiscarded,
					Attempt:     job.Attempt,
					MaxAttempts: maxAttemptsForJob(job),
					DiscardedAt: job.CompletedAt,
					Job:         job,
				}, nil
			}
		}
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
	maxAttempts := maxAttemptsForJob(job)

	// Handle requeue
	if requeue {
		score := computeScore(job.Priority, now)
		if err := b.store.AtomicRequeue(ctx, jobID, job.Queue, core.FormatTime(now), score); err != nil {
			return nil, core.NewInternalError(fmt.Sprintf("atomic nack requeue failed: %v", err))
		}

		updatedJob, getErr := b.store.GetJob(ctx, jobID)
		if getErr != nil {
			slog.Error("failed to fetch job after nack requeue", "job_id", jobID, "error", getErr)
		}
		return &core.NackResponse{
			ID:          jobID,
			State:       core.StateAvailable,
			Attempt:     job.Attempt,
			MaxAttempts: maxAttempts,
			Job:         updatedJob,
		}, nil
	}

	newAttempt := job.Attempt + 1

	errJSON := buildJobErrorJSON(job, jobErr)

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

	isNonRetryable := resolveNonRetryable(job, jobErr)

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

		job.State = core.StateDiscarded
		job.Attempt = newAttempt
		job.CompletedAt = discardedAt
		_, workflowErr := b.advanceWorkflowJob(
			ctx,
			job.WorkflowID,
			job,
			nil,
			true,
		)
		b.producer.EmitJobDiscarded(ctx, jobID, job.Queue, job.Type)
		if workflowErr != nil {
			return nil, core.NewInternalError(
				fmt.Sprintf("job discarded but workflow advancement failed: %v", workflowErr),
			)
		}

		updatedJob, getErr := b.store.GetJob(ctx, jobID)
		if getErr != nil {
			slog.Error("failed to fetch job after discard", "job_id", jobID, "error", getErr)
		}
		return &core.NackResponse{
			ID:          jobID,
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
		ID:            jobID,
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
		if !isWorkflowJobAbsent(err) {
			return nil, fmt.Errorf("get job %q for cancellation: %w", jobID, err)
		}
		return nil, core.NewNotFoundError("Job", jobID)
	}

	now := core.NowFormatted()
	result, err := b.store.AtomicCancelJob(ctx, jobID, now)
	if err != nil {
		return nil, fmt.Errorf("atomic cancel job %q: %w", jobID, err)
	}
	if !result.Cancelled {
		if result.PreviousState == "" {
			return nil, core.NewNotFoundError("Job", jobID)
		}
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot cancel job in terminal state '%s'.", result.PreviousState),
			map[string]any{
				"job_id":        jobID,
				"current_state": result.PreviousState,
			},
		)
	}

	b.emitJobCancelled(ctx, job)

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
	if err := b.store.SaveCron(ctx, cronJob.Name, data); err != nil {
		return nil, fmt.Errorf("save cron job: %w", err)
	}

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

	if err := b.store.DeleteCron(ctx, name); err != nil {
		return nil, fmt.Errorf("delete cron job: %w", err)
	}
	return &cj, nil
}

// CreateWorkflow creates and starts a workflow.
func (b *KafkaBackend) CreateWorkflow(ctx context.Context, req *core.WorkflowRequest) (*core.Workflow, error) {
	if req == nil {
		return nil, core.NewInvalidRequestError("Workflow request is required.", nil)
	}

	var jobs []core.WorkflowJobRequest
	switch req.Type {
	case "chain":
		jobs = req.Steps
	case "group", "batch":
		jobs = req.Jobs
	default:
		return nil, core.NewInvalidRequestError(
			fmt.Sprintf("Unsupported workflow type %q.", req.Type),
			map[string]any{"type": req.Type},
		)
	}
	if len(jobs) == 0 {
		return nil, core.NewInvalidRequestError(
			fmt.Sprintf("Workflow type %q requires at least one job.", req.Type),
			map[string]any{"type": req.Type},
		)
	}
	for i, job := range jobs {
		if job.Type == "" {
			return nil, core.NewInvalidRequestError(
				fmt.Sprintf("Workflow job %d type is required.", i),
				map[string]any{"index": i},
			)
		}
		if job.Options != nil && job.Options.Unique != nil {
			return nil, core.NewInvalidRequestError(
				"Unique jobs are not supported inside workflows by this backend.",
				map[string]any{
					"workflow_type": req.Type,
					"job_index":     i,
				},
			)
		}
	}
	if req.Callbacks != nil {
		callbacks := map[string]*core.WorkflowCallback{
			"on_success":  req.Callbacks.OnSuccess,
			"on_failure":  req.Callbacks.OnFailure,
			"on_complete": req.Callbacks.OnComplete,
		}
		for name, callback := range callbacks {
			if callback != nil && callback.Options != nil && callback.Options.Unique != nil {
				return nil, core.NewInvalidRequestError(
					"Unique jobs are not supported for workflow callbacks by this backend.",
					map[string]any{
						"workflow_type": req.Type,
						"callback":      name,
					},
				)
			}
		}
	}

	now := time.Now()
	wfID := core.NewUUIDv7()

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

	if err := b.store.SaveWorkflow(ctx, wfID, wfHash); err != nil {
		return nil, fmt.Errorf("save workflow: %w", err)
	}

	if req.Type == "chain" {
		created, err := b.pushJob(ctx, buildWorkflowStepJob(jobs[0], wfID, 0, nil))
		if err != nil {
			return nil, err
		}
		if err := b.store.AppendWorkflowJob(ctx, wfID, created.ID); err != nil {
			return nil, fmt.Errorf("append workflow job: %w", err)
		}
	} else {
		for i, step := range jobs {
			created, err := b.pushJob(ctx, buildWorkflowStepJob(step, wfID, i, nil))
			if err != nil {
				return nil, err
			}
			if err := b.store.AppendWorkflowJob(ctx, wfID, created.ID); err != nil {
				return nil, fmt.Errorf("append workflow job: %w", err)
			}
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

// CancelWorkflow atomically fences the workflow outbox before cancelling every
// job already associated with the workflow, including stable effect IDs that
// may have been created immediately before the cancellation fence.
func (b *KafkaBackend) CancelWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	cancelledAt := core.NowFormatted()
	cancelResult, err := b.store.AtomicCancelWorkflow(ctx, id, cancelledAt)
	if err != nil {
		return nil, fmt.Errorf("atomically cancel workflow %q: %w", id, err)
	}
	if cancelResult.State == "missing" {
		return nil, core.NewNotFoundError("Workflow", id)
	}
	if cancelResult.State != "cancelled" {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot cancel workflow in state '%s'.", cancelResult.State),
			nil,
		)
	}

	jobIDs, err := b.store.GetWorkflowJobs(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get workflow jobs: %w", err)
	}
	jobIDs = append(jobIDs, cancelResult.EffectJobIDs...)
	seen := make(map[string]struct{}, len(jobIDs))
	var cancelErrors []error
	for _, jobID := range jobIDs {
		if jobID == "" {
			continue
		}
		if _, duplicate := seen[jobID]; duplicate {
			continue
		}
		seen[jobID] = struct{}{}
		job, err := b.store.GetJob(ctx, jobID)
		if err != nil {
			if isWorkflowJobAbsent(err) {
				continue
			}
			cancelErrors = append(cancelErrors, fmt.Errorf("get workflow job %q: %w", jobID, err))
			continue
		}
		if !core.IsTerminalState(job.State) {
			if _, err := b.Cancel(ctx, jobID); err != nil {
				var ojsErr *core.OJSError
				if errors.As(err, &ojsErr) && ojsErr.Code == core.ErrCodeConflict {
					continue
				}
				cancelErrors = append(cancelErrors, fmt.Errorf("cancel workflow job %q: %w", jobID, err))
			}
		}
	}
	if err := errors.Join(cancelErrors...); err != nil {
		return nil, err
	}

	wf, err := b.GetWorkflow(ctx, id)
	if err != nil {
		return nil, err
	}
	if !cancelResult.Applied {
		return nil, core.NewConflictError(
			"Cannot cancel workflow in state 'cancelled'.",
			nil,
		)
	}
	return wf, nil
}

// AdvanceWorkflow advances workflow state after job completion or failure.
func (b *KafkaBackend) AdvanceWorkflow(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error {
	job, err := b.store.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get workflow job %q: %w", jobID, err)
	}
	_, err = b.advanceWorkflowJob(ctx, workflowID, job, result, failed)
	return err
}

func (b *KafkaBackend) advanceWorkflowJob(ctx context.Context, workflowID string, job *core.Job, result json.RawMessage, failed bool) (bool, error) {
	if workflowID == "" {
		return false, nil
	}

	completedAt := job.CompletedAt
	if completedAt == "" {
		completedAt = core.NowFormatted()
	}
	advance, err := b.store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
		WorkflowID:  workflowID,
		JobID:       job.ID,
		Step:        job.WorkflowStep,
		Result:      result,
		Failed:      failed,
		CompletedAt: completedAt,
		// Stable, preassigned identities for whichever dispatch effect the
		// advancement activates. Only the chosen effect's ID is persisted.
		NextChainJobID:  core.NewUUIDv7(),
		OnCompleteJobID: core.NewUUIDv7(),
		OnSuccessJobID:  core.NewUUIDv7(),
		OnFailureJobID:  core.NewUUIDv7(),
	})
	if err != nil {
		return false, fmt.Errorf("advance workflow %q: %w", workflowID, err)
	}

	// The advancement durably recorded any follow-up dispatch effects, so drain
	// them here for low latency. A crash or cancellation before the drain leaves
	// the effects pending for the scheduler drain or another worker to finish;
	// the same worker is never required to retry.
	recovered := advance.Applied
	if advance.HasPendingEffects {
		drained, drainErr := b.processWorkflowEffects(ctx, workflowID)
		if drained {
			recovered = true
		}
		if drainErr != nil {
			return recovered, drainErr
		}
	}
	return recovered, nil
}

// DrainWorkflowEffects processes the durable dispatch outbox for every workflow
// that has pending effects. It is safe to run repeatedly and concurrently with
// the operation-path drain; effect leases and stable job IDs guarantee each
// effect is dispatched exactly once.
func (b *KafkaBackend) DrainWorkflowEffects(ctx context.Context) error {
	ids, err := b.store.GetWorkflowsWithPendingEffects(ctx)
	if err != nil {
		return fmt.Errorf("list workflows with pending effects: %w", err)
	}
	var errs []error
	for _, id := range ids {
		if _, err := b.processWorkflowEffects(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("drain workflow %q effects: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// processWorkflowEffects drains a single workflow's dispatch outbox. Each effect
// is leased, then processed idempotently: the preassigned job is created only if
// it does not already exist (reconciling an ambiguous push by stable job ID),
// and the effect is completed only after the job is confirmed. Effects are
// independent so one failing effect never suppresses the others. It reports
// whether any effect still needed work.
func (b *KafkaBackend) processWorkflowEffects(ctx context.Context, workflowID string) (bool, error) {
	wfData, err := b.store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return false, fmt.Errorf("get workflow %q for effects: %w", workflowID, err)
	}
	if wfData["state"] == "cancelled" {
		return false, nil
	}

	effects, err := b.store.GetWorkflowEffects(ctx, workflowID)
	if err != nil {
		return false, fmt.Errorf("get workflow %q effects: %w", workflowID, err)
	}
	if len(effects) == 0 {
		return false, nil
	}

	effectIDs := make([]string, 0, len(effects))
	for id := range effects {
		effectIDs = append(effectIDs, id)
	}
	sort.Strings(effectIDs)

	hadWork := false
	var errs []error
	for _, effectID := range effectIDs {
		status, _ := parseWorkflowEffectValue(effects[effectID])
		if status == "done" {
			continue
		}
		hadWork = true
		if err := b.dispatchWorkflowEffect(ctx, workflowID, wfData, effectID); err != nil {
			errs = append(errs, err)
		}
	}
	return hadWork, errors.Join(errs...)
}

// dispatchWorkflowEffect leases, dispatches, and completes a single effect.
func (b *KafkaBackend) dispatchWorkflowEffect(ctx context.Context, workflowID string, wfData map[string]string, effectID string) error {
	owner := core.NewUUIDv7()
	claim, err := b.store.ClaimWorkflowEffect(ctx, workflowID, effectID, owner, b.now().UnixMilli(), workflowEffectLeaseMs)
	if err != nil {
		return fmt.Errorf("claim workflow effect %q: %w", effectID, err)
	}
	switch claim.Status {
	case state.WorkflowEffectDone, state.WorkflowEffectGone, state.WorkflowEffectBusy, state.WorkflowEffectFenced:
		// Already handled, cleaned up, or leased by another drainer.
		return nil
	case state.WorkflowEffectClaimed:
	default:
		return fmt.Errorf("workflow effect %q returned unknown claim status %q", effectID, claim.Status)
	}

	job, appendJob, err := b.buildWorkflowEffectJob(ctx, workflowID, wfData, effectID, claim.JobID)
	if err != nil {
		return b.releaseWorkflowEffectAfterError(
			ctx,
			workflowID,
			effectID,
			owner,
			fmt.Errorf("build workflow effect %q: %w", effectID, err),
		)
	}
	if job != nil {
		score, scheduled := prepareJobForPush(job, b.now())
		createStatus, createErr := b.store.AtomicCreateWorkflowEffectJob(
			ctx,
			workflowID,
			effectID,
			owner,
			job,
			score,
			scheduled,
		)
		if createErr != nil {
			return b.releaseWorkflowEffectAfterError(
				ctx,
				workflowID,
				effectID,
				owner,
				fmt.Errorf("create workflow effect %q job: %w", effectID, createErr),
			)
		}
		createdNew := false
		switch createStatus {
		case state.WorkflowEffectJobCreated:
			createdNew = true
		case state.WorkflowEffectJobExisting:
			existing, getErr := b.store.GetJob(ctx, claim.JobID)
			if getErr != nil {
				return b.releaseWorkflowEffectAfterError(
					ctx,
					workflowID,
					effectID,
					owner,
					fmt.Errorf("reconcile workflow effect %q job %q: %w", effectID, claim.JobID, getErr),
				)
			}
			if !sameWorkflowEffectJob(existing, job) {
				return b.releaseWorkflowEffectAfterError(
					ctx,
					workflowID,
					effectID,
					owner,
					fmt.Errorf("workflow effect %q stable job %q belongs to a different job", effectID, claim.JobID),
				)
			}
		case state.WorkflowEffectJobFenced, state.WorkflowEffectJobNotOwner, state.WorkflowEffectJobGone:
			return nil
		default:
			return b.releaseWorkflowEffectAfterError(
				ctx,
				workflowID,
				effectID,
				owner,
				fmt.Errorf("workflow effect %q returned unknown create status %q", effectID, createStatus),
			)
		}
		if job.RateLimit != nil && job.RateLimit.MaxPerSecond > 0 {
			if err := b.store.SetRateLimit(ctx, job.Queue, job.RateLimit.MaxPerSecond); err != nil {
				return b.releaseWorkflowEffectAfterError(
					ctx,
					workflowID,
					effectID,
					owner,
					fmt.Errorf("set workflow effect %q rate limit: %w", effectID, err),
				)
			}
		}
		if createdNew {
			b.publishEnqueuedJob(ctx, job)
		}
	}

	completed, err := b.store.CompleteWorkflowEffect(ctx, workflowID, effectID, owner, claim.JobID, appendJob)
	if err != nil {
		return fmt.Errorf("complete workflow effect %q: %w", effectID, err)
	}
	if !completed {
		// The lease was reclaimed or the workflow was cancelled after creation.
		// Only the current lease owner may append/complete the effect.
		return nil
	}
	return nil
}

func (b *KafkaBackend) releaseWorkflowEffectAfterError(ctx context.Context, workflowID string, effectID string, owner string, cause error) error {
	if err := b.store.ReleaseWorkflowEffect(ctx, workflowID, effectID, owner); err != nil {
		return errors.Join(cause, fmt.Errorf("release workflow effect %q: %w", effectID, err))
	}
	return cause
}

// buildWorkflowEffectJob reconstructs the job for a dispatch effect from the
// workflow definition and the effect's stable preassigned job ID. It returns a
// nil job (with no error) when the effect refers to an undefined callback or a
// non-existent chain step, which is completed as a no-op. The bool reports
// whether the created job should be appended to the workflow job list (chain
// steps are tracked; fire-and-forget callbacks are not).
func (b *KafkaBackend) buildWorkflowEffectJob(ctx context.Context, workflowID string, wfData map[string]string, effectID string, jobID string) (*core.Job, bool, error) {
	kind, ref := parseWorkflowEffectID(effectID)
	switch kind {
	case "chain":
		stepIdx, err := strconv.Atoi(ref)
		if err != nil {
			return nil, false, fmt.Errorf("invalid chain effect step %q: %w", ref, err)
		}
		var jobDefs []core.WorkflowJobRequest
		if err := json.Unmarshal([]byte(wfData["job_defs"]), &jobDefs); err != nil {
			return nil, false, fmt.Errorf("unmarshal workflow job definitions: %w", err)
		}
		if stepIdx < 0 || stepIdx >= len(jobDefs) {
			return nil, false, nil
		}
		var parentResults []json.RawMessage
		resultsData, err := b.store.GetWorkflowResults(ctx, workflowID)
		if err != nil {
			return nil, false, fmt.Errorf("get workflow results: %w", err)
		}
		for i := 0; i < stepIdx; i++ {
			if r, ok := resultsData[strconv.Itoa(i)]; ok {
				parentResults = append(parentResults, json.RawMessage(r))
			}
		}
		job := buildWorkflowStepJob(jobDefs[stepIdx], workflowID, stepIdx, parentResults)
		if job.Unique != nil {
			return nil, false, fmt.Errorf("workflow step uniqueness is not supported")
		}
		job.ID = jobID
		return job, true, nil
	case "callback":
		cbStr, ok := wfData["callbacks"]
		if !ok || cbStr == "" {
			return nil, false, nil
		}
		var callbacks core.WorkflowCallbacks
		if err := json.Unmarshal([]byte(cbStr), &callbacks); err != nil {
			return nil, false, fmt.Errorf("unmarshal workflow callbacks: %w", err)
		}
		var cb *core.WorkflowCallback
		switch ref {
		case "on_complete":
			cb = callbacks.OnComplete
		case "on_success":
			cb = callbacks.OnSuccess
		case "on_failure":
			cb = callbacks.OnFailure
		}
		if cb == nil {
			return nil, false, nil
		}
		job := &core.Job{Type: cb.Type, Args: cb.Args, Queue: "default"}
		applyEnqueueOptions(job, cb.Options)
		if job.Unique != nil {
			return nil, false, fmt.Errorf("workflow callback uniqueness is not supported")
		}
		job.ID = jobID
		return job, false, nil
	default:
		return nil, false, fmt.Errorf("unknown workflow effect kind %q", kind)
	}
}

// parseWorkflowEffectID splits an effect ID into its kind and reference, e.g.
// "chain:1" -> ("chain", "1") and "callback:on_complete" -> ("callback", "on_complete").
func parseWorkflowEffectID(effectID string) (kind string, ref string) {
	if idx := strings.IndexByte(effectID, ':'); idx >= 0 {
		return effectID[:idx], effectID[idx+1:]
	}
	return effectID, ""
}

// parseWorkflowEffectValue extracts the status and job ID from a stored effect
// value ("pending|jobID", "active|owner|leaseUntil|jobID", or "done|jobID").
func parseWorkflowEffectValue(value string) (status string, jobID string) {
	parts := strings.Split(value, "|")
	if len(parts) == 0 {
		return "", ""
	}
	switch parts[0] {
	case "active":
		if len(parts) >= 4 {
			return parts[0], parts[3]
		}
	default:
		if len(parts) >= 2 {
			return parts[0], parts[1]
		}
	}
	return parts[0], ""
}

// isWorkflowJobAbsent reports whether err definitively means the job does not exist.
func isWorkflowJobAbsent(err error) bool {
	if err == nil {
		return false
	}
	var ojsErr *core.OJSError
	return errors.As(err, &ojsErr) && ojsErr.Code == core.ErrCodeNotFound
}

func sameWorkflowEffectJob(existing *core.Job, expected *core.Job) bool {
	if existing == nil || expected == nil {
		return false
	}
	return existing.ID == expected.ID &&
		existing.Type == expected.Type &&
		existing.Queue == expected.Queue &&
		existing.WorkflowID == expected.WorkflowID &&
		existing.WorkflowStep == expected.WorkflowStep &&
		bytes.Equal(existing.Args, expected.Args)
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

// --- Scheduler-callable methods ---

// PromoteScheduled moves due scheduled jobs to their available queues.
func (b *KafkaBackend) PromoteScheduled(ctx context.Context) error {
	now := time.Now()
	ids, err := b.store.GetDueScheduled(ctx, now.UnixMilli())
	if err != nil {
		return err
	}

	var promotionErrors []error
	for _, jobID := range ids {
		job, err := b.store.GetJob(ctx, jobID)
		if err != nil {
			if !isWorkflowJobAbsent(err) {
				promotionErrors = append(promotionErrors, fmt.Errorf("get scheduled job %q: %w", jobID, err))
				continue
			}
			if removeErr := b.store.RemoveFromScheduled(ctx, jobID); removeErr != nil {
				promotionErrors = append(promotionErrors, fmt.Errorf("remove missing scheduled job %q: %w", jobID, removeErr))
			}
			continue
		}

		score := computeScore(job.Priority, now)
		if _, err := b.store.AtomicPromote(ctx, jobID, job.Queue, core.StateScheduled, core.FormatTime(now), score); err != nil {
			promotionErrors = append(promotionErrors, fmt.Errorf("promote scheduled job %q: %w", jobID, err))
		}
	}
	return errors.Join(promotionErrors...)
}

// PromoteRetries moves due retry jobs to their available queues.
func (b *KafkaBackend) PromoteRetries(ctx context.Context) error {
	now := time.Now()
	ids, err := b.store.GetDueRetries(ctx, now.UnixMilli())
	if err != nil {
		return err
	}

	var promotionErrors []error
	for _, jobID := range ids {
		job, err := b.store.GetJob(ctx, jobID)
		if err != nil {
			if !isWorkflowJobAbsent(err) {
				promotionErrors = append(promotionErrors, fmt.Errorf("get retryable job %q: %w", jobID, err))
				continue
			}
			if removeErr := b.store.RemoveFromRetry(ctx, jobID); removeErr != nil {
				promotionErrors = append(promotionErrors, fmt.Errorf("remove missing retry job %q: %w", jobID, removeErr))
			}
			continue
		}

		score := computeScore(job.Priority, now)
		if _, err := b.store.AtomicPromote(ctx, jobID, job.Queue, core.StateRetryable, core.FormatTime(now), score); err != nil {
			promotionErrors = append(promotionErrors, fmt.Errorf("promote retryable job %q: %w", jobID, err))
		}
	}
	return errors.Join(promotionErrors...)
}

// RequeueStalled finds and requeues jobs that exceeded their visibility timeout.
func (b *KafkaBackend) RequeueStalled(ctx context.Context) error {
	queues, err := b.store.GetAllQueues(ctx)
	if err != nil {
		return err
	}

	now := time.Now()
	var transitionErrors []error

	for _, queue := range queues {
		activeJobs, err := b.store.GetActiveJobs(ctx, queue)
		if err != nil {
			transitionErrors = append(transitionErrors, fmt.Errorf("get active jobs for queue %q: %w", queue, err))
			continue
		}

		for _, jobID := range activeJobs {
			visDeadline, err := b.store.GetVisibility(ctx, jobID)
			if err != nil {
				transitionErrors = append(transitionErrors, fmt.Errorf("get visibility for stalled job %q: %w", jobID, err))
				continue
			}

			deadline, err := time.Parse(core.TimeFormat, visDeadline)
			if err != nil {
				transitionErrors = append(transitionErrors, fmt.Errorf("parse visibility for stalled job %q: %w", jobID, err))
				continue
			}

			if now.After(deadline) {
				job, err := b.store.GetJob(ctx, jobID)
				if err != nil {
					transitionErrors = append(transitionErrors, fmt.Errorf("get stalled job %q: %w", jobID, err))
					continue
				}

				score := computeScore(job.Priority, now)
				if err := b.store.AtomicRequeue(ctx, jobID, queue, core.FormatTime(now), score); err != nil {
					transitionErrors = append(transitionErrors, fmt.Errorf("requeue stalled job %q: %w", jobID, err))
				}
			}
		}
	}
	return errors.Join(transitionErrors...)
}

// FireCronJobs checks cron schedules and fires due jobs.
func (b *KafkaBackend) FireCronJobs(ctx context.Context) error {
	names, err := b.store.GetAllCronNames(ctx)
	if err != nil {
		return err
	}

	now := b.now()
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	var cronErrors []error

	for _, name := range names {
		data, err := b.store.GetCron(ctx, name)
		if err != nil {
			cronErrors = append(cronErrors, fmt.Errorf("get cron %q: %w", name, err))
			continue
		}

		var cronJob core.CronJob
		if err := json.Unmarshal(data, &cronJob); err != nil {
			cronErrors = append(cronErrors, fmt.Errorf("unmarshal cron %q: %w", name, err))
			continue
		}

		if cronJob.NextRunAt == "" {
			continue
		}

		nextRun, err := time.Parse(core.TimeFormat, cronJob.NextRunAt)
		if err != nil {
			cronErrors = append(cronErrors, fmt.Errorf("parse next run for cron %q: %w", name, err))
			continue
		}

		if now.Before(nextRun) {
			continue
		}

		lockKey := fmt.Sprintf("ojs:cron_lock:%s:%d", name, nextRun.Unix())
		acquired, err := b.store.AcquireCronLock(ctx, lockKey, 60000)
		if err != nil {
			cronErrors = append(cronErrors, fmt.Errorf("acquire cron lock %q: %w", name, err))
			continue
		}
		if !acquired {
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
			if err != nil {
				cronErrors = append(cronErrors, fmt.Errorf("get cron instance %q: %w", name, err))
				continue
			}
			skipFire := false
			if instanceJobID != "" {
				instanceJob, err := b.store.GetJob(ctx, instanceJobID)
				if err != nil {
					var ojsErr *core.OJSError
					if !errors.As(err, &ojsErr) || ojsErr.Code != core.ErrCodeNotFound {
						cronErrors = append(cronErrors, fmt.Errorf("get cron instance job %q: %w", instanceJobID, err))
						continue
					}
				} else if !core.IsTerminalState(instanceJob.State) {
					skipFire = true
				}
			}
			if skipFire {
				if err := b.advanceCronNextRun(ctx, &cronJob, name, nextRun, now, parser); err != nil {
					cronErrors = append(cronErrors, err)
				}
				continue
			}
		}

		cronVisTimeout := 600000
		occurrenceJobID, idErr := cronOccurrenceJobID(name, nextRun)
		if idErr != nil {
			cronErrors = append(cronErrors, fmt.Errorf("derive cron occurrence %q job ID: %w", name, idErr))
			continue
		}
		job := &core.Job{
			ID:                  occurrenceJobID,
			Type:                jobType,
			Args:                args,
			Queue:               queue,
			VisibilityTimeoutMs: &cronVisTimeout,
		}

		owner := core.NewUUIDv7()
		claim, claimErr := b.store.ClaimCronOccurrence(
			ctx,
			name,
			nextRun.UnixMilli(),
			owner,
			job.ID,
			now.UnixMilli(),
			60000,
		)
		if claimErr != nil {
			cronErrors = append(cronErrors, fmt.Errorf("claim cron occurrence %q at %s: %w", name, cronJob.NextRunAt, claimErr))
			continue
		}
		switch claim.Status {
		case state.CronOccurrenceBusy:
			continue
		case state.CronOccurrenceFired:
			if claim.JobID == "" {
				cronErrors = append(cronErrors, fmt.Errorf("cron occurrence %q at %s is fired without a job ID", name, cronJob.NextRunAt))
				continue
			}
			if cronJob.OverlapPolicy == "skip" {
				if err := b.store.SetCronInstance(ctx, name, claim.JobID); err != nil {
					cronErrors = append(cronErrors, fmt.Errorf("restore cron instance %q: %w", name, err))
					continue
				}
			}
			if err := b.advanceCronNextRunAndReleaseMarker(
				ctx,
				&cronJob,
				name,
				nextRun,
				now,
				parser,
				owner,
				claim.JobID,
			); err != nil {
				cronErrors = append(cronErrors, err)
			}
			continue
		case state.CronOccurrenceAcquired:
		default:
			cronErrors = append(cronErrors, fmt.Errorf("cron occurrence %q returned unknown claim status %q", name, claim.Status))
			continue
		}

		created, _, pushErr := b.pushJobIfAbsent(ctx, job)
		if pushErr != nil {
			// The push outcome is ambiguous: the job may have been persisted
			// before the error surfaced. Reconcile by the preassigned job ID
			// instead of releasing the claim, which would let a later cycle
			// create a duplicate.
			existing, getErr := b.store.GetJob(ctx, job.ID)
			switch {
			case getErr == nil && existing != nil:
				// The job is durably persisted, so treat this as a completed
				// fire and finalize the occurrence below.
				created = existing
			case isCronJobAbsent(getErr):
				// Absence is definitive, so releasing the claim is safe and a
				// later cycle can retry the push cleanly.
				releaseErr := b.store.ReleaseCronOccurrence(ctx, name, nextRun.UnixMilli(), owner, job.ID)
				cronErrors = append(cronErrors, errors.Join(
					fmt.Errorf("push cron occurrence %q at %s: %w", name, cronJob.NextRunAt, pushErr),
					releaseErr,
				))
				continue
			default:
				// The outcome cannot be proven. Retain the pending marker so
				// lease recovery reconciles it: the claim script detects the
				// preassigned job if it exists, otherwise re-acquires and
				// retries. Deleting the claim here risks a duplicate.
				cronErrors = append(cronErrors, errors.Join(
					fmt.Errorf("push cron occurrence %q at %s: %w", name, cronJob.NextRunAt, pushErr),
					getErr,
				))
				continue
			}
		} else if created == nil || created.ID != job.ID {
			releaseErr := b.store.ReleaseCronOccurrence(ctx, name, nextRun.UnixMilli(), owner, job.ID)
			cronErrors = append(cronErrors, errors.Join(
				fmt.Errorf("push cron occurrence %q returned an unexpected job ID", name),
				releaseErr,
			))
			continue
		}
		if err := b.store.CompleteCronOccurrence(ctx, name, nextRun.UnixMilli(), owner, job.ID); err != nil {
			cronErrors = append(cronErrors, fmt.Errorf("complete cron occurrence %q at %s: %w", name, cronJob.NextRunAt, err))
			continue
		}

		if cronJob.OverlapPolicy == "skip" {
			if err := b.store.SetCronInstance(ctx, name, created.ID); err != nil {
				cronErrors = append(cronErrors, fmt.Errorf("set cron instance %q: %w", name, err))
				continue
			}
		}

		if err := b.advanceCronNextRunAndReleaseMarker(
			ctx,
			&cronJob,
			name,
			nextRun,
			now,
			parser,
			owner,
			created.ID,
		); err != nil {
			cronErrors = append(cronErrors, err)
		}
	}
	return errors.Join(cronErrors...)
}

// advanceCronNextRun recomputes a cron job's LastRunAt/NextRunAt and persists
// the update. LastRunAt records the occurrence that was just fired, while
// NextRunAt is set to the first scheduled time strictly after the current
// evaluation time (now). Using now rather than the fired occurrence means a
// long downtime yields exactly one catch-up run and then a future cursor,
// instead of replaying every missed occurrence one cycle at a time.
func (b *KafkaBackend) advanceCronNextRun(ctx context.Context, cronJob *core.CronJob, name string, occurrence time.Time, now time.Time, parser cron.Parser) error {
	expr := cronJob.Expression
	if expr == "" {
		expr = cronJob.Schedule
	}
	schedule, err := parser.Parse(expr)
	if err != nil {
		return fmt.Errorf("parse cron %q schedule: %w", name, err)
	}
	cronJob.LastRunAt = core.FormatTime(occurrence)
	// schedule.Next returns the first activation strictly after the argument.
	// Anchor it at now (which is >= occurrence, since the job only fires when
	// it is due) so the cursor always lands strictly in the future.
	cursor := now
	if occurrence.After(cursor) {
		cursor = occurrence
	}
	cronJob.NextRunAt = core.FormatTime(schedule.Next(cursor))
	cronData, marshalErr := json.Marshal(cronJob)
	if marshalErr != nil {
		return fmt.Errorf("marshal cron %q: %w", name, marshalErr)
	}
	if err := b.store.SaveCron(ctx, name, cronData); err != nil {
		return fmt.Errorf("save cron %q cursor: %w", name, err)
	}
	return nil
}

func (b *KafkaBackend) advanceCronNextRunAndReleaseMarker(
	ctx context.Context,
	cronJob *core.CronJob,
	name string,
	occurrence time.Time,
	now time.Time,
	parser cron.Parser,
	owner string,
	jobID string,
) error {
	if err := b.advanceCronNextRun(ctx, cronJob, name, occurrence, now, parser); err != nil {
		return err
	}
	if err := b.store.ReleaseCronOccurrence(ctx, name, occurrence.UnixMilli(), owner, jobID); err != nil {
		return fmt.Errorf("release persisted cron occurrence %q at %s: %w", name, core.FormatTime(occurrence), err)
	}
	return nil
}

// isCronJobAbsent reports whether err definitively means the job does not
// exist in the state store (a not-found error). Any other error is treated as
// an unknown outcome so callers do not release a claim prematurely.
func isCronJobAbsent(err error) bool {
	if err == nil {
		return false
	}
	var ojsErr *core.OJSError
	return errors.As(err, &ojsErr) && ojsErr.Code == core.ErrCodeNotFound
}

// cronOccurrenceJobID deterministically derives a UUIDv7 from the cron name and
// scheduled occurrence. The UUID timestamp is the occurrence's Unix
// millisecond value; the remaining 74 bits come from a stable SHA-256 hash.
func cronOccurrenceJobID(name string, occurrence time.Time) (string, error) {
	occurrenceMs := occurrence.UnixMilli()
	if occurrenceMs < 0 || uint64(occurrenceMs) >= 1<<48 {
		return "", fmt.Errorf("occurrence timestamp %d is outside UUIDv7 range", occurrenceMs)
	}

	digest := sha256.Sum256([]byte("ojs-cron\x00" + name + "\x00" + strconv.FormatInt(occurrenceMs, 10)))
	var id [16]byte
	ms := uint64(occurrenceMs)
	id[0] = byte(ms >> 40)
	id[1] = byte(ms >> 32)
	id[2] = byte(ms >> 24)
	id[3] = byte(ms >> 16)
	id[4] = byte(ms >> 8)
	id[5] = byte(ms)
	copy(id[6:], digest[:10])
	id[6] = (id[6] & 0x0f) | 0x70
	id[8] = (id[8] & 0x3f) | 0x80

	return fmt.Sprintf(
		"%x-%x-%x-%x-%x",
		id[0:4],
		id[4:6],
		id[6:8],
		id[8:10],
		id[10:16],
	), nil
}

// --- Helper functions ---

// buildJobErrorJSON serializes a JobError into the stored error JSON shape.
// Returns nil when jobErr is nil. On marshal failure it returns a minimal
// fallback payload so the failure is still recorded rather than lost.
func buildJobErrorJSON(job *core.Job, jobErr *core.JobError) []byte {
	if jobErr == nil {
		return nil
	}
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
	errJSON, marshalErr := json.Marshal(errObj)
	if marshalErr != nil {
		slog.Error("failed to marshal job error", "job_id", job.ID, "error", marshalErr)
		return []byte(`{"message":"marshal error"}`)
	}
	return errJSON
}

// resolveNonRetryable reports whether a failed job must not be retried, either
// because the error is explicitly marked non-retryable or because it matches one
// of the job's configured non-retryable error patterns.
func resolveNonRetryable(job *core.Job, jobErr *core.JobError) bool {
	if jobErr == nil {
		return false
	}
	if jobErr.Retryable != nil && !*jobErr.Retryable {
		return true
	}
	if job.Retry == nil {
		return false
	}
	errType := jobErr.Code
	if jobErr.Type != "" {
		errType = jobErr.Type
	}
	for _, pattern := range job.Retry.NonRetryableErrors {
		if matchesPattern(errType, pattern) || matchesPattern(jobErr.Message, pattern) {
			return true
		}
	}
	return false
}

// buildWorkflowStepJob constructs a Job for a single workflow step, applying the
// shared queue-default and retry-policy resolution used by chain creation, group
// creation, and chained-step enqueues. parentResults may be nil.
func buildWorkflowStepJob(step core.WorkflowJobRequest, workflowID string, stepIdx int, parentResults []json.RawMessage) *core.Job {
	job := &core.Job{
		Type:          step.Type,
		Args:          step.Args,
		Queue:         "default",
		WorkflowID:    workflowID,
		WorkflowStep:  stepIdx,
		ParentResults: parentResults,
	}
	applyEnqueueOptions(job, step.Options)
	return job
}

func applyEnqueueOptions(job *core.Job, options *core.EnqueueOptions) {
	if options == nil {
		return
	}
	if options.Queue != "" {
		job.Queue = options.Queue
	}
	job.Priority = options.Priority
	job.TimeoutMs = options.TimeoutMs
	if options.ScheduledAt != "" {
		job.ScheduledAt = options.ScheduledAt
	} else if options.DelayUntil != "" {
		job.ScheduledAt = options.DelayUntil
	}
	job.ExpiresAt = options.ExpiresAt
	if options.RetryPolicy != nil {
		job.Retry = options.RetryPolicy
	} else {
		job.Retry = options.Retry
	}
	if job.Retry != nil {
		job.MaxAttempts = &job.Retry.MaxAttempts
	}
	job.Unique = options.Unique
	job.Tags = append([]string(nil), options.Tags...)
	job.VisibilityTimeoutMs = options.VisibilityTimeoutMs
	job.Meta = append(json.RawMessage(nil), options.Metadata...)
	job.RateLimit = options.RateLimit
}

func maxAttemptsForJob(job *core.Job) int {
	if job.MaxAttempts != nil {
		return *job.MaxAttempts
	}
	return core.DefaultRetryPolicy().MaxAttempts
}

func prepareJobForPush(job *core.Job, now time.Time) (score float64, scheduled bool) {
	if job.ID == "" {
		job.ID = core.NewUUIDv7()
	}
	job.CreatedAt = core.FormatTime(now)
	job.Attempt = 0

	if job.ScheduledAt != "" {
		scheduledTime, err := time.Parse(time.RFC3339, job.ScheduledAt)
		if err == nil && scheduledTime.After(now) {
			scheduled = true
			score = float64(scheduledTime.UnixMilli())
			job.State = core.StateScheduled
		}
	}
	if !scheduled {
		job.State = core.StateAvailable
		score = computeScore(job.Priority, now)
	}
	job.EnqueuedAt = core.FormatTime(now)
	return score, scheduled
}

// pushJobIfAbsent persists a stable-ID job without ever resetting an existing
// job. It is used by durable schedulers whose marker may expire independently
// of the job itself.
func (b *KafkaBackend) pushJobIfAbsent(ctx context.Context, job *core.Job) (*core.Job, bool, error) {
	score, scheduled := prepareJobForPush(job, b.now())
	if b.pushJobFn != nil {
		created, err := b.pushJobFn(ctx, job)
		if err != nil {
			return nil, false, err
		}
		if created == nil || created.ID != job.ID {
			return nil, false, fmt.Errorf("stable push returned an unexpected job ID")
		}
		return created, true, nil
	}
	created, err := b.store.AtomicPushIfAbsent(ctx, job, score, scheduled)
	if err != nil {
		return nil, false, fmt.Errorf("atomic push-if-absent job: %w", err)
	}
	if !created {
		existing, getErr := b.store.GetJob(ctx, job.ID)
		if getErr != nil {
			return nil, false, fmt.Errorf("load existing stable job %q: %w", job.ID, getErr)
		}
		return existing, false, nil
	}
	if !scheduled && job.RateLimit != nil && job.RateLimit.MaxPerSecond > 0 {
		if err := b.store.SetRateLimit(ctx, job.Queue, job.RateLimit.MaxPerSecond); err != nil {
			return nil, true, fmt.Errorf("set rate limit for stable job %q: %w", job.ID, err)
		}
	}
	b.publishEnqueuedJob(ctx, job)
	return job, true, nil
}

func (b *KafkaBackend) publishEnqueuedJob(ctx context.Context, job *core.Job) {
	if b.producer == nil || b.producer.client == nil {
		return
	}
	b.producer.ProduceJobAsync(ctx, job)
	b.producer.EmitJobEnqueued(ctx, job)
}

func (b *KafkaBackend) emitJobCancelled(ctx context.Context, job *core.Job) {
	if b.producer == nil || job == nil {
		return
	}
	b.producer.EmitJobCancelled(ctx, job.ID, job.Queue, job.Type)
}

func (b *KafkaBackend) pushJob(ctx context.Context, job *core.Job) (*core.Job, error) {
	if b.pushJobFn != nil {
		return b.pushJobFn(ctx, job)
	}
	return b.Push(ctx, job)
}

func (b *KafkaBackend) now() time.Time {
	if b.nowFn != nil {
		return b.nowFn()
	}
	return time.Now()
}

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

// escapedGlobStar matches the escaped `*` produced by regexp.QuoteMeta,
// so glob wildcards can be converted to `.*` without recompiling per call.
var escapedGlobStar = regexp.MustCompile(`\\\*`)

func matchesPattern(s, pattern string) bool {
	// Convert glob-like pattern (with * wildcards) to safe regex.
	// All other regex metacharacters are escaped to prevent injection.
	escaped := regexp.QuoteMeta(pattern)
	// Restore * wildcards: QuoteMeta turns * into \*, convert back to .*
	safePattern := "^" + escapedGlobStar.ReplaceAllString(escaped, ".*") + "$"
	re, err := regexp.Compile(safePattern)
	if err != nil {
		return s == pattern
	}
	return re.MatchString(s)
}
