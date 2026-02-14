package state

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

const keyPrefix = "ojs:"

// RedisStore implements Store using Redis.
type RedisStore struct {
	client *redis.Client
}

// NewRedisStore creates a new Redis-backed state store.
func NewRedisStore(redisURL string) (*RedisStore, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parsing redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connecting to redis: %w", err)
	}

	return &RedisStore{client: client}, nil
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}

func (s *RedisStore) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// --- Job state operations ---

func (s *RedisStore) SaveJob(ctx context.Context, job *core.Job) error {
	h := jobToHash(job)
	return s.client.HSet(ctx, jobKey(job.ID), h).Err()
}

func (s *RedisStore) GetJob(ctx context.Context, jobID string) (*core.Job, error) {
	data, err := s.client.HGetAll(ctx, jobKey(jobID)).Result()
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	if len(data) == 0 {
		return nil, core.NewNotFoundError("Job", jobID)
	}
	return hashToJob(data), nil
}

func (s *RedisStore) UpdateJob(ctx context.Context, jobID string, updates map[string]any) error {
	return s.client.HSet(ctx, jobKey(jobID), updates).Err()
}

func (s *RedisStore) DeleteJobFields(ctx context.Context, jobID string, fields ...string) error {
	return s.client.HDel(ctx, jobKey(jobID), fields...).Err()
}

// --- Queue operations ---

func (s *RedisStore) AddToAvailable(ctx context.Context, queue string, jobID string, score float64) error {
	return s.client.ZAdd(ctx, queueAvailableKey(queue), redis.Z{Score: score, Member: jobID}).Err()
}

func (s *RedisStore) PopFromAvailable(ctx context.Context, queue string) (string, error) {
	results, err := s.client.ZPopMin(ctx, queueAvailableKey(queue), 1).Result()
	if err != nil || len(results) == 0 {
		return "", err
	}
	return results[0].Member.(string), nil
}

func (s *RedisStore) RemoveFromAvailable(ctx context.Context, queue string, jobID string) error {
	return s.client.ZRem(ctx, queueAvailableKey(queue), jobID).Err()
}

func (s *RedisStore) AddToActive(ctx context.Context, queue string, jobID string) error {
	return s.client.SAdd(ctx, queueActiveKey(queue), jobID).Err()
}

func (s *RedisStore) RemoveFromActive(ctx context.Context, queue string, jobID string) error {
	return s.client.SRem(ctx, queueActiveKey(queue), jobID).Err()
}

func (s *RedisStore) GetActiveJobs(ctx context.Context, queue string) ([]string, error) {
	return s.client.SMembers(ctx, queueActiveKey(queue)).Result()
}

func (s *RedisStore) RegisterQueue(ctx context.Context, queue string) error {
	return s.client.SAdd(ctx, queuesKey(), queue).Err()
}

func (s *RedisStore) GetAllQueues(ctx context.Context) ([]string, error) {
	return s.client.SMembers(ctx, queuesKey()).Result()
}

func (s *RedisStore) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	exists, err := s.client.Exists(ctx, queuePausedKey(queue)).Result()
	return exists > 0, err
}

func (s *RedisStore) PauseQueue(ctx context.Context, queue string) error {
	return s.client.Set(ctx, queuePausedKey(queue), "1", 0).Err()
}

func (s *RedisStore) ResumeQueue(ctx context.Context, queue string) error {
	return s.client.Del(ctx, queuePausedKey(queue)).Err()
}

func (s *RedisStore) GetAvailableCount(ctx context.Context, queue string) (int64, error) {
	return s.client.ZCard(ctx, queueAvailableKey(queue)).Result()
}

func (s *RedisStore) GetActiveCount(ctx context.Context, queue string) (int64, error) {
	return s.client.SCard(ctx, queueActiveKey(queue)).Result()
}

func (s *RedisStore) IncrCompletedCount(ctx context.Context, queue string) error {
	return s.client.Incr(ctx, queueCompletedKey(queue)).Err()
}

func (s *RedisStore) GetCompletedCount(ctx context.Context, queue string) (int64, error) {
	return s.client.Get(ctx, queueCompletedKey(queue)).Int64()
}

// --- Rate limiting ---

func (s *RedisStore) SetRateLimit(ctx context.Context, queue string, maxPerSecond int) error {
	return s.client.Set(ctx, queueRateLimitKey(queue), strconv.Itoa(maxPerSecond), 0).Err()
}

func (s *RedisStore) CheckRateLimit(ctx context.Context, queue string) (bool, error) {
	rateLimitStr, err := s.client.Get(ctx, queueRateLimitKey(queue)).Result()
	if err != nil || rateLimitStr == "" {
		return true, nil // no rate limit
	}

	maxPerSec, _ := strconv.Atoi(rateLimitStr)
	if maxPerSec <= 0 {
		return true, nil
	}

	windowMs := int64(1000 / maxPerSec)
	lastFetchStr, _ := s.client.Get(ctx, queueRateLimitLastKey(queue)).Result()
	if lastFetchStr != "" {
		lastFetchMs, _ := strconv.ParseInt(lastFetchStr, 10, 64)
		nowMs := time.Now().UnixMilli()
		if nowMs-lastFetchMs < windowMs {
			return false, nil // rate limited
		}
	}
	return true, nil
}

func (s *RedisStore) RecordFetch(ctx context.Context, queue string) error {
	return s.client.Set(ctx, queueRateLimitLastKey(queue), strconv.FormatInt(time.Now().UnixMilli(), 10), 0).Err()
}

// --- Scheduled jobs ---

func (s *RedisStore) AddToScheduled(ctx context.Context, jobID string, scheduledAtMs int64) error {
	return s.client.ZAdd(ctx, scheduledKey(), redis.Z{Score: float64(scheduledAtMs), Member: jobID}).Err()
}

func (s *RedisStore) RemoveFromScheduled(ctx context.Context, jobID string) error {
	return s.client.ZRem(ctx, scheduledKey(), jobID).Err()
}

func (s *RedisStore) GetDueScheduled(ctx context.Context, nowMs int64) ([]string, error) {
	return s.client.ZRangeByScore(ctx, scheduledKey(), &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(nowMs, 10),
	}).Result()
}

// --- Retry jobs ---

func (s *RedisStore) AddToRetry(ctx context.Context, jobID string, retryAtMs int64) error {
	return s.client.ZAdd(ctx, retryKey(), redis.Z{Score: float64(retryAtMs), Member: jobID}).Err()
}

func (s *RedisStore) RemoveFromRetry(ctx context.Context, jobID string) error {
	return s.client.ZRem(ctx, retryKey(), jobID).Err()
}

func (s *RedisStore) GetDueRetries(ctx context.Context, nowMs int64) ([]string, error) {
	return s.client.ZRangeByScore(ctx, retryKey(), &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(nowMs, 10),
	}).Result()
}

// --- Dead letter queue ---

func (s *RedisStore) AddToDead(ctx context.Context, jobID string, nowMs int64) error {
	return s.client.ZAdd(ctx, deadKey(), redis.Z{Score: float64(nowMs), Member: jobID}).Err()
}

func (s *RedisStore) RemoveFromDead(ctx context.Context, jobID string) error {
	removed, err := s.client.ZRem(ctx, deadKey(), jobID).Result()
	if err != nil {
		return err
	}
	if removed == 0 {
		return core.NewNotFoundError("Dead letter job", jobID)
	}
	return nil
}

func (s *RedisStore) GetDeadJobs(ctx context.Context, offset, limit int) ([]string, int64, error) {
	total, err := s.client.ZCard(ctx, deadKey()).Result()
	if err != nil {
		return nil, 0, err
	}
	ids, err := s.client.ZRevRange(ctx, deadKey(), int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		return nil, 0, err
	}
	return ids, total, nil
}

func (s *RedisStore) IsInDead(ctx context.Context, jobID string) (bool, error) {
	_, err := s.client.ZScore(ctx, deadKey(), jobID).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// --- Visibility timeout ---

func (s *RedisStore) SetVisibility(ctx context.Context, jobID string, deadline string) error {
	return s.client.Set(ctx, visibilityKey(jobID), deadline, 0).Err()
}

func (s *RedisStore) GetVisibility(ctx context.Context, jobID string) (string, error) {
	return s.client.Get(ctx, visibilityKey(jobID)).Result()
}

func (s *RedisStore) DeleteVisibility(ctx context.Context, jobID string) error {
	return s.client.Del(ctx, visibilityKey(jobID)).Err()
}

// --- Unique jobs ---

func (s *RedisStore) GetUniqueJobID(ctx context.Context, fingerprint string) (string, error) {
	return s.client.Get(ctx, uniqueKey(fingerprint)).Result()
}

func (s *RedisStore) SetUniqueJobID(ctx context.Context, fingerprint string, jobID string, ttlMs int64) error {
	ttl := time.Duration(ttlMs) * time.Millisecond
	return s.client.Set(ctx, uniqueKey(fingerprint), jobID, ttl).Err()
}

// --- Workers ---

func (s *RedisStore) RegisterWorker(ctx context.Context, workerID string, data map[string]any) error {
	s.client.SAdd(ctx, workersKey(), workerID)
	return s.client.HSet(ctx, workerKey(workerID), data).Err()
}

func (s *RedisStore) GetWorkerData(ctx context.Context, workerID string, field string) (string, error) {
	return s.client.HGet(ctx, workerKey(workerID), field).Result()
}

func (s *RedisStore) SetWorkerData(ctx context.Context, workerID string, field string, value string) error {
	return s.client.HSet(ctx, workerKey(workerID), field, value).Err()
}

// --- Cron ---

func (s *RedisStore) SaveCron(ctx context.Context, name string, data []byte) error {
	pipe := s.client.Pipeline()
	pipe.Set(ctx, cronKey(name), string(data), 0)
	pipe.SAdd(ctx, cronNamesKey(), name)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisStore) GetCron(ctx context.Context, name string) ([]byte, error) {
	data, err := s.client.Get(ctx, cronKey(name)).Result()
	if err == redis.Nil {
		return nil, core.NewNotFoundError("Cron job", name)
	}
	if err != nil {
		return nil, err
	}
	return []byte(data), nil
}

func (s *RedisStore) DeleteCron(ctx context.Context, name string) error {
	pipe := s.client.Pipeline()
	pipe.Del(ctx, cronKey(name))
	pipe.SRem(ctx, cronNamesKey(), name)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisStore) GetAllCronNames(ctx context.Context) ([]string, error) {
	return s.client.SMembers(ctx, cronNamesKey()).Result()
}

func (s *RedisStore) AcquireCronLock(ctx context.Context, key string, ttlMs int64) (bool, error) {
	ttl := time.Duration(ttlMs) * time.Millisecond
	return s.client.SetNX(ctx, key, "1", ttl).Result()
}

func (s *RedisStore) SetCronInstance(ctx context.Context, name string, jobID string) error {
	return s.client.Set(ctx, cronInstanceKey(name), jobID, 0).Err()
}

func (s *RedisStore) GetCronInstance(ctx context.Context, name string) (string, error) {
	val, err := s.client.Get(ctx, cronInstanceKey(name)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

// --- Workflows ---

func (s *RedisStore) SaveWorkflow(ctx context.Context, id string, data map[string]any) error {
	return s.client.HSet(ctx, workflowKey(id), data).Err()
}

func (s *RedisStore) GetWorkflow(ctx context.Context, id string) (map[string]string, error) {
	data, err := s.client.HGetAll(ctx, workflowKey(id)).Result()
	if err != nil || len(data) == 0 {
		return nil, core.NewNotFoundError("Workflow", id)
	}
	return data, nil
}

func (s *RedisStore) UpdateWorkflow(ctx context.Context, id string, updates map[string]any) error {
	return s.client.HSet(ctx, workflowKey(id), updates).Err()
}

func (s *RedisStore) AppendWorkflowJob(ctx context.Context, workflowID string, jobID string) error {
	return s.client.RPush(ctx, workflowKey(workflowID)+":jobs", jobID).Err()
}

func (s *RedisStore) GetWorkflowJobs(ctx context.Context, workflowID string) ([]string, error) {
	return s.client.LRange(ctx, workflowKey(workflowID)+":jobs", 0, -1).Result()
}

func (s *RedisStore) SetWorkflowResult(ctx context.Context, workflowID string, step int, result json.RawMessage) error {
	return s.client.HSet(ctx, workflowKey(workflowID)+":results", strconv.Itoa(step), string(result)).Err()
}

func (s *RedisStore) GetWorkflowResults(ctx context.Context, workflowID string) (map[string]string, error) {
	return s.client.HGetAll(ctx, workflowKey(workflowID)+":results").Result()
}

// --- Atomic Lua script operations ---

// AtomicPush atomically saves a job, adds to available/scheduled, and registers the queue.
func (s *RedisStore) AtomicPush(ctx context.Context, job *core.Job, score float64, scheduled bool) error {
	h := jobToHash(job)

	// Build alternating field/value args
	var args []any
	for k, v := range h {
		args = append(args, k, v)
	}
	args = append(args, "__score__", score, job.ID, job.Queue)

	sortedSetKey := queueAvailableKey(job.Queue)
	if scheduled {
		sortedSetKey = scheduledKey()
	}

	return pushJobScript.Run(ctx, s.client,
		[]string{jobKey(job.ID), sortedSetKey, queuesKey()},
		args...,
	).Err()
}

// AtomicFetch atomically pops from available, adds to active, and sets visibility.
// Returns the job ID or empty string if queue is empty.
func (s *RedisStore) AtomicFetch(ctx context.Context, queue string, visDeadline string) (string, error) {
	result, err := fetchJobScript.Run(ctx, s.client,
		[]string{queueAvailableKey(queue), queueActiveKey(queue)},
		visDeadline,
	).Text()
	if err != nil {
		return "", err
	}
	return result, nil
}

// AtomicAck atomically completes a job: updates state, removes from active, clears visibility, increments completed.
func (s *RedisStore) AtomicAck(ctx context.Context, jobID string, queue string, completedAt string, result string) error {
	return ackJobScript.Run(ctx, s.client,
		[]string{
			jobKey(jobID),
			queueActiveKey(queue),
			visibilityKey(jobID),
			queueCompletedKey(queue),
		},
		jobID, completedAt, result,
	).Err()
}

// AtomicNackDiscard atomically discards a job: updates state, removes from active, clears visibility, optionally adds to DLQ.
func (s *RedisStore) AtomicNackDiscard(ctx context.Context, jobID string, queue string, completedAt string, errJSON string, histJSON string, attempt string, addToDead bool, nowMs int64) error {
	deadFlag := "0"
	if addToDead {
		deadFlag = "1"
	}
	return nackDiscardScript.Run(ctx, s.client,
		[]string{
			jobKey(jobID),
			queueActiveKey(queue),
			visibilityKey(jobID),
			deadKey(),
		},
		jobID, completedAt, errJSON, histJSON, attempt, deadFlag, nowMs,
	).Err()
}

// AtomicNackRetry atomically retries a job: updates state, removes from active, clears visibility, adds to retry set.
func (s *RedisStore) AtomicNackRetry(ctx context.Context, jobID string, queue string, errJSON string, histJSON string, attempt string, retryDelayMs string, retryAtMs int64) error {
	return nackRetryScript.Run(ctx, s.client,
		[]string{
			jobKey(jobID),
			queueActiveKey(queue),
			visibilityKey(jobID),
			retryKey(),
		},
		jobID, errJSON, histJSON, attempt, retryDelayMs, retryAtMs,
	).Err()
}

// --- Redis key builders ---

func jobKey(id string) string                  { return fmt.Sprintf("%sjob:%s", keyPrefix, id) }
func queueAvailableKey(name string) string     { return fmt.Sprintf("%squeue:%s:available", keyPrefix, name) }
func queueActiveKey(name string) string        { return fmt.Sprintf("%squeue:%s:active", keyPrefix, name) }
func queuePausedKey(name string) string        { return fmt.Sprintf("%squeue:%s:paused", keyPrefix, name) }
func queuesKey() string                        { return keyPrefix + "queues" }
func scheduledKey() string                     { return keyPrefix + "scheduled" }
func retryKey() string                         { return keyPrefix + "retry" }
func deadKey() string                          { return keyPrefix + "dead" }
func uniqueKey(fingerprint string) string      { return fmt.Sprintf("%sunique:%s", keyPrefix, fingerprint) }
func cronKey(name string) string               { return fmt.Sprintf("%scron:%s", keyPrefix, name) }
func cronNamesKey() string                     { return keyPrefix + "cron:names" }
func workflowKey(id string) string             { return fmt.Sprintf("%sworkflow:%s", keyPrefix, id) }
func workerKey(id string) string               { return fmt.Sprintf("%sworker:%s", keyPrefix, id) }
func workersKey() string                       { return keyPrefix + "workers" }
func visibilityKey(jobID string) string        { return fmt.Sprintf("%svisibility:%s", keyPrefix, jobID) }
func cronInstanceKey(name string) string       { return fmt.Sprintf("%scron:%s:instance", keyPrefix, name) }
func queueCompletedKey(name string) string     { return fmt.Sprintf("%squeue:%s:completed", keyPrefix, name) }
func queueRateLimitKey(name string) string     { return fmt.Sprintf("%squeue:%s:ratelimit", keyPrefix, name) }
func queueRateLimitLastKey(name string) string { return fmt.Sprintf("%squeue:%s:ratelimit:last", keyPrefix, name) }

// --- Job serialization ---

func jobToHash(job *core.Job) map[string]any {
	h := map[string]any{
		"id":      job.ID,
		"type":    job.Type,
		"state":   job.State,
		"queue":   job.Queue,
		"attempt": strconv.Itoa(job.Attempt),
	}

	if job.Args != nil {
		h["args"] = string(job.Args)
	}
	if job.Meta != nil && len(job.Meta) > 0 {
		h["meta"] = string(job.Meta)
	}
	if job.Priority != nil {
		h["priority"] = strconv.Itoa(*job.Priority)
	}
	if job.MaxAttempts != nil {
		h["max_attempts"] = strconv.Itoa(*job.MaxAttempts)
	}
	if job.TimeoutMs != nil {
		h["timeout_ms"] = strconv.Itoa(*job.TimeoutMs)
	}
	if job.CreatedAt != "" {
		h["created_at"] = job.CreatedAt
	}
	if job.EnqueuedAt != "" {
		h["enqueued_at"] = job.EnqueuedAt
	}
	if job.StartedAt != "" {
		h["started_at"] = job.StartedAt
	}
	if job.CompletedAt != "" {
		h["completed_at"] = job.CompletedAt
	}
	if job.CancelledAt != "" {
		h["cancelled_at"] = job.CancelledAt
	}
	if job.ScheduledAt != "" {
		h["scheduled_at"] = job.ScheduledAt
	}
	if job.Result != nil && len(job.Result) > 0 {
		h["result"] = string(job.Result)
	}
	if job.Error != nil && len(job.Error) > 0 {
		h["error"] = string(job.Error)
	}
	if len(job.Tags) > 0 {
		tagsJSON, _ := json.Marshal(job.Tags)
		h["tags"] = string(tagsJSON)
	}
	if job.Retry != nil {
		retryJSON, _ := json.Marshal(job.Retry)
		h["retry"] = string(retryJSON)
	}
	if job.Unique != nil {
		uniqueJSON, _ := json.Marshal(job.Unique)
		h["unique"] = string(uniqueJSON)
	}
	if job.ExpiresAt != "" {
		h["expires_at"] = job.ExpiresAt
	}
	if job.VisibilityTimeoutMs != nil {
		h["visibility_timeout_ms"] = strconv.Itoa(*job.VisibilityTimeoutMs)
	}
	if job.WorkflowID != "" {
		h["workflow_id"] = job.WorkflowID
	}
	if job.WorkflowStep >= 0 && job.WorkflowID != "" {
		h["workflow_step"] = strconv.Itoa(job.WorkflowStep)
	}
	if len(job.ParentResults) > 0 {
		prJSON, _ := json.Marshal(job.ParentResults)
		h["parent_results"] = string(prJSON)
	}

	// Store unknown fields
	for k, v := range job.UnknownFields {
		h["x:"+k] = string(v)
	}

	return h
}

func hashToJob(data map[string]string) *core.Job {
	if len(data) == 0 {
		return nil
	}

	job := &core.Job{
		ID:    data["id"],
		Type:  data["type"],
		State: data["state"],
		Queue: data["queue"],
	}

	if v, ok := data["args"]; ok {
		job.Args = json.RawMessage(v)
	}
	if v, ok := data["meta"]; ok && v != "" {
		job.Meta = json.RawMessage(v)
	}
	if v, ok := data["priority"]; ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			job.Priority = &n
		}
	}
	if v, ok := data["attempt"]; ok {
		job.Attempt, _ = strconv.Atoi(v)
	}
	if v, ok := data["max_attempts"]; ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			job.MaxAttempts = &n
		}
	}
	if v, ok := data["timeout_ms"]; ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			job.TimeoutMs = &n
		}
	}
	if v, ok := data["created_at"]; ok {
		job.CreatedAt = v
	}
	if v, ok := data["enqueued_at"]; ok {
		job.EnqueuedAt = v
	}
	if v, ok := data["started_at"]; ok && v != "" {
		job.StartedAt = v
	}
	if v, ok := data["completed_at"]; ok && v != "" {
		job.CompletedAt = v
	}
	if v, ok := data["cancelled_at"]; ok && v != "" {
		job.CancelledAt = v
	}
	if v, ok := data["scheduled_at"]; ok && v != "" {
		job.ScheduledAt = v
	}
	if v, ok := data["result"]; ok && v != "" {
		job.Result = json.RawMessage(v)
	}
	if v, ok := data["error"]; ok && v != "" {
		job.Error = json.RawMessage(v)
	}
	if v, ok := data["tags"]; ok && v != "" {
		var tags []string
		json.Unmarshal([]byte(v), &tags)
		job.Tags = tags
	}
	if v, ok := data["retry"]; ok && v != "" {
		var retry core.RetryPolicy
		json.Unmarshal([]byte(v), &retry)
		job.Retry = &retry
	}
	if v, ok := data["unique"]; ok && v != "" {
		var unique core.UniquePolicy
		json.Unmarshal([]byte(v), &unique)
		job.Unique = &unique
	}
	if v, ok := data["expires_at"]; ok && v != "" {
		job.ExpiresAt = v
	}
	if v, ok := data["error_history"]; ok && v != "" {
		var errors []json.RawMessage
		if json.Unmarshal([]byte(v), &errors) == nil {
			job.Errors = errors
		}
	}
	if v, ok := data["retry_delay_ms"]; ok && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			job.RetryDelayMs = &n
		}
	}
	if v, ok := data["visibility_timeout_ms"]; ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			job.VisibilityTimeoutMs = &n
		}
	}
	if v, ok := data["workflow_id"]; ok && v != "" {
		job.WorkflowID = v
	}
	if v, ok := data["workflow_step"]; ok && v != "" {
		job.WorkflowStep, _ = strconv.Atoi(v)
	}
	if v, ok := data["parent_results"]; ok && v != "" {
		var pr []json.RawMessage
		if json.Unmarshal([]byte(v), &pr) == nil {
			job.ParentResults = pr
		}
	}

	// Restore unknown fields
	job.UnknownFields = make(map[string]json.RawMessage)
	for k, v := range data {
		if len(k) > 2 && k[:2] == "x:" {
			job.UnknownFields[k[2:]] = json.RawMessage(v)
		}
	}
	if len(job.UnknownFields) == 0 {
		job.UnknownFields = nil
	}

	return job
}
