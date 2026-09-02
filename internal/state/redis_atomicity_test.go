package state_test

import (
	"context"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

func setupRedisWithRawClient(t *testing.T) (*state.RedisStore, *goredis.Client, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := rediscontainer.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	connectionString, err := container.ConnectionString(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("redis connection string: %v", err)
	}
	store, err := state.NewRedisStore(connectionString)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("new redis store: %v", err)
	}
	options, err := goredis.ParseURL(connectionString)
	if err != nil {
		_ = store.Close()
		_ = container.Terminate(ctx)
		t.Fatalf("parse redis URL: %v", err)
	}
	raw := goredis.NewClient(options)

	return store, raw, func() {
		_ = raw.Close()
		_ = store.Close()
		_ = container.Terminate(ctx)
	}
}

func TestAtomicRequeueWrongTypeLeavesActiveStateIntact(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	job := &core.Job{
		ID:        "job-atomic-failure",
		Type:      "task",
		State:     core.StateActive,
		Queue:     "q1",
		StartedAt: core.NowFormatted(),
		WorkerID:  "worker-1",
	}
	if err := store.SaveJob(ctx, job); err != nil {
		t.Fatalf("save job: %v", err)
	}
	if err := raw.HSet(ctx, "ojs:job:"+job.ID, "worker_id", job.WorkerID).Err(); err != nil {
		t.Fatalf("seed worker id: %v", err)
	}
	if err := store.AddToActive(ctx, job.Queue, job.ID); err != nil {
		t.Fatalf("add active: %v", err)
	}
	visibility := core.FormatTime(time.Now().Add(time.Minute))
	if err := store.SetVisibility(ctx, job.ID, visibility); err != nil {
		t.Fatalf("set visibility: %v", err)
	}
	if err := raw.Set(ctx, "ojs:queue:q1:available", "wrong-type", 0).Err(); err != nil {
		t.Fatalf("seed wrong type: %v", err)
	}

	if err := store.AtomicRequeue(ctx, job.ID, job.Queue, core.NowFormatted(), 1); err == nil {
		t.Fatal("expected WRONGTYPE-safe preflight failure")
	}

	unchanged, err := store.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if unchanged.State != core.StateActive || unchanged.StartedAt == "" {
		t.Fatalf("job was partially mutated: %+v", unchanged)
	}
	workerID, err := raw.HGet(ctx, "ojs:job:"+job.ID, "worker_id").Result()
	if err != nil || workerID != "worker-1" {
		t.Fatalf("worker id was partially mutated: %q, %v", workerID, err)
	}
	active, err := store.GetActiveJobs(ctx, job.Queue)
	if err != nil {
		t.Fatalf("get active: %v", err)
	}
	if len(active) != 1 || active[0] != job.ID {
		t.Fatalf("active set was partially mutated: %v", active)
	}
	gotVisibility, err := store.GetVisibility(ctx, job.ID)
	if err != nil || gotVisibility != visibility {
		t.Fatalf("visibility was partially mutated: %q, %v", gotVisibility, err)
	}
}

func TestAtomicCancelJobWrongTypeLeavesJobAndIndexesIntact(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	job := &core.Job{
		ID:    "job-cancel-wrongtype",
		Type:  "task",
		State: core.StateAvailable,
		Queue: "cancel-q",
	}
	if err := store.SaveJob(ctx, job); err != nil {
		t.Fatalf("save job: %v", err)
	}
	if err := store.AddToAvailable(ctx, job.Queue, job.ID, 1); err != nil {
		t.Fatalf("add available: %v", err)
	}
	if err := raw.Set(ctx, "ojs:queue:cancel-q:active", "wrong-type", 0).Err(); err != nil {
		t.Fatalf("poison active index: %v", err)
	}

	if _, err := store.AtomicCancelJob(ctx, job.ID, core.NowFormatted()); err == nil {
		t.Fatal("expected cancellation preflight failure")
	}
	persisted, err := store.GetJob(ctx, job.ID)
	if err != nil || persisted.State != core.StateAvailable || persisted.CancelledAt != "" {
		t.Fatalf("job partially cancelled: %+v, %v", persisted, err)
	}
	members, err := raw.ZRange(ctx, "ojs:queue:cancel-q:available", 0, -1).Result()
	if err != nil || len(members) != 1 || members[0] != job.ID {
		t.Fatalf("available index partially removed: %v, %v", members, err)
	}
}

func TestAtomicPushIfAbsentNeverResetsExistingJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	job := &core.Job{
		ID:    "stable-job",
		Type:  "task",
		State: core.StateAvailable,
		Queue: "stable-q",
	}
	created, err := store.AtomicPushIfAbsent(ctx, job, 1, false)
	if err != nil || !created {
		t.Fatalf("first create = %v, %v", created, err)
	}
	if err := store.RemoveFromAvailable(ctx, job.Queue, job.ID); err != nil {
		t.Fatalf("remove available: %v", err)
	}
	if err := store.UpdateJob(ctx, job.ID, map[string]any{
		"state":        core.StateCompleted,
		"completed_at": core.NowFormatted(),
	}); err != nil {
		t.Fatalf("complete job: %v", err)
	}

	replay := *job
	replay.State = core.StateAvailable
	created, err = store.AtomicPushIfAbsent(ctx, &replay, 2, false)
	if err != nil || created {
		t.Fatalf("replay create = %v, %v; want existing", created, err)
	}
	persisted, err := store.GetJob(ctx, job.ID)
	if err != nil || persisted.State != core.StateCompleted || persisted.CompletedAt == "" {
		t.Fatalf("existing job was reset: %+v, %v", persisted, err)
	}
	if members, err := raw.ZRange(ctx, "ojs:queue:stable-q:available", 0, -1).Result(); err != nil || len(members) != 0 {
		t.Fatalf("terminal job was re-indexed: %v, %v", members, err)
	}
}

func TestCancelledJobsCannotBePromotedOrRequeued(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()
	ctx := context.Background()

	for _, sourceState := range []string{core.StateScheduled, core.StateRetryable} {
		t.Run(sourceState, func(t *testing.T) {
			job := &core.Job{
				ID:    "job-cancelled-" + sourceState,
				Type:  "task",
				State: sourceState,
				Queue: "fenced-" + sourceState,
			}
			if err := store.SaveJob(ctx, job); err != nil {
				t.Fatalf("save job: %v", err)
			}
			if sourceState == core.StateScheduled {
				if err := store.AddToScheduled(ctx, job.ID, time.Now().UnixMilli()); err != nil {
					t.Fatalf("add scheduled: %v", err)
				}
			} else {
				if err := store.AddToRetry(ctx, job.ID, time.Now().UnixMilli()); err != nil {
					t.Fatalf("add retry: %v", err)
				}
			}
			if result, err := store.AtomicCancelJob(ctx, job.ID, core.NowFormatted()); err != nil || !result.Cancelled {
				t.Fatalf("cancel = %+v, %v", result, err)
			}
			if promoted, err := store.AtomicPromote(ctx, job.ID, job.Queue, sourceState, core.NowFormatted(), 1); err != nil || promoted {
				t.Fatalf("post-cancel promote = %v, %v", promoted, err)
			}
			persisted, err := store.GetJob(ctx, job.ID)
			if err != nil || persisted.State != core.StateCancelled {
				t.Fatalf("job was resurrected: %+v, %v", persisted, err)
			}
			if count, err := raw.ZCard(ctx, "ojs:queue:"+job.Queue+":available").Result(); err != nil || count != 0 {
				t.Fatalf("cancelled job re-entered available: count=%d, %v", count, err)
			}
		})
	}

	active := &core.Job{
		ID:    "job-cancelled-active",
		Type:  "task",
		State: core.StateActive,
		Queue: "fenced-active",
	}
	if err := store.SaveJob(ctx, active); err != nil {
		t.Fatalf("save active job: %v", err)
	}
	if err := store.AddToActive(ctx, active.Queue, active.ID); err != nil {
		t.Fatalf("add active: %v", err)
	}
	if result, err := store.AtomicCancelJob(ctx, active.ID, core.NowFormatted()); err != nil || !result.Cancelled {
		t.Fatalf("cancel active = %+v, %v", result, err)
	}
	if err := store.AtomicRequeue(ctx, active.ID, active.Queue, core.NowFormatted(), 1); err != nil {
		t.Fatalf("post-cancel requeue: %v", err)
	}
	persisted, err := store.GetJob(ctx, active.ID)
	if err != nil || persisted.State != core.StateCancelled {
		t.Fatalf("active job was resurrected: %+v, %v", persisted, err)
	}
}

func TestAtomicAdvanceWorkflowWrongTypeLeavesCountersIntact(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.SaveWorkflow(ctx, "wf-wrong-type", map[string]any{
		"id":        "wf-wrong-type",
		"type":      "batch",
		"state":     "running",
		"total":     "1",
		"completed": "0",
		"failed":    "0",
	}); err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if err := raw.Set(ctx, "ojs:workflow:wf-wrong-type:advanced", "wrong-type", 0).Err(); err != nil {
		t.Fatalf("seed wrong type: %v", err)
	}

	if _, err := store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
		WorkflowID:  "wf-wrong-type",
		JobID:       "job-1",
		Step:        0,
		CompletedAt: core.NowFormatted(),
	}); err == nil {
		t.Fatal("expected WRONGTYPE-safe preflight failure")
	}
	workflow, err := store.GetWorkflow(ctx, "wf-wrong-type")
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if workflow["state"] != "running" || workflow["completed"] != "0" || workflow["failed"] != "0" {
		t.Fatalf("workflow was partially mutated: %v", workflow)
	}
}

func TestAtomicAdvanceWorkflowPendingIndexWrongTypeHasNoPartialMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	const workflowID = "wf-pending-wrongtype"
	if err := store.SaveWorkflow(ctx, workflowID, map[string]any{
		"id": workflowID, "type": "chain", "state": "running",
		"total": "2", "completed": "0", "failed": "0",
	}); err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if err := raw.Set(ctx, "ojs:workflows:pending", "wrong-type", 0).Err(); err != nil {
		t.Fatalf("poison pending index: %v", err)
	}

	if _, err := store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
		WorkflowID:      workflowID,
		JobID:           "job-0",
		Step:            0,
		Result:          []byte(`{"ok":true}`),
		CompletedAt:     core.NowFormatted(),
		NextChainJobID:  "stable-job",
		OnCompleteJobID: "complete",
		OnSuccessJobID:  "success",
		OnFailureJobID:  "failure",
	}); err == nil {
		t.Fatal("expected pending-index type failure")
	}

	workflow, err := store.GetWorkflow(ctx, workflowID)
	if err != nil || workflow["state"] != "running" || workflow["completed"] != "0" || workflow["failed"] != "0" {
		t.Fatalf("workflow partially advanced: %v, %v", workflow, err)
	}
	for _, key := range []string{
		"ojs:workflow:" + workflowID + ":advanced",
		"ojs:workflow:" + workflowID + ":results",
		"ojs:workflow:" + workflowID + ":effects",
	} {
		if exists, err := raw.Exists(ctx, key).Result(); err != nil || exists != 0 {
			t.Fatalf("partial key %q exists=%d, %v", key, exists, err)
		}
	}
}

func TestCompleteWorkflowEffectPendingIndexWrongTypeDoesNotAppendOrComplete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	const workflowID = "wf-complete-wrongtype"
	if err := store.SaveWorkflow(ctx, workflowID, map[string]any{
		"id": workflowID, "type": "chain", "state": "running",
		"total": "2", "completed": "0", "failed": "0",
	}); err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if _, err := store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
		WorkflowID: workflowID, JobID: "job-0", Step: 0, CompletedAt: core.NowFormatted(),
		NextChainJobID: "stable-job", OnCompleteJobID: "complete", OnSuccessJobID: "success", OnFailureJobID: "failure",
	}); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if claim, err := store.ClaimWorkflowEffect(ctx, workflowID, "chain:1", "owner", time.Now().UnixMilli(), 60_000); err != nil || claim.Status != state.WorkflowEffectClaimed {
		t.Fatalf("claim = %+v, %v", claim, err)
	}
	if err := raw.Del(ctx, "ojs:workflows:pending").Err(); err != nil {
		t.Fatalf("delete pending index: %v", err)
	}
	if err := raw.Set(ctx, "ojs:workflows:pending", "wrong-type", 0).Err(); err != nil {
		t.Fatalf("poison pending index: %v", err)
	}

	if completed, err := store.CompleteWorkflowEffect(ctx, workflowID, "chain:1", "owner", "stable-job", true); err == nil || completed {
		t.Fatalf("complete = %v, %v; want atomic type failure", completed, err)
	}
	jobs, err := store.GetWorkflowJobs(ctx, workflowID)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("job list partially appended: %v, %v", jobs, err)
	}
	effects, err := store.GetWorkflowEffects(ctx, workflowID)
	if err != nil || !strings.HasPrefix(effects["chain:1"], "active|") {
		t.Fatalf("effect partially completed: %v, %v", effects, err)
	}
}

func TestCancelWorkflowPendingIndexWrongTypeDoesNotRevokeOrTransition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	const workflowID = "wf-cancel-wrongtype"
	if err := store.SaveWorkflow(ctx, workflowID, map[string]any{
		"id": workflowID, "type": "chain", "state": "running",
		"total": "2", "completed": "0", "failed": "0",
	}); err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if _, err := store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
		WorkflowID: workflowID, JobID: "job-0", Step: 0, CompletedAt: core.NowFormatted(),
		NextChainJobID: "stable-job", OnCompleteJobID: "complete", OnSuccessJobID: "success", OnFailureJobID: "failure",
	}); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := raw.Del(ctx, "ojs:workflows:pending").Err(); err != nil {
		t.Fatalf("delete pending index: %v", err)
	}
	if err := raw.Set(ctx, "ojs:workflows:pending", "wrong-type", 0).Err(); err != nil {
		t.Fatalf("poison pending index: %v", err)
	}

	if _, err := store.AtomicCancelWorkflow(ctx, workflowID, core.NowFormatted()); err == nil {
		t.Fatal("expected cancellation type failure")
	}
	workflow, err := store.GetWorkflow(ctx, workflowID)
	if err != nil || workflow["state"] != "running" || workflow["completed"] != "1" {
		t.Fatalf("workflow partially cancelled: %v, %v", workflow, err)
	}
	effects, err := store.GetWorkflowEffects(ctx, workflowID)
	if err != nil || effects["chain:1"] != "pending|stable-job" {
		t.Fatalf("effects partially revoked: %v, %v", effects, err)
	}
}

func TestCronOccurrenceMarkersHaveBoundedRetentionAndCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, raw, cleanup := setupRedisWithRawClient(t)
	defer cleanup()

	ctx := context.Background()
	const leaseMs = int64(60_000)
	nowMs := time.Now().UnixMilli()

	assertBoundedTTL := func(key string) {
		t.Helper()
		ttl, err := raw.PTTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("PTTL %q: %v", key, err)
		}
		if ttl <= time.Hour || ttl > 24*time.Hour {
			t.Fatalf("marker TTL = %s, want >1h and <=24h", ttl)
		}
	}

	const pendingOccurrence = int64(1700000000000)
	pendingKey := "ojs:cron:pending:occurrence:1700000000000"
	if _, err := store.ClaimCronOccurrence(ctx, "pending", pendingOccurrence, "owner-1", "job-1", nowMs, leaseMs); err != nil {
		t.Fatalf("claim pending marker: %v", err)
	}
	assertBoundedTTL(pendingKey)
	if err := store.ReleaseCronOccurrence(ctx, "pending", pendingOccurrence, "owner-1", "job-1"); err != nil {
		t.Fatalf("release pending marker: %v", err)
	}
	if exists, err := raw.Exists(ctx, pendingKey).Result(); err != nil || exists != 0 {
		t.Fatalf("pending marker exists=%d, err=%v; want deleted", exists, err)
	}

	const firedOccurrence = int64(1700000060000)
	firedKey := "ojs:cron:fired:occurrence:1700000060000"
	if _, err := store.ClaimCronOccurrence(ctx, "fired", firedOccurrence, "owner-2", "job-2", nowMs, leaseMs); err != nil {
		t.Fatalf("claim fired marker: %v", err)
	}
	if err := store.CompleteCronOccurrence(ctx, "fired", firedOccurrence, "owner-2", "job-2"); err != nil {
		t.Fatalf("complete fired marker: %v", err)
	}
	assertBoundedTTL(firedKey)
	if err := store.ReleaseCronOccurrence(ctx, "fired", firedOccurrence, "reconciler", "job-2"); err != nil {
		t.Fatalf("release fired marker: %v", err)
	}
	if exists, err := raw.Exists(ctx, firedKey).Result(); err != nil || exists != 0 {
		t.Fatalf("fired marker exists=%d, err=%v; want deleted", exists, err)
	}

	const orphanOccurrence = int64(1700000120000)
	orphanKey := "ojs:cron:orphan:occurrence:1700000120000"
	if err := store.SaveCron(ctx, "orphan", []byte(`{"name":"orphan"}`)); err != nil {
		t.Fatalf("save orphan cron: %v", err)
	}
	if _, err := store.ClaimCronOccurrence(ctx, "orphan", orphanOccurrence, "owner-3", "job-3", nowMs, leaseMs); err != nil {
		t.Fatalf("claim orphan marker: %v", err)
	}
	if err := store.DeleteCron(ctx, "orphan"); err != nil {
		t.Fatalf("delete orphan cron: %v", err)
	}
	assertBoundedTTL(orphanKey)
}
