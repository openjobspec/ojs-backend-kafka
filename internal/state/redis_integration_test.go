package state_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

func setupRedis(t *testing.T) (*state.RedisStore, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("failed to get connection string: %v", err)
	}

	store, err := state.NewRedisStore(connStr)
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("failed to create redis store: %v", err)
	}

	cleanup := func() {
		store.Close()
		container.Terminate(ctx)
	}

	return store, cleanup
}

func TestPing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	err := store.Ping(context.Background())
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func TestSaveAndGetJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	job := &core.Job{
		ID:        "job-1",
		Type:      "email.send",
		State:     core.StateAvailable,
		Queue:     "default",
		Args:      json.RawMessage(`["hello"]`),
		CreatedAt: core.FormatTime(time.Now()),
	}

	if err := store.SaveJob(ctx, job); err != nil {
		t.Fatalf("save job: %v", err)
	}

	got, err := store.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if got.ID != "job-1" {
		t.Errorf("got ID %q, want %q", got.ID, "job-1")
	}
	if got.Type != "email.send" {
		t.Errorf("got Type %q, want %q", got.Type, "email.send")
	}
	if got.State != core.StateAvailable {
		t.Errorf("got State %q, want %q", got.State, core.StateAvailable)
	}
	if string(got.Args) != `["hello"]` {
		t.Errorf("got Args %q, want %q", string(got.Args), `["hello"]`)
	}
}

func TestGetJobNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	_, err := store.GetJob(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent job")
	}
}

func TestUpdateJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	job := &core.Job{ID: "job-2", Type: "test", State: core.StateAvailable, Queue: "q1"}
	store.SaveJob(ctx, job)

	store.UpdateJob(ctx, "job-2", map[string]any{"state": core.StateActive, "started_at": "2024-01-01T00:00:00Z"})

	got, _ := store.GetJob(ctx, "job-2")
	if got.State != core.StateActive {
		t.Errorf("got State %q, want %q", got.State, core.StateActive)
	}
	if got.StartedAt != "2024-01-01T00:00:00Z" {
		t.Errorf("got StartedAt %q, want %q", got.StartedAt, "2024-01-01T00:00:00Z")
	}
}

func TestDeleteJobFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	job := &core.Job{ID: "job-3", Type: "test", State: core.StateActive, Queue: "q1", StartedAt: "2024-01-01T00:00:00Z"}
	store.SaveJob(ctx, job)

	store.DeleteJobFields(ctx, "job-3", "started_at")

	got, _ := store.GetJob(ctx, "job-3")
	if got.StartedAt != "" {
		t.Errorf("expected empty StartedAt, got %q", got.StartedAt)
	}
}

func TestQueueAvailableOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Add jobs with different scores (priority)
	store.AddToAvailable(ctx, "q1", "job-low", 200.0)
	store.AddToAvailable(ctx, "q1", "job-high", 100.0)
	store.AddToAvailable(ctx, "q1", "job-mid", 150.0)

	count, _ := store.GetAvailableCount(ctx, "q1")
	if count != 3 {
		t.Errorf("expected 3 available, got %d", count)
	}

	// Pop should return lowest score first (highest priority)
	id, err := store.PopFromAvailable(ctx, "q1")
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if id != "job-high" {
		t.Errorf("expected job-high, got %q", id)
	}

	// Remove specific job
	store.RemoveFromAvailable(ctx, "q1", "job-mid")
	count, _ = store.GetAvailableCount(ctx, "q1")
	if count != 1 {
		t.Errorf("expected 1 available after removal, got %d", count)
	}
}

func TestQueueActiveOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	store.AddToActive(ctx, "q1", "job-a")
	store.AddToActive(ctx, "q1", "job-b")

	count, _ := store.GetActiveCount(ctx, "q1")
	if count != 2 {
		t.Errorf("expected 2 active, got %d", count)
	}

	jobs, _ := store.GetActiveJobs(ctx, "q1")
	if len(jobs) != 2 {
		t.Errorf("expected 2 active jobs, got %d", len(jobs))
	}

	store.RemoveFromActive(ctx, "q1", "job-a")
	count, _ = store.GetActiveCount(ctx, "q1")
	if count != 1 {
		t.Errorf("expected 1 active after removal, got %d", count)
	}
}

func TestQueuePauseResume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	paused, _ := store.IsQueuePaused(ctx, "q1")
	if paused {
		t.Error("queue should not be paused initially")
	}

	store.PauseQueue(ctx, "q1")
	paused, _ = store.IsQueuePaused(ctx, "q1")
	if !paused {
		t.Error("queue should be paused")
	}

	store.ResumeQueue(ctx, "q1")
	paused, _ = store.IsQueuePaused(ctx, "q1")
	if paused {
		t.Error("queue should not be paused after resume")
	}
}

func TestRegisterAndListQueues(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	store.RegisterQueue(ctx, "alpha")
	store.RegisterQueue(ctx, "beta")
	store.RegisterQueue(ctx, "alpha") // duplicate

	queues, _ := store.GetAllQueues(ctx)
	if len(queues) != 2 {
		t.Errorf("expected 2 queues, got %d", len(queues))
	}
}

func TestCompletedCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	store.IncrCompletedCount(ctx, "q1")
	store.IncrCompletedCount(ctx, "q1")
	store.IncrCompletedCount(ctx, "q1")

	count, _ := store.GetCompletedCount(ctx, "q1")
	if count != 3 {
		t.Errorf("expected 3 completed, got %d", count)
	}
}

func TestScheduledJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UnixMilli()

	store.AddToScheduled(ctx, "job-future", now+60000)
	store.AddToScheduled(ctx, "job-past", now-1000)
	store.AddToScheduled(ctx, "job-now", now)

	due, _ := store.GetDueScheduled(ctx, now)
	if len(due) != 2 {
		t.Errorf("expected 2 due scheduled, got %d: %v", len(due), due)
	}

	store.RemoveFromScheduled(ctx, "job-past")
	due, _ = store.GetDueScheduled(ctx, now)
	if len(due) != 1 {
		t.Errorf("expected 1 due after removal, got %d", len(due))
	}
}

func TestRetryJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UnixMilli()

	store.AddToRetry(ctx, "job-r1", now-500)
	store.AddToRetry(ctx, "job-r2", now+60000)

	due, _ := store.GetDueRetries(ctx, now)
	if len(due) != 1 {
		t.Errorf("expected 1 due retry, got %d", len(due))
	}
	if due[0] != "job-r1" {
		t.Errorf("expected job-r1, got %q", due[0])
	}

	store.RemoveFromRetry(ctx, "job-r1")
	due, _ = store.GetDueRetries(ctx, now)
	if len(due) != 0 {
		t.Errorf("expected 0 due after removal, got %d", len(due))
	}
}

func TestDeadLetterQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UnixMilli()

	store.AddToDead(ctx, "dead-1", now)
	store.AddToDead(ctx, "dead-2", now+1)

	inDead, _ := store.IsInDead(ctx, "dead-1")
	if !inDead {
		t.Error("dead-1 should be in dead letter")
	}

	inDead, _ = store.IsInDead(ctx, "not-dead")
	if inDead {
		t.Error("not-dead should not be in dead letter")
	}

	ids, total, _ := store.GetDeadJobs(ctx, 0, 10)
	if total != 2 {
		t.Errorf("expected total 2, got %d", total)
	}
	if len(ids) != 2 {
		t.Errorf("expected 2 dead jobs, got %d", len(ids))
	}

	store.RemoveFromDead(ctx, "dead-1")
	_, total, _ = store.GetDeadJobs(ctx, 0, 10)
	if total != 1 {
		t.Errorf("expected total 1 after removal, got %d", total)
	}

	err := store.RemoveFromDead(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error removing nonexistent dead letter job")
	}
}

func TestVisibility(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	deadline := "2024-06-01T12:00:00Z"
	store.SetVisibility(ctx, "job-v1", deadline)

	got, err := store.GetVisibility(ctx, "job-v1")
	if err != nil {
		t.Fatalf("get visibility: %v", err)
	}
	if got != deadline {
		t.Errorf("got %q, want %q", got, deadline)
	}

	store.DeleteVisibility(ctx, "job-v1")
	_, err = store.GetVisibility(ctx, "job-v1")
	if err == nil {
		t.Error("expected error after deleting visibility")
	}
}

func TestUniqueJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	store.SetUniqueJobID(ctx, "fp-abc", "job-u1", 60000)

	got, err := store.GetUniqueJobID(ctx, "fp-abc")
	if err != nil {
		t.Fatalf("get unique: %v", err)
	}
	if got != "job-u1" {
		t.Errorf("got %q, want %q", got, "job-u1")
	}

	_, err = store.GetUniqueJobID(ctx, "fp-nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent fingerprint")
	}
}

func TestWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	store.RegisterWorker(ctx, "worker-1", map[string]any{
		"last_heartbeat": "2024-01-01T00:00:00Z",
		"active_jobs":    5,
	})

	val, err := store.GetWorkerData(ctx, "worker-1", "last_heartbeat")
	if err != nil {
		t.Fatalf("get worker data: %v", err)
	}
	if val != "2024-01-01T00:00:00Z" {
		t.Errorf("got %q, want %q", val, "2024-01-01T00:00:00Z")
	}

	store.SetWorkerData(ctx, "worker-1", "directive", "shutdown")
	val, _ = store.GetWorkerData(ctx, "worker-1", "directive")
	if val != "shutdown" {
		t.Errorf("got %q, want %q", val, "shutdown")
	}
}

func TestCron(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	data := []byte(`{"name":"nightly","schedule":"0 0 * * *"}`)
	store.SaveCron(ctx, "nightly", data)

	got, err := store.GetCron(ctx, "nightly")
	if err != nil {
		t.Fatalf("get cron: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("cron data mismatch")
	}

	names, _ := store.GetAllCronNames(ctx)
	if len(names) != 1 || names[0] != "nightly" {
		t.Errorf("expected [nightly], got %v", names)
	}

	store.DeleteCron(ctx, "nightly")
	_, err = store.GetCron(ctx, "nightly")
	if err == nil {
		t.Error("expected error after deleting cron")
	}
}

func TestCronLockAndInstance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	acquired, _ := store.AcquireCronLock(ctx, "lock-1", 5000)
	if !acquired {
		t.Error("expected to acquire lock")
	}

	acquired, _ = store.AcquireCronLock(ctx, "lock-1", 5000)
	if acquired {
		t.Error("should not acquire lock twice")
	}

	store.SetCronInstance(ctx, "nightly", "job-c1")
	val, _ := store.GetCronInstance(ctx, "nightly")
	if val != "job-c1" {
		t.Errorf("got %q, want %q", val, "job-c1")
	}

	val, _ = store.GetCronInstance(ctx, "nonexistent")
	if val != "" {
		t.Errorf("expected empty for nonexistent cron instance, got %q", val)
	}
}

func TestWorkflows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	store.SaveWorkflow(ctx, "wf-1", map[string]any{
		"id":    "wf-1",
		"type":  "chain",
		"state": "running",
		"total": "3",
	})

	data, err := store.GetWorkflow(ctx, "wf-1")
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if data["state"] != "running" {
		t.Errorf("got state %q, want %q", data["state"], "running")
	}

	store.UpdateWorkflow(ctx, "wf-1", map[string]any{"state": "completed"})
	data, _ = store.GetWorkflow(ctx, "wf-1")
	if data["state"] != "completed" {
		t.Errorf("got state %q after update, want %q", data["state"], "completed")
	}

	// Workflow jobs
	store.AppendWorkflowJob(ctx, "wf-1", "job-w1")
	store.AppendWorkflowJob(ctx, "wf-1", "job-w2")
	jobs, _ := store.GetWorkflowJobs(ctx, "wf-1")
	if len(jobs) != 2 {
		t.Errorf("expected 2 workflow jobs, got %d", len(jobs))
	}

	// Workflow results
	store.SetWorkflowResult(ctx, "wf-1", 0, json.RawMessage(`{"ok":true}`))
	results, _ := store.GetWorkflowResults(ctx, "wf-1")
	if results["0"] != `{"ok":true}` {
		t.Errorf("got result %q, want %q", results["0"], `{"ok":true}`)
	}

	// Not found
	_, err = store.GetWorkflow(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent workflow")
	}
}

func TestAtomicPush(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	job := &core.Job{
		ID:        "job-ap1",
		Type:      "email.send",
		State:     core.StateAvailable,
		Queue:     "default",
		Args:      json.RawMessage(`[1,2,3]`),
		CreatedAt: core.FormatTime(time.Now()),
	}

	err := store.AtomicPush(ctx, job, 100.0, false)
	if err != nil {
		t.Fatalf("atomic push: %v", err)
	}

	// Job should exist
	got, err := store.GetJob(ctx, "job-ap1")
	if err != nil {
		t.Fatalf("get job after push: %v", err)
	}
	if got.Type != "email.send" {
		t.Errorf("got Type %q, want %q", got.Type, "email.send")
	}

	// Should be in available queue
	count, _ := store.GetAvailableCount(ctx, "default")
	if count != 1 {
		t.Errorf("expected 1 available, got %d", count)
	}

	// Queue should be registered
	queues, _ := store.GetAllQueues(ctx)
	found := false
	for _, q := range queues {
		if q == "default" {
			found = true
		}
	}
	if !found {
		t.Error("expected 'default' queue to be registered")
	}
}

func TestAtomicPushScheduled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	futureMs := float64(time.Now().Add(time.Hour).UnixMilli())

	job := &core.Job{
		ID:        "job-sched",
		Type:      "report.gen",
		State:     core.StateScheduled,
		Queue:     "reports",
		CreatedAt: core.FormatTime(time.Now()),
	}

	err := store.AtomicPush(ctx, job, futureMs, true)
	if err != nil {
		t.Fatalf("atomic push scheduled: %v", err)
	}

	// Should not be in available
	count, _ := store.GetAvailableCount(ctx, "reports")
	if count != 0 {
		t.Errorf("expected 0 available for scheduled job, got %d", count)
	}

	// Should be in scheduled set
	due, _ := store.GetDueScheduled(ctx, time.Now().Add(2*time.Hour).UnixMilli())
	found := false
	for _, id := range due {
		if id == "job-sched" {
			found = true
		}
	}
	if !found {
		t.Error("expected job-sched in scheduled set")
	}
}

func TestAtomicFetch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Push two jobs
	store.AtomicPush(ctx, &core.Job{
		ID: "job-f1", Type: "t1", State: core.StateAvailable, Queue: "q1",
		CreatedAt: core.FormatTime(time.Now()),
	}, 200.0, false)
	store.AtomicPush(ctx, &core.Job{
		ID: "job-f2", Type: "t2", State: core.StateAvailable, Queue: "q1",
		CreatedAt: core.FormatTime(time.Now()),
	}, 100.0, false)

	// Fetch should return lowest score first
	deadline := core.FormatTime(time.Now().Add(30 * time.Second))
	jobID, err := store.AtomicFetch(ctx, "q1", deadline)
	if err != nil {
		t.Fatalf("atomic fetch: %v", err)
	}
	if jobID != "job-f2" {
		t.Errorf("expected job-f2 (lower score), got %q", jobID)
	}

	// Job should be in active set
	active, _ := store.GetActiveJobs(ctx, "q1")
	found := false
	for _, id := range active {
		if id == "job-f2" {
			found = true
		}
	}
	if !found {
		t.Error("expected job-f2 in active set")
	}

	// Visibility should be set
	vis, err := store.GetVisibility(ctx, "job-f2")
	if err != nil {
		t.Fatalf("get visibility: %v", err)
	}
	if vis != deadline {
		t.Errorf("got visibility %q, want %q", vis, deadline)
	}

	// Fetch from empty queue
	store.AtomicFetch(ctx, "q1", deadline) // pop job-f1
	jobID, _ = store.AtomicFetch(ctx, "q1", deadline)
	if jobID != "" {
		t.Errorf("expected empty string from empty queue, got %q", jobID)
	}
}

func TestAtomicAck(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Setup: push and fetch a job
	store.AtomicPush(ctx, &core.Job{
		ID: "job-ack", Type: "t1", State: core.StateAvailable, Queue: "q1",
		CreatedAt: core.FormatTime(time.Now()),
	}, 100.0, false)

	deadline := core.FormatTime(time.Now().Add(30 * time.Second))
	store.AtomicFetch(ctx, "q1", deadline)

	// Ack
	now := core.FormatTime(time.Now())
	err := store.AtomicAck(ctx, "job-ack", "q1", now, `{"result":"ok"}`)
	if err != nil {
		t.Fatalf("atomic ack: %v", err)
	}

	// Job state should be completed
	job, _ := store.GetJob(ctx, "job-ack")
	if job.State != "completed" {
		t.Errorf("got state %q, want %q", job.State, "completed")
	}
	if string(job.Result) != `{"result":"ok"}` {
		t.Errorf("got result %q, want %q", string(job.Result), `{"result":"ok"}`)
	}

	// Should not be in active
	active, _ := store.GetActiveJobs(ctx, "q1")
	if len(active) != 0 {
		t.Errorf("expected 0 active after ack, got %d", len(active))
	}

	// Visibility should be cleared
	_, err = store.GetVisibility(ctx, "job-ack")
	if err == nil {
		t.Error("expected visibility to be cleared after ack")
	}

	// Completed count should be incremented
	count, _ := store.GetCompletedCount(ctx, "q1")
	if count != 1 {
		t.Errorf("expected completed count 1, got %d", count)
	}
}

func TestAtomicNackDiscard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Setup: push and fetch
	store.AtomicPush(ctx, &core.Job{
		ID: "job-nd", Type: "t1", State: core.StateAvailable, Queue: "q1",
		CreatedAt: core.FormatTime(time.Now()),
	}, 100.0, false)
	deadline := core.FormatTime(time.Now().Add(30 * time.Second))
	store.AtomicFetch(ctx, "q1", deadline)

	now := core.FormatTime(time.Now())
	errJSON := `{"message":"fatal error"}`
	histJSON := `[{"message":"fatal error"}]`

	// Discard with dead letter
	err := store.AtomicNackDiscard(ctx, "job-nd", "q1", now, errJSON, histJSON, "1", true, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("atomic nack discard: %v", err)
	}

	// Job state should be discarded
	job, _ := store.GetJob(ctx, "job-nd")
	if job.State != "discarded" {
		t.Errorf("got state %q, want %q", job.State, "discarded")
	}

	// Should be in dead letter
	inDead, _ := store.IsInDead(ctx, "job-nd")
	if !inDead {
		t.Error("expected job in dead letter queue")
	}

	// Should not be in active
	active, _ := store.GetActiveJobs(ctx, "q1")
	if len(active) != 0 {
		t.Errorf("expected 0 active after nack discard, got %d", len(active))
	}
}

func TestAtomicNackDiscardWithoutDead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	store.AtomicPush(ctx, &core.Job{
		ID: "job-nd2", Type: "t1", State: core.StateAvailable, Queue: "q1",
		CreatedAt: core.FormatTime(time.Now()),
	}, 100.0, false)
	deadline := core.FormatTime(time.Now().Add(30 * time.Second))
	store.AtomicFetch(ctx, "q1", deadline)

	now := core.FormatTime(time.Now())
	err := store.AtomicNackDiscard(ctx, "job-nd2", "q1", now, "", "[]", "1", false, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("atomic nack discard without dead: %v", err)
	}

	inDead, _ := store.IsInDead(ctx, "job-nd2")
	if inDead {
		t.Error("expected job NOT in dead letter queue")
	}
}

func TestAtomicNackRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	store.AtomicPush(ctx, &core.Job{
		ID: "job-nr", Type: "t1", State: core.StateAvailable, Queue: "q1",
		CreatedAt: core.FormatTime(time.Now()),
	}, 100.0, false)
	deadline := core.FormatTime(time.Now().Add(30 * time.Second))
	store.AtomicFetch(ctx, "q1", deadline)

	errJSON := `{"message":"transient"}`
	histJSON := `[{"message":"transient"}]`
	retryAt := time.Now().Add(5 * time.Second).UnixMilli()

	err := store.AtomicNackRetry(ctx, "job-nr", "q1", errJSON, histJSON, "1", "5000", retryAt)
	if err != nil {
		t.Fatalf("atomic nack retry: %v", err)
	}

	// Job state should be retryable
	job, _ := store.GetJob(ctx, "job-nr")
	if job.State != "retryable" {
		t.Errorf("got state %q, want %q", job.State, "retryable")
	}

	// Should be in retry set
	due, _ := store.GetDueRetries(ctx, retryAt+1000)
	found := false
	for _, id := range due {
		if id == "job-nr" {
			found = true
		}
	}
	if !found {
		t.Error("expected job-nr in retry set")
	}

	// Should not be in active
	active, _ := store.GetActiveJobs(ctx, "q1")
	if len(active) != 0 {
		t.Errorf("expected 0 active after nack retry, got %d", len(active))
	}
}

func TestRateLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()

	// No rate limit set — should be allowed
	allowed, _ := store.CheckRateLimit(ctx, "q1")
	if !allowed {
		t.Error("expected allowed when no rate limit set")
	}

	// Set rate limit
	store.SetRateLimit(ctx, "q1", 1)

	// Record a fetch
	store.RecordFetch(ctx, "q1")

	// Immediately check — should be rate limited
	allowed, _ = store.CheckRateLimit(ctx, "q1")
	if allowed {
		t.Error("expected rate limited after immediate re-check")
	}
}
