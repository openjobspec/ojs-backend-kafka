package state_test

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

// advanceInput builds a WorkflowAdvanceInput with distinct preassigned effect
// job IDs derived from the advancing job so tests exercise the outbox recording.
func advanceInput(workflowID, jobID string, step int, result json.RawMessage, failed bool) state.WorkflowAdvanceInput {
	return state.WorkflowAdvanceInput{
		WorkflowID:      workflowID,
		JobID:           jobID,
		Step:            step,
		Result:          result,
		Failed:          failed,
		CompletedAt:     core.NowFormatted(),
		NextChainJobID:  "next-" + jobID,
		OnCompleteJobID: "cb-complete-" + jobID,
		OnSuccessJobID:  "cb-success-" + jobID,
		OnFailureJobID:  "cb-failure-" + jobID,
	}
}

func TestAtomicRequeue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	job := &core.Job{
		ID:        "job-requeue",
		Type:      "task",
		State:     core.StateActive,
		Queue:     "q-requeue",
		StartedAt: core.FormatTime(time.Now()),
		WorkerID:  "worker-1",
	}
	if err := store.SaveJob(ctx, job); err != nil {
		t.Fatalf("save job: %v", err)
	}
	if err := store.AddToActive(ctx, job.Queue, job.ID); err != nil {
		t.Fatalf("add active: %v", err)
	}
	if err := store.SetVisibility(ctx, job.ID, core.FormatTime(time.Now().Add(time.Minute))); err != nil {
		t.Fatalf("set visibility: %v", err)
	}

	enqueuedAt := core.NowFormatted()
	if err := store.AtomicRequeue(ctx, job.ID, job.Queue, enqueuedAt, 42); err != nil {
		t.Fatalf("atomic requeue: %v", err)
	}

	updated, err := store.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if updated.State != core.StateAvailable || updated.StartedAt != "" || updated.WorkerID != "" || updated.EnqueuedAt != enqueuedAt {
		t.Fatalf("updated job = %+v", updated)
	}
	active, err := store.GetActiveJobs(ctx, job.Queue)
	if err != nil {
		t.Fatalf("get active: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("active jobs = %v", active)
	}
	if _, err := store.GetVisibility(ctx, job.ID); err == nil {
		t.Fatal("visibility was not deleted")
	}
	available, err := store.PopFromAvailable(ctx, job.Queue)
	if err != nil || available != job.ID {
		t.Fatalf("available pop = %q, %v", available, err)
	}
}

func TestAtomicAdvanceWorkflowConcurrentTerminalOwnership(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	const jobs = 32
	ctx := context.Background()
	if err := store.SaveWorkflow(ctx, "wf-concurrent", map[string]any{
		"id":        "wf-concurrent",
		"type":      "batch",
		"state":     "running",
		"total":     strconv.Itoa(jobs),
		"completed": "0",
		"failed":    "0",
	}); err != nil {
		t.Fatalf("save workflow: %v", err)
	}

	start := make(chan struct{})
	results := make(chan *state.WorkflowAdvanceResult, jobs)
	errs := make(chan error, jobs)
	var wg sync.WaitGroup
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			result, err := store.AtomicAdvanceWorkflow(
				ctx,
				advanceInput("wf-concurrent", "job-"+strconv.Itoa(index), index, json.RawMessage(`{"ok":true}`), false),
			)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("atomic advance: %v", err)
	}
	applied := 0
	terminalOwners := 0
	for result := range results {
		if result.Applied {
			applied++
		}
		if result.TerminalOwner {
			terminalOwners++
		}
	}
	if applied != jobs || terminalOwners != 1 {
		t.Fatalf("applied=%d terminal owners=%d, want %d/1", applied, terminalOwners, jobs)
	}

	workflow, err := store.GetWorkflow(ctx, "wf-concurrent")
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if workflow["state"] != "completed" || workflow["completed"] != strconv.Itoa(jobs) || workflow["failed"] != "0" {
		t.Fatalf("workflow = %v", workflow)
	}
}

func TestAtomicAdvanceWorkflowFailureAndIdempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.SaveWorkflow(ctx, "wf-failed", map[string]any{
		"id":        "wf-failed",
		"type":      "group",
		"state":     "running",
		"total":     "2",
		"completed": "0",
		"failed":    "0",
	}); err != nil {
		t.Fatalf("save workflow: %v", err)
	}

	first, err := store.AtomicAdvanceWorkflow(ctx, advanceInput("wf-failed", "job-a", 0, nil, false))
	if err != nil {
		t.Fatalf("advance first: %v", err)
	}
	duplicate, err := store.AtomicAdvanceWorkflow(ctx, advanceInput("wf-failed", "job-a", 0, nil, false))
	if err != nil {
		t.Fatalf("advance duplicate: %v", err)
	}
	last, err := store.AtomicAdvanceWorkflow(ctx, advanceInput("wf-failed", "job-b", 1, nil, true))
	if err != nil {
		t.Fatalf("advance failed job: %v", err)
	}
	if !first.Applied || duplicate.Applied || !last.TerminalOwner || last.State != "failed" {
		t.Fatalf("results: first=%+v duplicate=%+v last=%+v", first, duplicate, last)
	}

	workflow, err := store.GetWorkflow(ctx, "wf-failed")
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if workflow["completed"] != "1" || workflow["failed"] != "1" || workflow["state"] != "failed" {
		t.Fatalf("workflow = %v", workflow)
	}
}

func TestAtomicAdvanceWorkflowChainProgression(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.SaveWorkflow(ctx, "wf-chain", map[string]any{
		"id":        "wf-chain",
		"type":      "chain",
		"state":     "running",
		"total":     "2",
		"completed": "0",
		"failed":    "0",
	}); err != nil {
		t.Fatalf("save workflow: %v", err)
	}

	first, err := store.AtomicAdvanceWorkflow(
		ctx,
		advanceInput("wf-chain", "job-first", 0, json.RawMessage(`{"value":42}`), false),
	)
	if err != nil {
		t.Fatalf("advance first step: %v", err)
	}
	if !first.Applied || !first.EnqueueNext || first.NextStep != 1 || first.TerminalOwner || first.State != "running" {
		t.Fatalf("first result = %+v", first)
	}
	duplicate, err := store.AtomicAdvanceWorkflow(ctx, advanceInput("wf-chain", "job-first", 0, nil, false))
	if err != nil {
		t.Fatalf("duplicate first step: %v", err)
	}
	if duplicate.Applied || duplicate.EnqueueNext {
		t.Fatalf("duplicate result = %+v", duplicate)
	}
	last, err := store.AtomicAdvanceWorkflow(ctx, advanceInput("wf-chain", "job-second", 1, nil, false))
	if err != nil {
		t.Fatalf("advance last step: %v", err)
	}
	if !last.TerminalOwner || last.State != "completed" || last.Completed != 2 {
		t.Fatalf("last result = %+v", last)
	}
	results, err := store.GetWorkflowResults(ctx, "wf-chain")
	if err != nil {
		t.Fatalf("get results: %v", err)
	}
	if results["0"] != `{"value":42}` {
		t.Fatalf("stored result = %q", results["0"])
	}
}

func TestAtomicAdvanceWorkflowInvalidCountersDoNotClaimJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.SaveWorkflow(ctx, "wf-invalid", map[string]any{
		"id":        "wf-invalid",
		"type":      "batch",
		"state":     "running",
		"total":     "invalid",
		"completed": "0",
		"failed":    "0",
	}); err != nil {
		t.Fatalf("save workflow: %v", err)
	}
	if _, err := store.AtomicAdvanceWorkflow(ctx, advanceInput("wf-invalid", "job-a", 0, nil, false)); err == nil {
		t.Fatal("expected invalid counter error")
	}
	workflow, err := store.GetWorkflow(ctx, "wf-invalid")
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if workflow["completed"] != "0" || workflow["state"] != "running" {
		t.Fatalf("workflow mutated after failure: %v", workflow)
	}

	if err := store.UpdateWorkflow(ctx, "wf-invalid", map[string]any{"total": "1"}); err != nil {
		t.Fatalf("fix workflow: %v", err)
	}
	result, err := store.AtomicAdvanceWorkflow(ctx, advanceInput("wf-invalid", "job-a", 0, nil, false))
	if err != nil {
		t.Fatalf("retry advancement: %v", err)
	}
	if !result.Applied || !result.TerminalOwner {
		t.Fatalf("retry result = %+v", result)
	}
}

func TestCronOccurrenceClaims(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UnixMilli()
	const occurrence = int64(1700000000000)

	claim, err := store.ClaimCronOccurrence(ctx, "nightly", occurrence, "owner-1", "job-1", now, 1000)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claim.Status != state.CronOccurrenceAcquired || claim.JobID != "job-1" {
		t.Fatalf("claim = %+v", claim)
	}
	busy, err := store.ClaimCronOccurrence(ctx, "nightly", occurrence, "owner-2", "job-2", now+1, 1000)
	if err != nil {
		t.Fatalf("busy claim: %v", err)
	}
	if busy.Status != state.CronOccurrenceBusy || busy.JobID != "job-1" {
		t.Fatalf("busy claim = %+v", busy)
	}
	if err := store.ReleaseCronOccurrence(ctx, "nightly", occurrence, "owner-1", "job-1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	retry, err := store.ClaimCronOccurrence(ctx, "nightly", occurrence, "owner-2", "job-2", now+2, 1000)
	if err != nil {
		t.Fatalf("retry claim: %v", err)
	}
	if retry.Status != state.CronOccurrenceAcquired || retry.JobID != "job-2" {
		t.Fatalf("retry claim = %+v", retry)
	}
	if err := store.CompleteCronOccurrence(ctx, "nightly", occurrence, "owner-2", "job-2"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	fired, err := store.ClaimCronOccurrence(ctx, "nightly", occurrence, "owner-3", "job-3", now+5000, 1000)
	if err != nil {
		t.Fatalf("fired claim: %v", err)
	}
	if fired.Status != state.CronOccurrenceFired || fired.JobID != "job-2" {
		t.Fatalf("fired claim = %+v", fired)
	}
}

func TestCronOccurrenceExpiredClaimReconcilesSuccessfulPush(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()

	ctx := context.Background()
	const occurrence = int64(1700000060000)
	if _, err := store.ClaimCronOccurrence(ctx, "hourly", occurrence, "owner-1", "job-existing", 1000, 100); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.SaveJob(ctx, &core.Job{
		ID:    "job-existing",
		Type:  "task",
		State: core.StateAvailable,
		Queue: "default",
	}); err != nil {
		t.Fatalf("save claimed job: %v", err)
	}

	reconciled, err := store.ClaimCronOccurrence(ctx, "hourly", occurrence, "owner-2", "job-new", 1200, 100)
	if err != nil {
		t.Fatalf("reconcile claim: %v", err)
	}
	if reconciled.Status != state.CronOccurrenceFired || reconciled.JobID != "job-existing" {
		t.Fatalf("reconciled claim = %+v", reconciled)
	}
}
