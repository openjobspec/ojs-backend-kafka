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

func seedChainWorkflow(t *testing.T, store *state.RedisStore, id string, total int) {
	t.Helper()
	if err := store.SaveWorkflow(context.Background(), id, map[string]any{
		"id": id, "type": "chain", "state": "running",
		"total": strconv.Itoa(total), "completed": "0", "failed": "0",
	}); err != nil {
		t.Fatalf("save workflow: %v", err)
	}
}

// TestWorkflowAdvanceRecordsChainEffect verifies the atomic advance durably
// records the next chain step effect with its preassigned stable job ID and
// marks the workflow as having pending effects.
func TestWorkflowAdvanceRecordsChainEffect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()
	ctx := context.Background()
	seedChainWorkflow(t, store, "wf-eff", 2)

	advance, err := store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
		WorkflowID: "wf-eff", JobID: "job-0", Step: 0,
		Result: json.RawMessage(`{"v":1}`), CompletedAt: core.NowFormatted(),
		NextChainJobID: "chain-1", OnCompleteJobID: "c", OnSuccessJobID: "s", OnFailureJobID: "f",
	})
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if !advance.Applied || !advance.EnqueueNext || advance.NextStep != 1 || !advance.HasPendingEffects {
		t.Fatalf("advance = %+v", advance)
	}

	effects, err := store.GetWorkflowEffects(ctx, "wf-eff")
	if err != nil {
		t.Fatalf("get effects: %v", err)
	}
	if effects["chain:1"] != "pending|chain-1" {
		t.Fatalf("effects = %v, want chain:1 pending with stable ID", effects)
	}
	pending, err := store.GetWorkflowsWithPendingEffects(ctx)
	if err != nil || len(pending) != 1 || pending[0] != "wf-eff" {
		t.Fatalf("pending workflows = %v, err=%v", pending, err)
	}
}

func TestAtomicCancelWorkflowRevokesPendingAndLeasedEffects(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, leased := range []bool{false, true} {
		name := "pending"
		if leased {
			name = "leased"
		}
		t.Run(name, func(t *testing.T) {
			store, cleanup := setupRedis(t)
			defer cleanup()
			ctx := context.Background()
			workflowID := "wf-cancel-" + name
			seedChainWorkflow(t, store, workflowID, 2)
			if _, err := store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
				WorkflowID: workflowID, JobID: "job-0", Step: 0, CompletedAt: core.NowFormatted(),
				NextChainJobID: "stable-cancel-job", OnCompleteJobID: "c", OnSuccessJobID: "s", OnFailureJobID: "f",
			}); err != nil {
				t.Fatalf("advance: %v", err)
			}
			if leased {
				if claim, err := store.ClaimWorkflowEffect(ctx, workflowID, "chain:1", "owner-1", time.Now().UnixMilli(), 60_000); err != nil || claim.Status != state.WorkflowEffectClaimed {
					t.Fatalf("claim = %+v, %v", claim, err)
				}
			}

			cancelled, err := store.AtomicCancelWorkflow(ctx, workflowID, core.NowFormatted())
			if err != nil {
				t.Fatalf("cancel workflow: %v", err)
			}
			if !cancelled.Applied || cancelled.State != "cancelled" ||
				len(cancelled.EffectJobIDs) != 1 || cancelled.EffectJobIDs[0] != "stable-cancel-job" {
				t.Fatalf("cancel result = %+v", cancelled)
			}
			workflow, err := store.GetWorkflow(ctx, workflowID)
			if err != nil || workflow["state"] != "cancelled" {
				t.Fatalf("workflow = %v, %v", workflow, err)
			}
			if effects, err := store.GetWorkflowEffects(ctx, workflowID); err != nil || len(effects) != 0 {
				t.Fatalf("effects after cancellation = %v, %v", effects, err)
			}
			if pending, err := store.GetWorkflowsWithPendingEffects(ctx); err != nil || len(pending) != 0 {
				t.Fatalf("pending index after cancellation = %v, %v", pending, err)
			}
			if claim, err := store.ClaimWorkflowEffect(ctx, workflowID, "chain:1", "owner-2", time.Now().UnixMilli(), 60_000); err != nil || claim.Status != state.WorkflowEffectFenced {
				t.Fatalf("post-cancel claim = %+v, %v", claim, err)
			}
		})
	}
}

// TestWorkflowEffectLeaseCompleteLifecycle verifies claim leasing, busy while
// leased, idempotent completion with a single job-list append, and outbox
// cleanup when drained.
func TestWorkflowEffectLeaseCompleteLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()
	ctx := context.Background()
	seedChainWorkflow(t, store, "wf-life", 2)
	if _, err := store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
		WorkflowID: "wf-life", JobID: "job-0", Step: 0, CompletedAt: core.NowFormatted(),
		NextChainJobID: "chain-1", OnCompleteJobID: "c", OnSuccessJobID: "s", OnFailureJobID: "f",
	}); err != nil {
		t.Fatalf("advance: %v", err)
	}

	now := time.Now().UnixMilli()
	claim, err := store.ClaimWorkflowEffect(ctx, "wf-life", "chain:1", "owner-1", now, 60000)
	if err != nil || claim.Status != state.WorkflowEffectClaimed || claim.JobID != "chain-1" {
		t.Fatalf("claim = %+v, err=%v", claim, err)
	}
	busy, err := store.ClaimWorkflowEffect(ctx, "wf-life", "chain:1", "owner-2", now+1, 60000)
	if err != nil || busy.Status != state.WorkflowEffectBusy {
		t.Fatalf("busy claim = %+v, err=%v", busy, err)
	}

	// Complete twice: the append must happen exactly once.
	if completed, err := store.CompleteWorkflowEffect(ctx, "wf-life", "chain:1", "owner-1", "chain-1", true); err != nil || !completed {
		t.Fatalf("complete = %v, %v", completed, err)
	}
	if completed, err := store.CompleteWorkflowEffect(ctx, "wf-life", "chain:1", "owner-1", "chain-1", true); err != nil || completed {
		t.Fatalf("complete again = %v, %v; want no-op", completed, err)
	}
	jobs, err := store.GetWorkflowJobs(ctx, "wf-life")
	if err != nil {
		t.Fatalf("get workflow jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0] != "chain-1" {
		t.Fatalf("workflow jobs = %v, want exactly one append", jobs)
	}

	// The drained outbox is cleaned up.
	effects, err := store.GetWorkflowEffects(ctx, "wf-life")
	if err != nil || len(effects) != 0 {
		t.Fatalf("effects after drain = %v, err=%v", effects, err)
	}
	pending, err := store.GetWorkflowsWithPendingEffects(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after drain = %v, err=%v", pending, err)
	}
}

// TestWorkflowEffectLeaseExpiryAllowsReclaim verifies a lease-expired effect can
// be reclaimed by another owner, which is how a crashed drainer's work recovers.
func TestWorkflowEffectLeaseExpiryAllowsReclaim(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()
	ctx := context.Background()
	seedChainWorkflow(t, store, "wf-lease", 2)
	if _, err := store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
		WorkflowID: "wf-lease", JobID: "job-0", Step: 0, CompletedAt: core.NowFormatted(),
		NextChainJobID: "chain-1", OnCompleteJobID: "c", OnSuccessJobID: "s", OnFailureJobID: "f",
	}); err != nil {
		t.Fatalf("advance: %v", err)
	}

	base := time.Now().UnixMilli()
	if _, err := store.ClaimWorkflowEffect(ctx, "wf-lease", "chain:1", "owner-1", base, 1000); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	// Before expiry: busy.
	if claim, err := store.ClaimWorkflowEffect(ctx, "wf-lease", "chain:1", "owner-2", base+500, 1000); err != nil || claim.Status != state.WorkflowEffectBusy {
		t.Fatalf("pre-expiry claim = %+v, err=%v", claim, err)
	}
	// After expiry: reclaimable.
	claim, err := store.ClaimWorkflowEffect(ctx, "wf-lease", "chain:1", "owner-2", base+2000, 1000)
	if err != nil || claim.Status != state.WorkflowEffectClaimed || claim.JobID != "chain-1" {
		t.Fatalf("post-expiry claim = %+v, err=%v", claim, err)
	}
}

func TestWorkflowEffectExpiredLeaseNeverResetsExistingJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, lifecycleState := range []string{core.StateActive, core.StateCompleted} {
		t.Run(lifecycleState, func(t *testing.T) {
			store, raw, cleanup := setupRedisWithRawClient(t)
			defer cleanup()
			ctx := context.Background()
			workflowID := "wf-expired-" + lifecycleState
			seedChainWorkflow(t, store, workflowID, 2)
			if _, err := store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
				WorkflowID: workflowID, JobID: "job-0", Step: 0, CompletedAt: core.NowFormatted(),
				NextChainJobID: "stable-effect-job", OnCompleteJobID: "c", OnSuccessJobID: "s", OnFailureJobID: "f",
			}); err != nil {
				t.Fatalf("advance: %v", err)
			}

			base := time.Now().UnixMilli()
			if claim, err := store.ClaimWorkflowEffect(ctx, workflowID, "chain:1", "owner-1", base, 1000); err != nil || claim.Status != state.WorkflowEffectClaimed {
				t.Fatalf("first claim = %+v, %v", claim, err)
			}
			job := &core.Job{
				ID:           "stable-effect-job",
				Type:         "task.second",
				Queue:        "effects",
				State:        core.StateAvailable,
				WorkflowID:   workflowID,
				WorkflowStep: 1,
			}
			if status, err := store.AtomicCreateWorkflowEffectJob(ctx, workflowID, "chain:1", "owner-1", job, 1, false); err != nil || status != state.WorkflowEffectJobCreated {
				t.Fatalf("create effect job = %q, %v", status, err)
			}
			if err := store.RemoveFromAvailable(ctx, job.Queue, job.ID); err != nil {
				t.Fatalf("remove available: %v", err)
			}
			if err := store.UpdateJob(ctx, job.ID, map[string]any{"state": lifecycleState}); err != nil {
				t.Fatalf("set lifecycle state: %v", err)
			}
			if lifecycleState == core.StateActive {
				if err := store.AddToActive(ctx, job.Queue, job.ID); err != nil {
					t.Fatalf("add active: %v", err)
				}
			}

			if claim, err := store.ClaimWorkflowEffect(ctx, workflowID, "chain:1", "owner-2", base+2000, 1000); err != nil || claim.Status != state.WorkflowEffectClaimed {
				t.Fatalf("reclaim = %+v, %v", claim, err)
			}
			if status, err := store.AtomicCreateWorkflowEffectJob(ctx, workflowID, "chain:1", "owner-1", job, 1, false); err != nil || status != state.WorkflowEffectJobNotOwner {
				t.Fatalf("stale owner create = %q, %v", status, err)
			}
			if completed, err := store.CompleteWorkflowEffect(ctx, workflowID, "chain:1", "owner-1", job.ID, true); err != nil || completed {
				t.Fatalf("stale owner complete = %v, %v", completed, err)
			}
			if status, err := store.AtomicCreateWorkflowEffectJob(ctx, workflowID, "chain:1", "owner-2", job, 1, false); err != nil || status != state.WorkflowEffectJobExisting {
				t.Fatalf("current owner reconcile = %q, %v", status, err)
			}
			if completed, err := store.CompleteWorkflowEffect(ctx, workflowID, "chain:1", "owner-2", job.ID, true); err != nil || !completed {
				t.Fatalf("current owner complete = %v, %v", completed, err)
			}

			persisted, err := store.GetJob(ctx, job.ID)
			if err != nil || persisted.State != lifecycleState {
				t.Fatalf("persisted effect job = %+v, %v; state was reset", persisted, err)
			}
			if score, err := raw.ZScore(ctx, "ojs:queue:effects:available", job.ID).Result(); err == nil {
				t.Fatalf("existing %s job was re-added to available with score %v", lifecycleState, score)
			}
			if jobs, err := store.GetWorkflowJobs(ctx, workflowID); err != nil || len(jobs) != 1 || jobs[0] != job.ID {
				t.Fatalf("workflow jobs = %v, %v; want one stable ID", jobs, err)
			}
		})
	}
}

// TestWorkflowBatchCallbackEffectsRecorded verifies a terminal batch records the
// on_complete and the success/failure callback effects.
func TestWorkflowBatchCallbackEffectsRecorded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()
	ctx := context.Background()
	cb, _ := json.Marshal(core.WorkflowCallbacks{
		OnComplete: &core.WorkflowCallback{Type: "cb.complete"},
		OnSuccess:  &core.WorkflowCallback{Type: "cb.success"},
	})
	if err := store.SaveWorkflow(ctx, "wf-batch", map[string]any{
		"id": "wf-batch", "type": "batch", "state": "running",
		"total": "1", "completed": "0", "failed": "0", "callbacks": string(cb),
	}); err != nil {
		t.Fatalf("save workflow: %v", err)
	}

	advance, err := store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
		WorkflowID: "wf-batch", JobID: "job-0", Step: 0, CompletedAt: core.NowFormatted(),
		NextChainJobID: "n", OnCompleteJobID: "cb-complete", OnSuccessJobID: "cb-success", OnFailureJobID: "cb-failure",
	})
	if err != nil || !advance.TerminalOwner || !advance.HasPendingEffects {
		t.Fatalf("advance = %+v, err=%v", advance, err)
	}
	effects, err := store.GetWorkflowEffects(ctx, "wf-batch")
	if err != nil {
		t.Fatalf("get effects: %v", err)
	}
	if effects["callback:on_complete"] != "pending|cb-complete" || effects["callback:on_success"] != "pending|cb-success" {
		t.Fatalf("callback effects = %v", effects)
	}
	if _, ok := effects["callback:on_failure"]; ok {
		t.Fatalf("unexpected on_failure effect for a successful batch: %v", effects)
	}
}

// TestWorkflowEffectConcurrentClaimSingleDispatch verifies that under many
// concurrent claim attempts exactly one drainer leases the effect while the
// others observe it as busy, so an effect is dispatched exactly once.
func TestWorkflowEffectConcurrentClaimSingleDispatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, cleanup := setupRedis(t)
	defer cleanup()
	ctx := context.Background()
	seedChainWorkflow(t, store, "wf-conc", 2)
	if _, err := store.AtomicAdvanceWorkflow(ctx, state.WorkflowAdvanceInput{
		WorkflowID: "wf-conc", JobID: "job-0", Step: 0, CompletedAt: core.NowFormatted(),
		NextChainJobID: "chain-1", OnCompleteJobID: "c", OnSuccessJobID: "s", OnFailureJobID: "f",
	}); err != nil {
		t.Fatalf("advance: %v", err)
	}

	const n = 24
	now := time.Now().UnixMilli()
	var wg sync.WaitGroup
	claims := make(chan string, n)
	errs := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			claim, err := store.ClaimWorkflowEffect(ctx, "wf-conc", "chain:1", "owner-"+strconv.Itoa(i), now, 60000)
			if err != nil {
				errs <- err
				return
			}
			claims <- claim.Status
		}(i)
	}
	close(start)
	wg.Wait()
	close(claims)
	close(errs)

	for err := range errs {
		t.Fatalf("claim: %v", err)
	}
	claimed, busy := 0, 0
	for status := range claims {
		switch status {
		case state.WorkflowEffectClaimed:
			claimed++
		case state.WorkflowEffectBusy:
			busy++
		default:
			t.Fatalf("unexpected claim status %q", status)
		}
	}
	if claimed != 1 || busy != n-1 {
		t.Fatalf("claimed=%d busy=%d, want 1/%d", claimed, busy, n-1)
	}
}
