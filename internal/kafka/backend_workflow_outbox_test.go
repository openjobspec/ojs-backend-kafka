package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

func newBatchWorkflow(store *fakeWorkflowStore, id string, total int, callbacks core.WorkflowCallbacks) {
	cb, _ := json.Marshal(callbacks)
	store.putWorkflow(id, map[string]string{
		"id": id, "type": "batch", "state": "running",
		"total": strconv.Itoa(total), "completed": "0", "failed": "0",
		"callbacks": string(cb),
	})
}

func newChainWorkflow(store *fakeWorkflowStore, id string, steps []core.WorkflowJobRequest) {
	defs, _ := json.Marshal(steps)
	store.putWorkflow(id, map[string]string{
		"id": id, "type": "chain", "state": "running",
		"total": strconv.Itoa(len(steps)), "completed": "0", "failed": "0",
		"job_defs": string(defs),
	})
}

func TestAdvanceWorkflowFiresBatchCallbacksOnlyForTerminalOwner(t *testing.T) {
	for _, terminalOwner := range []bool{false, true} {
		name := map[bool]string{false: "non-owner", true: "owner"}[terminalOwner]
		t.Run(name, func(t *testing.T) {
			store := newFakeWorkflowStore()
			total := 2
			if terminalOwner {
				total = 1
			}
			newBatchWorkflow(store, "wf-1", total, core.WorkflowCallbacks{
				OnComplete: &core.WorkflowCallback{Type: "callback.complete"},
				OnSuccess:  &core.WorkflowCallback{Type: "callback.success"},
			})
			job := &core.Job{ID: "job-1", Type: "task", WorkflowID: "wf-1", WorkflowStep: 0, State: core.StateActive}
			store.putJob(job)

			backend := &KafkaBackend{store: store, producer: &Producer{}}

			if err := backend.AdvanceWorkflow(context.Background(), "wf-1", "job-1", nil, false); err != nil {
				t.Fatalf("AdvanceWorkflow() error = %v", err)
			}

			pushed := store.createdJobs()
			types := pushedTypes(pushed)
			if terminalOwner {
				if len(types) != 2 || !types["callback.complete"] || !types["callback.success"] {
					t.Fatalf("terminal owner callbacks = %v, want complete+success", types)
				}
			} else if len(types) != 0 {
				t.Fatalf("non-owner fired callbacks = %v", types)
			}
		})
	}
}

func TestAdvanceWorkflowPropagatesAtomicFailure(t *testing.T) {
	store := newFakeWorkflowStore()
	store.putWorkflow("wf-1", map[string]string{
		"id": "wf-1", "type": "group", "state": "running", "total": "1", "completed": "0", "failed": "0",
	})
	store.putJob(&core.Job{ID: "job-1", Type: "task", WorkflowID: "wf-1"})
	store.advanceErr = errors.New("redis unavailable")
	store.advanceErrN = 1
	backend := &KafkaBackend{store: store, producer: &Producer{}}

	if err := backend.AdvanceWorkflow(context.Background(), "wf-1", "job-1", nil, false); err == nil {
		t.Fatal("expected atomic advancement error")
	}
}

func TestAdvanceWorkflowEnqueuesChainStepOnce(t *testing.T) {
	store := newFakeWorkflowStore()
	newChainWorkflow(store, "wf-chain", []core.WorkflowJobRequest{
		{Name: "first", Type: "task.first"},
		{Name: "second", Type: "task.second"},
	})
	store.putJob(&core.Job{ID: "job-first", Type: "task.first", WorkflowID: "wf-chain", WorkflowStep: 0, State: core.StateActive})

	backend := &KafkaBackend{store: store, producer: &Producer{}}

	if err := backend.AdvanceWorkflow(context.Background(), "wf-chain", "job-first", json.RawMessage(`{"value":42}`), false); err != nil {
		t.Fatalf("AdvanceWorkflow() error = %v", err)
	}
	pushed := store.createdJobs()
	if len(pushed) != 1 || pushed[0].Type != "task.second" || pushed[0].WorkflowStep != 1 {
		t.Fatalf("pushed = %+v, want one task.second at step 1", pushed)
	}
	if len(pushed[0].ParentResults) != 1 || string(pushed[0].ParentResults[0]) != `{"value":42}` {
		t.Fatalf("parent results = %s", pushed[0].ParentResults)
	}
	if got := store.appendedJobs("wf-chain"); !reflect.DeepEqual(got, []string{pushed[0].ID}) {
		t.Fatalf("appended = %v, want [%s]", got, pushed[0].ID)
	}

	// A duplicate advance (already applied, effect already drained) must not
	// re-enqueue or re-append.
	if err := backend.AdvanceWorkflow(context.Background(), "wf-chain", "job-first", nil, false); err != nil {
		t.Fatalf("duplicate AdvanceWorkflow() error = %v", err)
	}
	if len(store.createdJobs()) != 1 || len(store.appendedJobs("wf-chain")) != 1 {
		t.Fatalf("duplicate advance caused extra dispatch: pushed=%d appended=%d", len(store.createdJobs()), len(store.appendedJobs("wf-chain")))
	}
}

// TestWorkflowChainEffectDrainsAfterCrash covers a crash after the atomic
// advance: the effect is recorded durably but not drained (as if the worker
// died), and a later drain (restart) dispatches it exactly once without the
// original worker retrying.
func TestWorkflowChainEffectDrainsAfterCrash(t *testing.T) {
	store := newFakeWorkflowStore()
	newChainWorkflow(store, "wf-chain", []core.WorkflowJobRequest{
		{Name: "first", Type: "task.first"},
		{Name: "second", Type: "task.second"},
	})

	// Record the advancement effect directly, then do NOT drain (simulate crash).
	advance, err := store.AtomicAdvanceWorkflow(context.Background(), state.WorkflowAdvanceInput{
		WorkflowID: "wf-chain", JobID: "job-first", Step: 0,
		Result: json.RawMessage(`{"value":1}`), CompletedAt: core.NowFormatted(),
		NextChainJobID: "chain-step-1", OnCompleteJobID: "c", OnSuccessJobID: "s", OnFailureJobID: "f",
	})
	if err != nil || !advance.Applied || !advance.HasPendingEffects {
		t.Fatalf("advance = %+v err=%v", advance, err)
	}

	backend := &KafkaBackend{store: store, producer: &Producer{}}

	// Restart drain dispatches the orphaned effect exactly once.
	if err := backend.DrainWorkflowEffects(context.Background()); err != nil {
		t.Fatalf("DrainWorkflowEffects() error = %v", err)
	}
	pushed := store.createdJobs()
	if len(pushed) != 1 || pushed[0].ID != "chain-step-1" || pushed[0].Type != "task.second" {
		t.Fatalf("drained push = %+v, want one task.second with preassigned ID", pushed)
	}

	// A second drain is idempotent.
	if err := backend.DrainWorkflowEffects(context.Background()); err != nil {
		t.Fatalf("second DrainWorkflowEffects() error = %v", err)
	}
	if len(store.createdJobs()) != 1 {
		t.Fatalf("second drain re-dispatched: pushes=%d", len(store.createdJobs()))
	}
	if got := store.appendedJobs("wf-chain"); !reflect.DeepEqual(got, []string{"chain-step-1"}) {
		t.Fatalf("appended = %v, want [chain-step-1]", got)
	}
	ids, _ := store.GetWorkflowsWithPendingEffects(context.Background())
	if len(ids) != 0 {
		t.Fatalf("drained workflow still marked pending: %v", ids)
	}
}

// TestWorkflowEffectReconcilesAmbiguousPush covers an ambiguous push: the job is
// persisted but the push reports an error. A retry must reconcile by the stable
// job ID and never dispatch a duplicate.
func TestWorkflowEffectReconcilesAmbiguousPush(t *testing.T) {
	store := newFakeWorkflowStore()
	newChainWorkflow(store, "wf-chain", []core.WorkflowJobRequest{
		{Name: "first", Type: "task.first"},
		{Name: "second", Type: "task.second"},
	})
	if _, err := store.AtomicAdvanceWorkflow(context.Background(), state.WorkflowAdvanceInput{
		WorkflowID: "wf-chain", JobID: "job-first", Step: 0, CompletedAt: core.NowFormatted(),
		NextChainJobID: "chain-step-1", OnCompleteJobID: "c", OnSuccessJobID: "s", OnFailureJobID: "f",
	}); err != nil {
		t.Fatalf("advance: %v", err)
	}

	backend := &KafkaBackend{store: store, producer: &Producer{}}
	store.createErr["task.second"] = errors.New("redis reply lost after persist")
	store.createAfterPersist["task.second"] = true

	if err := backend.DrainWorkflowEffects(context.Background()); err == nil {
		t.Fatal("expected the ambiguous push to surface an error")
	}
	if attempts := store.creationAttempts("task.second"); attempts != 1 {
		t.Fatalf("pushes on first drain = %d, want 1", attempts)
	}

	// The retry reconciles by stable job ID: the job already exists, so it is not
	// pushed again, and the effect completes.
	if err := backend.DrainWorkflowEffects(context.Background()); err != nil {
		t.Fatalf("reconcile drain error = %v", err)
	}
	if len(store.createdJobs()) != 1 {
		t.Fatalf("reconcile re-created the job: creates=%d, want 1", len(store.createdJobs()))
	}
	if got := store.appendedJobs("wf-chain"); !reflect.DeepEqual(got, []string{"chain-step-1"}) {
		t.Fatalf("appended = %v, want [chain-step-1]", got)
	}
}

// TestWorkflowBatchCallbacksDrainIndependently covers partial callback failure:
// one callback failing must not suppress the other, and a retry completes only
// the outstanding one.
func TestWorkflowBatchCallbacksDrainIndependently(t *testing.T) {
	store := newFakeWorkflowStore()
	newBatchWorkflow(store, "wf-batch", 1, core.WorkflowCallbacks{
		OnComplete: &core.WorkflowCallback{Type: "callback.complete"},
		OnSuccess:  &core.WorkflowCallback{Type: "callback.success"},
	})
	if _, err := store.AtomicAdvanceWorkflow(context.Background(), state.WorkflowAdvanceInput{
		WorkflowID: "wf-batch", JobID: "job-1", Step: 0, CompletedAt: core.NowFormatted(),
		NextChainJobID: "n", OnCompleteJobID: "cb-complete", OnSuccessJobID: "cb-success", OnFailureJobID: "cb-failure",
	}); err != nil {
		t.Fatalf("advance: %v", err)
	}

	backend := &KafkaBackend{store: store, producer: &Producer{}}
	store.createErr["callback.complete"] = errors.New("callback.complete transient failure")

	// First drain: on_success succeeds, on_complete fails but does not suppress it.
	if err := backend.DrainWorkflowEffects(context.Background()); err == nil {
		t.Fatal("expected the failing callback to surface an error")
	}
	fired := pushedTypeCounts(store.createdJobs())
	if fired["callback.success"] != 1 || fired["callback.complete"] != 0 {
		t.Fatalf("after first drain fired = %v, want success once and complete not yet", fired)
	}

	// Second drain: only the outstanding callback is retried and completes.
	delete(store.createErr, "callback.complete")
	if err := backend.DrainWorkflowEffects(context.Background()); err != nil {
		t.Fatalf("retry drain error = %v", err)
	}
	fired = pushedTypeCounts(store.createdJobs())
	if fired["callback.success"] != 1 || fired["callback.complete"] != 1 {
		t.Fatalf("after retry fired = %v, want each callback exactly once", fired)
	}
	ids, _ := store.GetWorkflowsWithPendingEffects(context.Background())
	if len(ids) != 0 {
		t.Fatalf("batch workflow still pending after all callbacks: %v", ids)
	}
}

func pushedTypes(pushed []*core.Job) map[string]bool {
	types := map[string]bool{}
	for _, job := range pushed {
		types[job.Type] = true
	}
	return types
}

func pushedTypeCounts(pushed []*core.Job) map[string]int {
	counts := map[string]int{}
	for _, job := range pushed {
		counts[job.Type]++
	}
	return counts
}

// TestWorkflowEffectConcurrentDrainDispatchesOnce covers concurrent retries: many
// drainers racing on the same effect must dispatch and append it exactly once.
func TestWorkflowEffectConcurrentDrainDispatchesOnce(t *testing.T) {
	store := newFakeWorkflowStore()
	newChainWorkflow(store, "wf-conc", []core.WorkflowJobRequest{
		{Name: "first", Type: "task.first"},
		{Name: "second", Type: "task.second"},
	})
	if _, err := store.AtomicAdvanceWorkflow(context.Background(), state.WorkflowAdvanceInput{
		WorkflowID: "wf-conc", JobID: "job-first", Step: 0, CompletedAt: core.NowFormatted(),
		NextChainJobID: "chain-step-1", OnCompleteJobID: "c", OnSuccessJobID: "s", OnFailureJobID: "f",
	}); err != nil {
		t.Fatalf("advance: %v", err)
	}

	backend := &KafkaBackend{store: store, producer: &Producer{}}

	const n = 24
	var wg sync.WaitGroup
	errs := make(chan error, n)
	begin := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-begin
			if err := backend.DrainWorkflowEffects(context.Background()); err != nil {
				errs <- err
			}
		}()
	}
	close(begin)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent drain error: %v", err)
	}

	gotPushes := len(store.createdJobs())
	if gotPushes != 1 {
		t.Fatalf("effect dispatched %d times under concurrency, want exactly 1", gotPushes)
	}
	if got := store.appendedJobs("wf-conc"); !reflect.DeepEqual(got, []string{"chain-step-1"}) {
		t.Fatalf("appended = %v, want a single [chain-step-1]", got)
	}
}
