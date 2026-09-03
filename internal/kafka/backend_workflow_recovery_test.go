package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// TestAckRecoversAdvancementAndEnqueuesChainStepOnce covers the ACK recovery
// path with the durable outbox: a client disconnect (context cancellation) after
// the atomic advance leaves the effect pending, and a later ACK (or drain)
// finishes the dispatch exactly once. Duplicate ACKs then conflict.
func TestAckRecoversAdvancementAndEnqueuesChainStepOnce(t *testing.T) {
	store := newFakeWorkflowStore()
	newChainWorkflow(store, "wf-chain", []core.WorkflowJobRequest{
		{Name: "first", Type: "task.first"},
		{Name: "second", Type: "task.second"},
	})
	store.putJob(&core.Job{
		ID: "job-first", Type: "task.first", Queue: "default",
		State: core.StateActive, WorkflowID: "wf-chain", WorkflowStep: 0,
	})

	backend := &KafkaBackend{store: store, producer: &Producer{}}

	// First ACK: cancel the context so the drain after the atomic advance fails,
	// leaving the effect durably pending.
	ctx, cancel := context.WithCancel(context.Background())
	store.advanceHook = func() {
		// Cancel right after the advance records the effect but before the drain.
		cancel()
	}
	if resp, err := backend.Ack(ctx, "job-first", []byte(`{"value":42}`)); err == nil || resp != nil {
		t.Fatalf("cancelled Ack = %+v, %v; want a durable-but-undispatched error", resp, err)
	}
	store.advanceHook = nil
	if len(store.createdJobs()) != 0 {
		t.Fatalf("chain step dispatched despite cancellation: pushed=%d", len(store.createdJobs()))
	}

	// Recovery ACK: the job is already completed; advancement recovers by draining
	// the pending effect exactly once.
	resp, err := backend.Ack(context.Background(), "job-first", nil)
	if err != nil {
		t.Fatalf("recovery Ack error = %v", err)
	}
	pushed := store.createdJobs()
	if resp.State != core.StateCompleted || len(pushed) != 1 || pushed[0].Type != "task.second" {
		t.Fatalf("recovery resp=%+v pushed=%v", resp, pushed)
	}

	// Duplicate ACK: nothing pending remains, so it conflicts.
	if resp, err := backend.Ack(context.Background(), "job-first", nil); err == nil || resp != nil {
		t.Fatalf("duplicate Ack = %+v, %v; want conflict", resp, err)
	} else {
		var ojsErr *core.OJSError
		if !errors.As(err, &ojsErr) || ojsErr.Code != core.ErrCodeConflict {
			t.Fatalf("duplicate Ack error = %v, want conflict", err)
		}
	}
	if len(store.createdJobs()) != 1 {
		t.Fatalf("duplicate ACK re-dispatched: pushed=%d", len(store.createdJobs()))
	}
}

// TestDiscardNackRecoversAndFiresBatchCallbackOnce covers the discard-NACK
// recovery path: a transient advancement failure leaves the terminal discard
// unadvanced, and a retry advances and fires the batch callback exactly once.
func TestDiscardNackRecoversAndFiresBatchCallbackOnce(t *testing.T) {
	store := newFakeWorkflowStore()
	newBatchWorkflow(store, "wf-batch", 1, core.WorkflowCallbacks{
		OnComplete: &core.WorkflowCallback{Type: "callback.complete"},
	})
	maxAttempts := 0
	store.putJob(&core.Job{
		ID: "job-batch", Type: "task.batch", Queue: "default", State: core.StateActive,
		MaxAttempts: &maxAttempts, WorkflowID: "wf-batch", WorkflowStep: 0,
	})

	backend := &KafkaBackend{store: store, producer: &Producer{}}

	// The first advance fails transiently after the discard is durable.
	store.advanceErr = errors.New("transient workflow store failure")
	store.advanceErrN = 1

	if resp, err := backend.Nack(context.Background(), "job-batch", &core.JobError{Message: "boom"}, false); err == nil || resp != nil {
		t.Fatalf("first Nack = %+v, %v; want durable discard with advancement error", resp, err)
	}
	if len(store.createdJobs()) != 0 {
		t.Fatalf("callback fired despite advancement failure: pushed=%d", len(store.createdJobs()))
	}

	// Recovery NACK: the job is already discarded; advancement recovers and fires
	// the callback once.
	resp, err := backend.Nack(context.Background(), "job-batch", nil, false)
	if err != nil {
		t.Fatalf("recovery Nack error = %v", err)
	}
	pushed := store.createdJobs()
	if resp.State != core.StateDiscarded || len(pushed) != 1 || pushed[0].Type != "callback.complete" {
		t.Fatalf("recovery resp=%+v pushed=%v", resp, pushed)
	}

	// Duplicate NACK: conflict, no extra callback.
	if resp, err := backend.Nack(context.Background(), "job-batch", nil, false); err == nil || resp != nil {
		t.Fatalf("duplicate Nack = %+v, %v; want conflict", resp, err)
	}
	if len(store.createdJobs()) != 1 {
		t.Fatalf("duplicate NACK re-fired callback: pushed=%d", len(store.createdJobs()))
	}
}

func TestAckNonWorkflowJobSkipsAdvancement(t *testing.T) {
	store := newFakeWorkflowStore()
	store.putJob(&core.Job{ID: "job-plain", Type: "task.plain", Queue: "default", State: core.StateActive})
	backend := &KafkaBackend{store: store, producer: &Producer{}}
	backend.pushJobFn = func(context.Context, *core.Job) (*core.Job, error) {
		t.Fatal("non-workflow ACK must not dispatch anything")
		return nil, nil
	}

	if _, err := backend.Ack(context.Background(), "job-plain", nil); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if store.advanceObs != 0 {
		t.Fatalf("AtomicAdvanceWorkflow called %d times for a non-workflow job", store.advanceObs)
	}
}
