package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

type gatedWorkflowEffectStore struct {
	*fakeWorkflowStore
	entered     chan struct{}
	release     chan struct{}
	afterCreate bool
	once        sync.Once
}

func (s *gatedWorkflowEffectStore) AtomicCreateWorkflowEffectJob(
	ctx context.Context,
	workflowID string,
	effectID string,
	owner string,
	job *core.Job,
	score float64,
	scheduled bool,
) (string, error) {
	if s.afterCreate {
		status, err := s.fakeWorkflowStore.AtomicCreateWorkflowEffectJob(
			ctx, workflowID, effectID, owner, job, score, scheduled,
		)
		gated := false
		s.once.Do(func() {
			gated = true
			close(s.entered)
		})
		if gated {
			<-s.release
		}
		return status, err
	}
	gated := false
	s.once.Do(func() {
		gated = true
		close(s.entered)
	})
	if gated {
		<-s.release
	}
	return s.fakeWorkflowStore.AtomicCreateWorkflowEffectJob(
		ctx, workflowID, effectID, owner, job, score, scheduled,
	)
}

func seedPendingChainEffect(t *testing.T, store *fakeWorkflowStore, workflowID string) {
	t.Helper()
	newChainWorkflow(store, workflowID, []core.WorkflowJobRequest{
		{Name: "first", Type: "task.first"},
		{Name: "second", Type: "task.second"},
	})
	if _, err := store.AtomicAdvanceWorkflow(context.Background(), state.WorkflowAdvanceInput{
		WorkflowID:      workflowID,
		JobID:           "job-first",
		Step:            0,
		CompletedAt:     core.NowFormatted(),
		NextChainJobID:  "effect-job",
		OnCompleteJobID: "complete",
		OnSuccessJobID:  "success",
		OnFailureJobID:  "failure",
	}); err != nil {
		t.Fatalf("seed workflow effect: %v", err)
	}
}

func TestCancelWorkflowBeforeEffectClaimRevokesDispatch(t *testing.T) {
	store := newFakeWorkflowStore()
	seedPendingChainEffect(t, store, "wf-cancel-before")
	backend := &KafkaBackend{store: store, producer: &Producer{}}

	workflow, err := backend.CancelWorkflow(context.Background(), "wf-cancel-before")
	if err != nil {
		t.Fatalf("CancelWorkflow() error = %v", err)
	}
	if workflow.State != "cancelled" {
		t.Fatalf("workflow state = %q, want cancelled", workflow.State)
	}
	if err := backend.DrainWorkflowEffects(context.Background()); err != nil {
		t.Fatalf("DrainWorkflowEffects() error = %v", err)
	}
	if len(store.createdJobs()) != 0 {
		t.Fatalf("cancelled workflow created effect jobs: %+v", store.createdJobs())
	}
	if pending, _ := store.GetWorkflowsWithPendingEffects(context.Background()); len(pending) != 0 {
		t.Fatalf("cancelled workflow remained in pending index: %v", pending)
	}
}

func TestCancelWorkflowMissingReturnsNotFound(t *testing.T) {
	backend := &KafkaBackend{store: newFakeWorkflowStore(), producer: &Producer{}}
	workflow, err := backend.CancelWorkflow(context.Background(), "missing")
	if workflow != nil || err == nil {
		t.Fatalf("CancelWorkflow() = %+v, %v; want not_found", workflow, err)
	}
	var ojsErr *core.OJSError
	if !errors.As(err, &ojsErr) || ojsErr.Code != core.ErrCodeNotFound {
		t.Fatalf("error = %v, want not_found", err)
	}
}

func TestCancelWorkflowDuringEffectLeaseFencesCreation(t *testing.T) {
	base := newFakeWorkflowStore()
	seedPendingChainEffect(t, base, "wf-cancel-leased")
	store := &gatedWorkflowEffectStore{
		fakeWorkflowStore: base,
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	backend := &KafkaBackend{store: store, producer: &Producer{}}

	drainErr := make(chan error, 1)
	go func() {
		_, err := backend.processWorkflowEffects(context.Background(), "wf-cancel-leased")
		drainErr <- err
	}()
	<-store.entered

	if _, err := backend.CancelWorkflow(context.Background(), "wf-cancel-leased"); err != nil {
		t.Fatalf("CancelWorkflow() error = %v", err)
	}
	close(store.release)
	if err := <-drainErr; err != nil {
		t.Fatalf("drain after cancellation = %v", err)
	}
	if len(base.createdJobs()) != 0 {
		t.Fatalf("leased effect created after cancellation: %+v", base.createdJobs())
	}
}

func TestCancelWorkflowRacingCreatedEffectCancelsStableJob(t *testing.T) {
	base := newFakeWorkflowStore()
	seedPendingChainEffect(t, base, "wf-cancel-race")
	store := &gatedWorkflowEffectStore{
		fakeWorkflowStore: base,
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
		afterCreate:       true,
	}
	backend := &KafkaBackend{store: store, producer: &Producer{}}

	drainErr := make(chan error, 1)
	go func() {
		_, err := backend.processWorkflowEffects(context.Background(), "wf-cancel-race")
		drainErr <- err
	}()
	<-store.entered

	if got := base.appendedJobs("wf-cancel-race"); len(got) != 0 {
		t.Fatalf("effect job appended before lease completion: %v", got)
	}
	if _, err := backend.CancelWorkflow(context.Background(), "wf-cancel-race"); err != nil {
		t.Fatalf("CancelWorkflow() error = %v", err)
	}
	created, err := base.GetJob(context.Background(), "effect-job")
	if err != nil {
		t.Fatalf("get raced effect job: %v", err)
	}
	if created.State != core.StateCancelled {
		t.Fatalf("raced effect job state = %q, want cancelled", created.State)
	}

	close(store.release)
	if err := <-drainErr; err != nil {
		t.Fatalf("drain after cancellation = %v", err)
	}
	if got := base.appendedJobs("wf-cancel-race"); len(got) != 0 {
		t.Fatalf("stale lease appended a cancelled effect job: %v", got)
	}
}

func TestExpiredWorkflowEffectLeaseTwoDrainersNeverResetFetchedOrTerminalJob(t *testing.T) {
	for _, lifecycleState := range []string{core.StateActive, core.StateCompleted} {
		t.Run(lifecycleState, func(t *testing.T) {
			base := newFakeWorkflowStore()
			seedPendingChainEffect(t, base, "wf-expired-race")
			store := &gatedWorkflowEffectStore{
				fakeWorkflowStore: base,
				entered:           make(chan struct{}),
				release:           make(chan struct{}),
				afterCreate:       true,
			}
			now := time.Now()
			backend := &KafkaBackend{
				store:    store,
				producer: &Producer{},
				nowFn:    func() time.Time { return now },
			}

			firstErr := make(chan error, 1)
			go func() {
				_, err := backend.processWorkflowEffects(context.Background(), "wf-expired-race")
				firstErr <- err
			}()
			<-store.entered

			base.mu.Lock()
			base.jobs["effect-job"].State = lifecycleState
			base.mu.Unlock()
			now = now.Add(time.Duration(workflowEffectLeaseMs+1) * time.Millisecond)

			if _, err := backend.processWorkflowEffects(context.Background(), "wf-expired-race"); err != nil {
				t.Fatalf("second drainer: %v", err)
			}
			close(store.release)
			if err := <-firstErr; err != nil {
				t.Fatalf("stale drainer: %v", err)
			}

			if created := base.createdJobs(); len(created) != 1 {
				t.Fatalf("effect was created %d times, want once", len(created))
			}
			persisted, err := base.GetJob(context.Background(), "effect-job")
			if err != nil || persisted.State != lifecycleState {
				t.Fatalf("existing job was reset: %+v, %v", persisted, err)
			}
			if appended := base.appendedJobs("wf-expired-race"); len(appended) != 1 || appended[0] != "effect-job" {
				t.Fatalf("effect completion = %v, want one stable job", appended)
			}
		})
	}
}
