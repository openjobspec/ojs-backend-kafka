package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

// uniquePushStore is a mock Store for exercising the KafkaBackend unique-job
// glue (pushUniqueJob and Cancel) without a live Redis or Kafka producer.
type uniquePushStore struct {
	state.Store
	mu sync.Mutex

	result    *state.UniqueClaimResult
	claimErr  error
	jobs      map[string]*core.Job
	claimArgs struct {
		conflict       string
		relevantStates []string
		scheduled      bool
	}
	replaceExpected string
	stateAtCreate   string
}

func (s *uniquePushStore) ClaimUniqueJob(_ context.Context, _ string, job *core.Job, _ float64, scheduled bool, _ int64, conflict string, relevantStates []string) (*state.UniqueClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimArgs.conflict = conflict
	s.claimArgs.relevantStates = relevantStates
	s.claimArgs.scheduled = scheduled
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	// Model the claim: the new job is created when the outcome is "claimed".
	if s.result.Outcome == state.UniqueClaimClaimed {
		s.jobs[job.ID] = job
	}
	return s.result, nil
}

func (s *uniquePushStore) GetUniqueJobID(context.Context, string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replaceExpected != "" {
		return s.replaceExpected, nil
	}
	if s.result != nil {
		return s.result.ExistingID, nil
	}
	return "", nil
}

func (s *uniquePushStore) ReplaceUniqueJob(_ context.Context, _ string, expectedID string, job *core.Job, _ float64, scheduled bool, _ int64, relevantStates []string, cancelledAt string) (*state.UniqueClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimArgs.conflict = "replace"
	s.claimArgs.relevantStates = relevantStates
	s.claimArgs.scheduled = scheduled
	s.replaceExpected = expectedID
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	if s.result.Outcome == state.UniqueClaimClaimed {
		if s.result.ExistingID != "" {
			if old := s.jobs[s.result.ExistingID]; old != nil {
				old.State = core.StateCancelled
				old.CancelledAt = cancelledAt
				s.stateAtCreate = old.State
			}
		}
		s.jobs[job.ID] = job
	}
	return s.result, nil
}

func (s *uniquePushStore) GetJob(_ context.Context, jobID string) (*core.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.jobs[jobID]; ok {
		return job, nil
	}
	return nil, core.NewNotFoundError("Job", jobID)
}

func (s *uniquePushStore) UpdateJob(_ context.Context, jobID string, updates map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.jobs[jobID]; ok {
		if v, ok := updates["state"].(string); ok {
			job.State = v
		}
	}
	return nil
}

func (s *uniquePushStore) RemoveFromAvailable(context.Context, string, string) error { return nil }
func (s *uniquePushStore) RemoveFromActive(context.Context, string, string) error    { return nil }
func (s *uniquePushStore) RemoveFromScheduled(context.Context, string) error         { return nil }
func (s *uniquePushStore) RemoveFromRetry(context.Context, string) error             { return nil }
func (s *uniquePushStore) DeleteVisibility(context.Context, string) error            { return nil }

func newUniquePushStore(result *state.UniqueClaimResult, existing ...*core.Job) *uniquePushStore {
	jobs := make(map[string]*core.Job)
	for _, job := range existing {
		jobs[job.ID] = job
	}
	return &uniquePushStore{result: result, jobs: jobs}
}

func uniquePushBackend(store *uniquePushStore) *KafkaBackend {
	return &KafkaBackend{store: store, producer: &Producer{}}
}

func TestPushUniqueJobRejectReturnsDuplicate(t *testing.T) {
	store := newUniquePushStore(&state.UniqueClaimResult{
		Outcome:    state.UniqueClaimRejected,
		ExistingID: "job-existing",
	})
	backend := uniquePushBackend(store)

	job := &core.Job{ID: "job-new", Type: "task", Queue: "default", Unique: &core.UniquePolicy{}}
	existing, claimed, err := backend.pushUniqueJob(context.Background(), job, 1, false)
	if claimed || existing != nil {
		t.Fatalf("reject returned existing=%v claimed=%v", existing, claimed)
	}
	var ojsErr *core.OJSError
	if !errors.As(err, &ojsErr) || ojsErr.Code != core.ErrCodeDuplicate {
		t.Fatalf("reject error = %v, want duplicate", err)
	}
	if ojsErr.Details["existing_job_id"] != "job-existing" {
		t.Fatalf("duplicate details = %v", ojsErr.Details)
	}
}

func TestPushUniqueJobIgnoreReturnsExistingJob(t *testing.T) {
	existing := &core.Job{ID: "job-existing", Type: "task", Queue: "default", State: core.StateActive}
	store := newUniquePushStore(&state.UniqueClaimResult{
		Outcome:    state.UniqueClaimIgnored,
		ExistingID: "job-existing",
	}, existing)
	backend := uniquePushBackend(store)

	job := &core.Job{ID: "job-new", Type: "task", Queue: "default", Unique: &core.UniquePolicy{OnConflict: "ignore"}}
	returned, claimed, err := backend.pushUniqueJob(context.Background(), job, 1, false)
	if err != nil {
		t.Fatalf("ignore error = %v", err)
	}
	if claimed {
		t.Fatal("ignore must not report a new claim")
	}
	if returned == nil || returned.ID != "job-existing" || !returned.IsExisting {
		t.Fatalf("ignore returned = %+v, want existing job flagged IsExisting", returned)
	}
	if store.claimArgs.conflict != "ignore" {
		t.Fatalf("conflict passed to claim = %q, want ignore", store.claimArgs.conflict)
	}
}

func TestPushUniqueJobClaimedWithoutReplacement(t *testing.T) {
	store := newUniquePushStore(&state.UniqueClaimResult{Outcome: state.UniqueClaimClaimed})
	backend := uniquePushBackend(store)

	job := &core.Job{ID: "job-new", Type: "task", Queue: "default", Unique: &core.UniquePolicy{}}
	existing, claimed, err := backend.pushUniqueJob(context.Background(), job, 1, false)
	if err != nil || !claimed || existing != nil {
		t.Fatalf("claimed path = existing:%v claimed:%v err:%v", existing, claimed, err)
	}
	// Reject is the default conflict action when unspecified.
	if store.claimArgs.conflict != "reject" {
		t.Fatalf("default conflict = %q, want reject", store.claimArgs.conflict)
	}
}

func TestPushUniqueJobReplaceCancelsReplacedJob(t *testing.T) {
	replaced := &core.Job{ID: "job-old", Type: "task", Queue: "default", State: core.StateAvailable}
	store := newUniquePushStore(&state.UniqueClaimResult{
		Outcome:    state.UniqueClaimClaimed,
		ExistingID: "job-old",
	}, replaced)
	backend := uniquePushBackend(store)

	job := &core.Job{ID: "job-new", Type: "task", Queue: "default", Unique: &core.UniquePolicy{OnConflict: "replace"}}
	existing, claimed, err := backend.pushUniqueJob(context.Background(), job, 1, false)
	if err != nil || !claimed || existing != nil {
		t.Fatalf("replace path = existing:%v claimed:%v err:%v", existing, claimed, err)
	}
	store.mu.Lock()
	jobState := store.jobs["job-old"].State
	store.mu.Unlock()
	if jobState != core.StateCancelled {
		t.Fatalf("replaced job state = %q, want cancelled", jobState)
	}
	if store.stateAtCreate != core.StateCancelled {
		t.Fatalf("replacement became visible while predecessor state was %q", store.stateAtCreate)
	}
}

func TestPushUniqueJobReplaceTreatsTerminalClaimAsStale(t *testing.T) {
	replaced := &core.Job{ID: "job-old", Type: "task", Queue: "default", State: core.StateCompleted}
	store := newUniquePushStore(&state.UniqueClaimResult{
		Outcome: state.UniqueClaimClaimed,
	}, replaced)
	store.replaceExpected = "job-old"
	backend := uniquePushBackend(store)

	job := &core.Job{ID: "job-new", Type: "task", Queue: "default", Unique: &core.UniquePolicy{OnConflict: "replace"}}
	_, claimed, err := backend.pushUniqueJob(context.Background(), job, 1, false)
	if err != nil || !claimed {
		t.Fatalf("replace over terminal job = claimed:%v err:%v", claimed, err)
	}
	if replaced.State != core.StateCompleted {
		t.Fatalf("terminal predecessor was mutated to %q", replaced.State)
	}
}

func TestPushUniqueJobReplaceRejectsActivePredecessor(t *testing.T) {
	active := &core.Job{ID: "job-active", Type: "task", Queue: "default", State: core.StateActive}
	store := newUniquePushStore(&state.UniqueClaimResult{
		Outcome:       state.UniqueClaimRejected,
		ExistingID:    active.ID,
		ExistingState: core.StateActive,
	}, active)
	backend := uniquePushBackend(store)

	job := &core.Job{ID: "job-new", Type: "task", Queue: "default", Unique: &core.UniquePolicy{OnConflict: "replace"}}
	existing, claimed, err := backend.pushUniqueJob(context.Background(), job, 1, false)
	if claimed || existing != nil {
		t.Fatalf("active replace = existing:%v claimed:%v", existing, claimed)
	}
	var ojsErr *core.OJSError
	if !errors.As(err, &ojsErr) || ojsErr.Code != core.ErrCodeConflict {
		t.Fatalf("active replace error = %v, want conflict", err)
	}
	if ojsErr.Details["existing_job_id"] != active.ID {
		t.Fatalf("active conflict details = %v", ojsErr.Details)
	}
	if _, exists := store.jobs[job.ID]; exists {
		t.Fatal("active conflict created replacement")
	}
}

func TestPushUniqueJobPropagatesClaimError(t *testing.T) {
	store := newUniquePushStore(nil)
	store.claimErr = errors.New("redis unavailable")
	backend := uniquePushBackend(store)

	job := &core.Job{ID: "job-new", Type: "task", Queue: "default", Unique: &core.UniquePolicy{}}
	if _, _, err := backend.pushUniqueJob(context.Background(), job, 1, false); err == nil {
		t.Fatal("expected claim error to propagate")
	}
}

func TestPushUniqueJobPassesStatesAndScheduledFlag(t *testing.T) {
	store := newUniquePushStore(&state.UniqueClaimResult{Outcome: state.UniqueClaimClaimed})
	backend := uniquePushBackend(store)

	job := &core.Job{
		ID:     "job-new",
		Type:   "task",
		Queue:  "default",
		Unique: &core.UniquePolicy{OnConflict: "reject", States: []string{core.StateAvailable, core.StateActive}},
	}
	if _, _, err := backend.pushUniqueJob(context.Background(), job, 1, true); err != nil {
		t.Fatalf("pushUniqueJob error = %v", err)
	}
	if !store.claimArgs.scheduled {
		t.Fatal("scheduled flag was not forwarded to the claim")
	}
	if len(store.claimArgs.relevantStates) != 2 {
		t.Fatalf("relevant states = %v, want two", store.claimArgs.relevantStates)
	}
}

type cancelFailureStore struct {
	state.Store
	job    *core.Job
	getErr error
	err    error
}

func (s *cancelFailureStore) GetJob(context.Context, string) (*core.Job, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	copyJob := *s.job
	return &copyJob, nil
}

func (s *cancelFailureStore) AtomicCancelJob(context.Context, string, string) (*state.JobCancelResult, error) {
	return nil, s.err
}

func TestCancelPropagatesAtomicStoreFailure(t *testing.T) {
	store := &cancelFailureStore{
		job: &core.Job{ID: "job-1", Type: "task", Queue: "default", State: core.StateAvailable},
		err: errors.New("redis wrongtype"),
	}
	backend := &KafkaBackend{store: store, producer: &Producer{}}
	cancelled, err := backend.Cancel(context.Background(), "job-1")
	if err == nil || cancelled != nil {
		t.Fatalf("Cancel() = %+v, %v; want atomic failure", cancelled, err)
	}
	if store.job.State != core.StateAvailable {
		t.Fatalf("job mutated despite cancellation failure: %+v", store.job)
	}
}

func TestCancelPropagatesGetStoreFailure(t *testing.T) {
	store := &cancelFailureStore{getErr: errors.New("redis unavailable")}
	backend := &KafkaBackend{store: store, producer: &Producer{}}
	cancelled, err := backend.Cancel(context.Background(), "job-1")
	if err == nil || cancelled != nil {
		t.Fatalf("Cancel() = %+v, %v; want store failure", cancelled, err)
	}
	var ojsErr *core.OJSError
	if errors.As(err, &ojsErr) && ojsErr.Code == core.ErrCodeNotFound {
		t.Fatalf("transient store failure was converted to not_found: %v", err)
	}
}
