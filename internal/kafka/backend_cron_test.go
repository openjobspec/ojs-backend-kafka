package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

type cronMarker struct {
	status     string
	owner      string
	jobID      string
	leaseUntil int64
}

type cronTestStore struct {
	state.Store
	mu sync.Mutex

	now          func() time.Time
	cronData     []byte
	saveFailures int
	getJobErr    error
	locks        map[string]int64
	markers      map[int64]cronMarker
	jobs         map[string]*core.Job
	pushAttempts int
	pushCreates  int
}

func newCronTestStore(now func() time.Time, cronJob *core.CronJob) *cronTestStore {
	data, err := json.Marshal(cronJob)
	if err != nil {
		panic(err)
	}
	return &cronTestStore{
		now:      now,
		cronData: data,
		locks:    make(map[string]int64),
		markers:  make(map[int64]cronMarker),
		jobs:     make(map[string]*core.Job),
	}
}

func (s *cronTestStore) GetAllCronNames(context.Context) ([]string, error) {
	return []string{"scheduled"}, nil
}

func (s *cronTestStore) GetCron(context.Context, string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.cronData...), nil
}

// GetJob backs the ambiguous-push reconciliation: it returns the persisted job
// when present, a configurable transient error to model an unknown outcome, or
// a definitive not-found error otherwise.
func (s *cronTestStore) GetJob(_ context.Context, jobID string) (*core.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getJobErr != nil {
		return nil, s.getJobErr
	}
	if job, ok := s.jobs[jobID]; ok {
		return job, nil
	}
	return nil, core.NewNotFoundError("Job", jobID)
}

func (s *cronTestStore) SaveCron(_ context.Context, _ string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveFailures > 0 {
		s.saveFailures--
		return errors.New("cursor save failed")
	}
	s.cronData = append([]byte(nil), data...)
	return nil
}

func (s *cronTestStore) AtomicPushIfAbsent(_ context.Context, job *core.Job, _ float64, _ bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushAttempts++
	if _, exists := s.jobs[job.ID]; exists {
		return false, nil
	}
	copyJob := *job
	s.jobs[job.ID] = &copyJob
	s.pushCreates++
	return true, nil
}

func (s *cronTestStore) AcquireCronLock(_ context.Context, key string, ttlMs int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nowMs := s.now().UnixMilli()
	if expiresAt, exists := s.locks[key]; exists && expiresAt > nowMs {
		return false, nil
	}
	s.locks[key] = nowMs + ttlMs
	return true, nil
}

func (s *cronTestStore) ClaimCronOccurrence(_ context.Context, _ string, occurrenceMs int64, owner string, jobID string, nowMs int64, leaseMs int64) (*state.CronOccurrenceClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if marker, exists := s.markers[occurrenceMs]; exists {
		if marker.status == state.CronOccurrenceFired {
			return &state.CronOccurrenceClaim{Status: marker.status, JobID: marker.jobID}, nil
		}
		if marker.leaseUntil > nowMs {
			return &state.CronOccurrenceClaim{Status: state.CronOccurrenceBusy, JobID: marker.jobID}, nil
		}
		if _, exists := s.jobs[marker.jobID]; exists {
			marker.status = state.CronOccurrenceFired
			s.markers[occurrenceMs] = marker
			return &state.CronOccurrenceClaim{Status: marker.status, JobID: marker.jobID}, nil
		}
	}
	s.markers[occurrenceMs] = cronMarker{
		status:     state.CronOccurrenceAcquired,
		owner:      owner,
		jobID:      jobID,
		leaseUntil: nowMs + leaseMs,
	}
	return &state.CronOccurrenceClaim{Status: state.CronOccurrenceAcquired, JobID: jobID}, nil
}

func (s *cronTestStore) CompleteCronOccurrence(_ context.Context, _ string, occurrenceMs int64, owner string, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	marker := s.markers[occurrenceMs]
	if marker.owner != owner || marker.jobID != jobID {
		return errors.New("claim ownership mismatch")
	}
	marker.status = state.CronOccurrenceFired
	s.markers[occurrenceMs] = marker
	return nil
}

func (s *cronTestStore) ReleaseCronOccurrence(_ context.Context, _ string, occurrenceMs int64, owner string, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	marker := s.markers[occurrenceMs]
	if marker.jobID == jobID &&
		(marker.status == state.CronOccurrenceFired || marker.owner == owner) {
		delete(s.markers, occurrenceMs)
	}
	return nil
}

func TestFireCronJobsSuccessfulCursorSaveDeletesMarker(t *testing.T) {
	current := time.Date(2026, 8, 11, 7, 0, 30, 0, time.UTC)
	now := func() time.Time { return current }
	occurrence := time.Date(2026, 8, 11, 7, 0, 0, 0, time.UTC)
	store := newCronTestStore(now, &core.CronJob{
		Name:        "scheduled",
		Expression:  "* * * * *",
		Schedule:    "* * * * *",
		Enabled:     true,
		NextRunAt:   core.FormatTime(occurrence),
		JobTemplate: &core.CronJobTemplate{Type: "task.run"},
	})
	backend := &KafkaBackend{
		store: store,
		nowFn: now,
		pushJobFn: func(_ context.Context, job *core.Job) (*core.Job, error) {
			store.mu.Lock()
			store.jobs[job.ID] = job
			store.mu.Unlock()
			return job, nil
		},
	}

	if err := backend.FireCronJobs(context.Background()); err != nil {
		t.Fatalf("FireCronJobs() error = %v", err)
	}
	store.mu.Lock()
	_, markerExists := store.markers[occurrence.UnixMilli()]
	store.mu.Unlock()
	if markerExists {
		t.Fatal("successful cursor persistence left an occurrence marker")
	}
}

func TestFireCronJobsDoesNotDuplicateAfterCursorFailureAndLockExpiry(t *testing.T) {
	current := time.Date(2026, 8, 11, 12, 5, 30, 0, time.UTC)
	now := func() time.Time { return current }
	firstOccurrence := time.Date(2026, 8, 11, 12, 5, 0, 0, time.UTC)
	store := newCronTestStore(now, &core.CronJob{
		Name:          "scheduled",
		Expression:    "* * * * *",
		Schedule:      "* * * * *",
		OverlapPolicy: "allow",
		Enabled:       true,
		NextRunAt:     core.FormatTime(firstOccurrence),
		JobTemplate:   &core.CronJobTemplate{Type: "task.run"},
	})
	store.saveFailures = 1

	var pushed []*core.Job
	backend := &KafkaBackend{
		store: store,
		nowFn: now,
		pushJobFn: func(_ context.Context, job *core.Job) (*core.Job, error) {
			copyJob := *job
			pushed = append(pushed, &copyJob)
			store.mu.Lock()
			store.jobs[job.ID] = &copyJob
			store.mu.Unlock()
			return job, nil
		},
	}

	if err := backend.FireCronJobs(context.Background()); err == nil {
		t.Fatal("expected post-push cursor save failure")
	}
	if len(pushed) != 1 {
		t.Fatalf("pushes after first run = %d, want 1", len(pushed))
	}
	store.mu.Lock()
	retainedMarker, markerExists := store.markers[firstOccurrence.UnixMilli()]
	store.mu.Unlock()
	if !markerExists || retainedMarker.status != state.CronOccurrenceFired {
		t.Fatalf("failed cursor save marker = %+v, exists=%v", retainedMarker, markerExists)
	}

	current = current.Add(61 * time.Second)
	if err := backend.FireCronJobs(context.Background()); err != nil {
		t.Fatalf("cursor convergence run: %v", err)
	}
	if len(pushed) != 1 {
		t.Fatalf("old occurrence duplicated after lock expiry: pushes=%d", len(pushed))
	}

	var stored core.CronJob
	if err := json.Unmarshal(store.cronData, &stored); err != nil {
		t.Fatalf("unmarshal stored cron: %v", err)
	}
	// After reconciling the already-fired occurrence, the cursor advances to the
	// first schedule strictly after the current evaluation time (12:06:31), i.e.
	// 12:07:00, skipping the missed 12:06:00 occurrence rather than replaying it.
	convergedNext := current.Truncate(time.Minute).Add(time.Minute)
	if stored.LastRunAt != core.FormatTime(firstOccurrence) || stored.NextRunAt != core.FormatTime(convergedNext) {
		t.Fatalf("converged cursor = last %q next %q, want last %q next %q",
			stored.LastRunAt, stored.NextRunAt, core.FormatTime(firstOccurrence), core.FormatTime(convergedNext))
	}
	store.mu.Lock()
	_, markerExists = store.markers[firstOccurrence.UnixMilli()]
	store.mu.Unlock()
	if markerExists {
		t.Fatal("recovered cursor left the reconciled occurrence marker")
	}

	// Advancing past the future cursor fires the next occurrence exactly once
	// with a distinct job ID, confirming liveness after recovery.
	current = current.Add(61 * time.Second)
	if err := backend.FireCronJobs(context.Background()); err != nil {
		t.Fatalf("catch-up occurrence: %v", err)
	}
	if len(pushed) != 2 {
		t.Fatalf("missed catch-up occurrence: pushes=%d, want 2", len(pushed))
	}
	if pushed[0].ID == pushed[1].ID {
		t.Fatalf("distinct occurrences reused job ID %q", pushed[0].ID)
	}
}

func TestFireCronJobsPushFailureCanRetry(t *testing.T) {
	current := time.Date(2026, 8, 11, 8, 0, 30, 0, time.UTC)
	now := func() time.Time { return current }
	occurrence := time.Date(2026, 8, 11, 8, 0, 0, 0, time.UTC)
	store := newCronTestStore(now, &core.CronJob{
		Name:        "scheduled",
		Expression:  "* * * * *",
		Schedule:    "* * * * *",
		Enabled:     true,
		NextRunAt:   core.FormatTime(occurrence),
		JobTemplate: &core.CronJobTemplate{Type: "task.run"},
	})

	attempts := 0
	backend := &KafkaBackend{
		store: store,
		nowFn: now,
		pushJobFn: func(_ context.Context, job *core.Job) (*core.Job, error) {
			attempts++
			if attempts == 1 {
				return nil, errors.New("kafka push failed")
			}
			store.mu.Lock()
			store.jobs[job.ID] = job
			store.mu.Unlock()
			return job, nil
		},
	}

	if err := backend.FireCronJobs(context.Background()); err == nil {
		t.Fatal("expected first push failure")
	}
	current = current.Add(61 * time.Second)
	if err := backend.FireCronJobs(context.Background()); err != nil {
		t.Fatalf("retry run: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("push attempts = %d, want 2", attempts)
	}

	store.mu.Lock()
	_, markerExists := store.markers[occurrence.UnixMilli()]
	store.mu.Unlock()
	if markerExists {
		t.Fatal("successful retry left an occurrence marker")
	}
}

// TestFireCronJobsCatchesUpOnceAfterLongDowntime covers finding #3: after a long
// downtime the scheduler must fire at most one due occurrence and then set the
// cursor to the first schedule strictly after the current evaluation time,
// rather than replaying every missed occurrence one cycle at a time.
func TestFireCronJobsCatchesUpOnceAfterLongDowntime(t *testing.T) {
	firstOccurrence := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	current := time.Date(2026, 8, 11, 12, 30, 15, 0, time.UTC) // ~30 missed occurrences
	now := func() time.Time { return current }
	store := newCronTestStore(now, &core.CronJob{
		Name:        "scheduled",
		Expression:  "* * * * *",
		Schedule:    "* * * * *",
		Enabled:     true,
		NextRunAt:   core.FormatTime(firstOccurrence),
		JobTemplate: &core.CronJobTemplate{Type: "task.run"},
	})
	var pushed []*core.Job
	backend := &KafkaBackend{
		store: store,
		nowFn: now,
		pushJobFn: func(_ context.Context, job *core.Job) (*core.Job, error) {
			copyJob := *job
			pushed = append(pushed, &copyJob)
			store.mu.Lock()
			store.jobs[job.ID] = &copyJob
			store.mu.Unlock()
			return job, nil
		},
	}

	if err := backend.FireCronJobs(context.Background()); err != nil {
		t.Fatalf("FireCronJobs() error = %v", err)
	}
	if len(pushed) != 1 {
		t.Fatalf("long downtime fired %d occurrences, want exactly one catch-up", len(pushed))
	}

	var stored core.CronJob
	if err := json.Unmarshal(store.cronData, &stored); err != nil {
		t.Fatalf("unmarshal stored cron: %v", err)
	}
	if stored.LastRunAt != core.FormatTime(firstOccurrence) {
		t.Fatalf("last run = %q, want %q", stored.LastRunAt, core.FormatTime(firstOccurrence))
	}
	wantNext := time.Date(2026, 8, 11, 12, 31, 0, 0, time.UTC) // first strictly after now
	if stored.NextRunAt != core.FormatTime(wantNext) {
		t.Fatalf("next run = %q, want %q (first schedule strictly after now)", stored.NextRunAt, core.FormatTime(wantNext))
	}
	nextRun, err := time.Parse(core.TimeFormat, stored.NextRunAt)
	if err != nil {
		t.Fatalf("parse next run: %v", err)
	}
	if !nextRun.After(current) {
		t.Fatalf("cursor %s is not strictly after the evaluation time %s", nextRun, current)
	}

	// Re-running at the same time must not replay the intermediate occurrences.
	if err := backend.FireCronJobs(context.Background()); err != nil {
		t.Fatalf("second FireCronJobs() error = %v", err)
	}
	if len(pushed) != 1 {
		t.Fatalf("catch-up replayed missed occurrences: pushes=%d, want 1", len(pushed))
	}
}

func TestCronOccurrenceJobIDIsDeterministicValidUUIDv7(t *testing.T) {
	occurrence := time.Date(2026, 8, 11, 12, 34, 56, 789_000_000, time.UTC)
	first, err := cronOccurrenceJobID("nightly", occurrence)
	if err != nil {
		t.Fatalf("derive first ID: %v", err)
	}
	repeated, err := cronOccurrenceJobID("nightly", occurrence)
	if err != nil {
		t.Fatalf("derive repeated ID: %v", err)
	}
	nextOccurrence, err := cronOccurrenceJobID("nightly", occurrence.Add(time.Minute))
	if err != nil {
		t.Fatalf("derive next occurrence ID: %v", err)
	}
	otherName, err := cronOccurrenceJobID("hourly", occurrence)
	if err != nil {
		t.Fatalf("derive other name ID: %v", err)
	}

	if first != repeated {
		t.Fatalf("same occurrence IDs differ: %q != %q", first, repeated)
	}
	if !core.IsValidUUIDv7(first) {
		t.Fatalf("cron occurrence ID %q is not a valid UUIDv7", first)
	}
	if first == nextOccurrence || first == otherName || nextOccurrence == otherName {
		t.Fatalf("cron occurrence IDs are not unique: %q %q %q", first, nextOccurrence, otherName)
	}
	if first[14] != '7' {
		t.Fatalf("UUID version nibble = %q, want 7", first[14])
	}
}

func TestFireCronJobsDoesNotDuplicateAfterMarkerExpiresBeyond24Hours(t *testing.T) {
	occurrence := time.Date(2026, 8, 10, 6, 0, 0, 0, time.UTC)
	current := occurrence.Add(30 * time.Second)
	now := func() time.Time { return current }
	store := newCronTestStore(now, &core.CronJob{
		Name:        "scheduled",
		Expression:  "* * * * *",
		Schedule:    "* * * * *",
		Enabled:     true,
		NextRunAt:   core.FormatTime(occurrence),
		JobTemplate: &core.CronJobTemplate{Type: "task.run"},
	})
	store.saveFailures = 1
	backend := &KafkaBackend{store: store, producer: &Producer{}, nowFn: now}

	if err := backend.FireCronJobs(context.Background()); err == nil {
		t.Fatal("expected cursor persistence failure after the first durable create")
	}
	if store.pushAttempts != 1 || store.pushCreates != 1 || len(store.jobs) != 1 {
		t.Fatalf("first fire attempts=%d creates=%d jobs=%d", store.pushAttempts, store.pushCreates, len(store.jobs))
	}
	expectedID, err := cronOccurrenceJobID("scheduled", occurrence)
	if err != nil {
		t.Fatalf("derive expected ID: %v", err)
	}
	if _, exists := store.jobs[expectedID]; !exists {
		t.Fatalf("first fire used a non-deterministic ID: jobs=%v", store.jobs)
	}

	// Simulate a process outage longer than the marker's bounded 24-hour TTL:
	// Redis expires the marker while the failed cursor still points at the old
	// occurrence.
	current = current.Add(25 * time.Hour)
	store.mu.Lock()
	delete(store.markers, occurrence.UnixMilli())
	store.mu.Unlock()
	backend = &KafkaBackend{store: store, producer: &Producer{}, nowFn: now}

	if err := backend.FireCronJobs(context.Background()); err != nil {
		t.Fatalf("post-expiry reconciliation: %v", err)
	}
	if store.pushAttempts != 2 || store.pushCreates != 1 || len(store.jobs) != 1 {
		t.Fatalf("post-expiry attempts=%d creates=%d jobs=%d; existing job was reset or duplicated",
			store.pushAttempts, store.pushCreates, len(store.jobs))
	}
	persisted := store.jobs[expectedID]
	if persisted == nil || persisted.State != core.StateAvailable {
		t.Fatalf("existing occurrence job was rewritten: %+v", persisted)
	}
	var cronJob core.CronJob
	if err := json.Unmarshal(store.cronData, &cronJob); err != nil {
		t.Fatalf("unmarshal reconciled cron: %v", err)
	}
	nextRun, err := time.Parse(core.TimeFormat, cronJob.NextRunAt)
	if err != nil || !nextRun.After(current) {
		t.Fatalf("reconciled cursor = %q, %v; want strictly after %s", cronJob.NextRunAt, err, current)
	}
}

// TestFireCronJobsAmbiguousPushReconcilesPersistedJob covers finding #4: when a
// push returns an error but the job was actually persisted, the scheduler must
// reconcile by the preassigned job ID and finalize the occurrence instead of
// releasing the claim and creating a duplicate on a later cycle.
func TestFireCronJobsAmbiguousPushReconcilesPersistedJob(t *testing.T) {
	current := time.Date(2026, 8, 11, 9, 0, 30, 0, time.UTC)
	occurrence := time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC)
	now := func() time.Time { return current }
	store := newCronTestStore(now, &core.CronJob{
		Name:        "scheduled",
		Expression:  "* * * * *",
		Schedule:    "* * * * *",
		Enabled:     true,
		NextRunAt:   core.FormatTime(occurrence),
		JobTemplate: &core.CronJobTemplate{Type: "task.run"},
	})

	attempts := 0
	backend := &KafkaBackend{
		store: store,
		nowFn: now,
		pushJobFn: func(_ context.Context, job *core.Job) (*core.Job, error) {
			attempts++
			// Persist the job, then report an ambiguous error (e.g. the async
			// produce failed after the durable state write).
			store.mu.Lock()
			store.jobs[job.ID] = job
			store.mu.Unlock()
			return nil, errors.New("kafka produce failed after persist")
		},
	}

	if err := backend.FireCronJobs(context.Background()); err != nil {
		t.Fatalf("ambiguous push should reconcile to a completed fire, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("push attempts = %d, want 1", attempts)
	}
	store.mu.Lock()
	_, markerExists := store.markers[occurrence.UnixMilli()]
	store.mu.Unlock()
	if markerExists {
		t.Fatal("reconciled occurrence left a marker")
	}

	// A subsequent cycle must not create a duplicate for the same occurrence.
	if err := backend.FireCronJobs(context.Background()); err != nil {
		t.Fatalf("second FireCronJobs() error = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("reconciled occurrence pushed again: attempts=%d, want 1", attempts)
	}
}

// TestFireCronJobsUnknownPushOutcomeRetainsPendingMarker covers finding #4: when
// the push outcome cannot be proven (the reconcile lookup itself fails), the
// scheduler must retain the pending claim so lease recovery reconciles it,
// rather than releasing the claim and risking a duplicate.
func TestFireCronJobsUnknownPushOutcomeRetainsPendingMarker(t *testing.T) {
	current := time.Date(2026, 8, 11, 10, 0, 30, 0, time.UTC)
	occurrence := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	now := func() time.Time { return current }
	store := newCronTestStore(now, &core.CronJob{
		Name:        "scheduled",
		Expression:  "* * * * *",
		Schedule:    "* * * * *",
		Enabled:     true,
		NextRunAt:   core.FormatTime(occurrence),
		JobTemplate: &core.CronJobTemplate{Type: "task.run"},
	})

	attempts := 0
	backend := &KafkaBackend{
		store: store,
		nowFn: now,
		pushJobFn: func(_ context.Context, job *core.Job) (*core.Job, error) {
			attempts++
			// The job is durably persisted, but the push still reports an error.
			store.mu.Lock()
			store.jobs[job.ID] = job
			store.mu.Unlock()
			return nil, errors.New("kafka produce failed")
		},
	}
	// The reconcile lookup itself fails, so the outcome is unknown.
	store.mu.Lock()
	store.getJobErr = errors.New("redis unavailable")
	store.mu.Unlock()

	if err := backend.FireCronJobs(context.Background()); err == nil {
		t.Fatal("unknown push outcome should surface an error")
	}
	if attempts != 1 {
		t.Fatalf("push attempts = %d, want 1", attempts)
	}
	store.mu.Lock()
	marker, markerExists := store.markers[occurrence.UnixMilli()]
	store.mu.Unlock()
	if !markerExists || marker.status != state.CronOccurrenceAcquired {
		t.Fatalf("unknown outcome marker = %+v exists=%v; want a retained pending claim", marker, markerExists)
	}

	// Before the lease/lock expire, the occurrence must not be pushed again.
	if err := backend.FireCronJobs(context.Background()); err != nil {
		t.Fatalf("pre-expiry FireCronJobs() error = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("pre-expiry duplicate: attempts=%d, want 1", attempts)
	}

	// After the lease and lock expire, claim reconciliation detects the
	// persisted job and finalizes the occurrence without a duplicate push.
	current = current.Add(61 * time.Second)
	if err := backend.FireCronJobs(context.Background()); err != nil {
		t.Fatalf("lease-recovery FireCronJobs() error = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("lease recovery duplicated the push: attempts=%d, want 1", attempts)
	}
	store.mu.Lock()
	_, markerExists = store.markers[occurrence.UnixMilli()]
	store.mu.Unlock()
	if markerExists {
		t.Fatal("lease recovery left an occurrence marker")
	}
}
