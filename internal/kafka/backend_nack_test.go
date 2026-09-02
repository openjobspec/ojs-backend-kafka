package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/openjobspec/ojs-backend-kafka/internal/api"
	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

func boolPtr(b bool) *bool { return &b }

func TestBuildJobErrorJSON_Nil(t *testing.T) {
	if got := buildJobErrorJSON(&core.Job{}, nil); got != nil {
		t.Errorf("buildJobErrorJSON(nil) = %q, want nil", got)
	}
}

func TestBuildJobErrorJSON_Fields(t *testing.T) {
	job := &core.Job{Attempt: 2}
	jobErr := &core.JobError{
		Code:      "timeout",
		Message:   "deadline exceeded",
		Retryable: boolPtr(true),
		Details:   map[string]any{"host": "db-1"},
	}

	raw := buildJobErrorJSON(job, jobErr)

	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if obj["message"] != "deadline exceeded" {
		t.Errorf("message = %v", obj["message"])
	}
	// attempt is the job's current attempt (not the incremented one).
	if obj["attempt"] != float64(2) {
		t.Errorf("attempt = %v, want 2", obj["attempt"])
	}
	// With no explicit Type, Code is used as "type".
	if obj["type"] != "timeout" {
		t.Errorf("type = %v, want %q", obj["type"], "timeout")
	}
	if obj["retryable"] != true {
		t.Errorf("retryable = %v, want true", obj["retryable"])
	}
}

func TestBuildJobErrorJSON_TypeOverridesCode(t *testing.T) {
	raw := buildJobErrorJSON(&core.Job{}, &core.JobError{Code: "c", Type: "explicit"})

	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if obj["type"] != "explicit" {
		t.Errorf("type = %v, want %q (Type overrides Code)", obj["type"], "explicit")
	}
}

func TestResolveNonRetryable_NilError(t *testing.T) {
	if resolveNonRetryable(&core.Job{}, nil) {
		t.Error("nil error should be retryable")
	}
}

func TestResolveNonRetryable_ExplicitFlag(t *testing.T) {
	job := &core.Job{}
	if !resolveNonRetryable(job, &core.JobError{Retryable: boolPtr(false)}) {
		t.Error("Retryable=false must be non-retryable")
	}
	if resolveNonRetryable(job, &core.JobError{Retryable: boolPtr(true)}) {
		t.Error("Retryable=true must be retryable")
	}
}

func TestResolveNonRetryable_PatternMatchOnType(t *testing.T) {
	job := &core.Job{Retry: &core.RetryPolicy{NonRetryableErrors: []string{"auth*"}}}

	if !resolveNonRetryable(job, &core.JobError{Type: "auth_expired"}) {
		t.Error("type matching non-retryable pattern must be non-retryable")
	}
	if resolveNonRetryable(job, &core.JobError{Type: "network_timeout"}) {
		t.Error("type not matching any pattern must be retryable")
	}
}

func TestResolveNonRetryable_PatternMatchOnMessage(t *testing.T) {
	job := &core.Job{Retry: &core.RetryPolicy{NonRetryableErrors: []string{"fatal: *"}}}

	if !resolveNonRetryable(job, &core.JobError{Message: "fatal: disk full"}) {
		t.Error("message matching non-retryable pattern must be non-retryable")
	}
}

func TestResolveNonRetryable_NoRetryPolicy(t *testing.T) {
	if resolveNonRetryable(&core.Job{}, &core.JobError{Type: "auth_expired"}) {
		t.Error("without a retry policy, pattern errors are still retryable")
	}
}

type nackRequeueStore struct {
	state.Store
	job       *core.Job
	atomicErr error
	calls     int
	getCalls  int
}

func (s *nackRequeueStore) GetJob(context.Context, string) (*core.Job, error) {
	s.getCalls++
	if s.getCalls == 1 {
		return s.job, nil
	}
	updated := *s.job
	updated.State = core.StateAvailable
	updated.StartedAt = ""
	updated.WorkerID = ""
	return &updated, nil
}

func (s *nackRequeueStore) AtomicRequeue(_ context.Context, jobID string, queue string, enqueuedAt string, score float64) error {
	s.calls++
	if jobID != s.job.ID || queue != s.job.Queue || enqueuedAt == "" || score == 0 {
		return errors.New("unexpected atomic requeue arguments")
	}
	return s.atomicErr
}

func TestNackRequeueUsesAtomicTransition(t *testing.T) {
	store := &nackRequeueStore{
		job: &core.Job{
			ID:      "job-1",
			Type:    "task",
			Queue:   "critical",
			State:   core.StateActive,
			Attempt: 2,
		},
	}
	backend := &KafkaBackend{store: store}

	response, err := backend.Nack(context.Background(), "job-1", nil, true)
	if err != nil {
		t.Fatalf("Nack() error = %v", err)
	}
	if store.calls != 1 {
		t.Fatalf("AtomicRequeue calls = %d, want 1", store.calls)
	}
	if response.State != core.StateAvailable || response.Job == nil || response.Job.State != core.StateAvailable {
		t.Fatalf("response = %+v", response)
	}
}

func TestNackRequeueDoesNotReportAvailableOnStoreFailure(t *testing.T) {
	store := &nackRequeueStore{
		job: &core.Job{
			ID:    "job-1",
			Type:  "task",
			Queue: "critical",
			State: core.StateActive,
		},
		atomicErr: errors.New("redis write failed"),
	}
	backend := &KafkaBackend{store: store}

	response, err := backend.Nack(context.Background(), "job-1", nil, true)
	if err == nil {
		t.Fatal("expected requeue failure")
	}
	if response != nil {
		t.Fatalf("response = %+v, want nil", response)
	}
	if store.calls != 1 || store.getCalls != 1 {
		t.Fatalf("calls: atomic=%d get=%d", store.calls, store.getCalls)
	}
}

type nackPolicyStore struct {
	state.Store
	job          core.Job
	discardCalls int
	retryCalls   int
}

func (s *nackPolicyStore) GetJob(context.Context, string) (*core.Job, error) {
	job := s.job
	return &job, nil
}

func (s *nackPolicyStore) AtomicNackDiscard(_ context.Context, _ string, _ string, completedAt string, _ string, _ string, attempt string, _ bool, _ int64) error {
	s.discardCalls++
	s.job.State = core.StateDiscarded
	s.job.CompletedAt = completedAt
	s.job.Attempt, _ = strconv.Atoi(attempt)
	return nil
}

func (s *nackPolicyStore) AtomicNackRetry(_ context.Context, _ string, _ string, _ string, _ string, attempt string, _ string, _ int64) error {
	s.retryCalls++
	s.job.State = core.StateRetryable
	s.job.Attempt, _ = strconv.Atoi(attempt)
	return nil
}

func TestNackMaxAttemptsSemantics(t *testing.T) {
	zero := 0
	one := 1
	tests := []struct {
		name            string
		maxAttempts     *int
		wantState       string
		wantMaxAttempts int
		wantDiscard     int
		wantRetry       int
	}{
		{
			name:            "zero disables retries",
			maxAttempts:     &zero,
			wantState:       core.StateDiscarded,
			wantMaxAttempts: 0,
			wantDiscard:     1,
		},
		{
			name:            "one makes first failure terminal",
			maxAttempts:     &one,
			wantState:       core.StateDiscarded,
			wantMaxAttempts: 1,
			wantDiscard:     1,
		},
		{
			name:            "absent policy uses default",
			wantState:       core.StateRetryable,
			wantMaxAttempts: core.DefaultRetryPolicy().MaxAttempts,
			wantRetry:       1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &nackPolicyStore{
				job: core.Job{
					ID:          "job-1",
					Type:        "task.run",
					Queue:       "default",
					State:       core.StateActive,
					MaxAttempts: test.maxAttempts,
				},
			}
			backend := &KafkaBackend{store: store, producer: &Producer{}}

			response, err := backend.Nack(
				context.Background(),
				"job-1",
				&core.JobError{Message: "failed"},
				false,
			)
			if err != nil {
				t.Fatalf("Nack() error = %v", err)
			}
			if response.State != test.wantState || response.MaxAttempts != test.wantMaxAttempts {
				t.Fatalf("response = %+v, want state %q max_attempts %d", response, test.wantState, test.wantMaxAttempts)
			}
			if store.discardCalls != test.wantDiscard || store.retryCalls != test.wantRetry {
				t.Fatalf("transitions: discard=%d retry=%d", store.discardCalls, store.retryCalls)
			}
		})
	}
}

func TestJSONRetryPolicyPreservesExplicitZeroMaxAttempts(t *testing.T) {
	request, err := core.ParseEnqueueRequest([]byte(
		`{"type":"task.run","args":[],"options":{"retry":{"max_attempts":0}}}`,
	))
	if err != nil {
		t.Fatalf("ParseEnqueueRequest() error = %v", err)
	}

	job := api.RequestToJob(request)
	if job.Retry == nil || job.Retry.MaxAttempts != 0 ||
		job.MaxAttempts == nil || *job.MaxAttempts != 0 {
		t.Fatalf("JSON retry mapping = Retry:%+v MaxAttempts:%v", job.Retry, job.MaxAttempts)
	}
}
