package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

func TestBuildWorkflowStepJob_Defaults(t *testing.T) {
	step := core.WorkflowJobRequest{Type: "email.send", Args: json.RawMessage(`["hi"]`)}

	job := buildWorkflowStepJob(step, "wf-1", 2, nil)

	if job.Type != "email.send" {
		t.Errorf("Type = %q, want %q", job.Type, "email.send")
	}
	if job.Queue != "default" {
		t.Errorf("Queue = %q, want %q (default when unspecified)", job.Queue, "default")
	}
	if job.WorkflowID != "wf-1" {
		t.Errorf("WorkflowID = %q, want %q", job.WorkflowID, "wf-1")
	}
	if job.WorkflowStep != 2 {
		t.Errorf("WorkflowStep = %d, want 2", job.WorkflowStep)
	}
	if job.ParentResults != nil {
		t.Errorf("ParentResults = %v, want nil", job.ParentResults)
	}
	if job.Retry != nil || job.MaxAttempts != nil {
		t.Errorf("expected no retry policy, got Retry=%v MaxAttempts=%v", job.Retry, job.MaxAttempts)
	}
}

func TestBuildWorkflowStepJob_QueueOverride(t *testing.T) {
	step := core.WorkflowJobRequest{
		Type:    "report.build",
		Options: &core.EnqueueOptions{Queue: "reports"},
	}

	job := buildWorkflowStepJob(step, "wf-2", 0, nil)

	if job.Queue != "reports" {
		t.Errorf("Queue = %q, want %q", job.Queue, "reports")
	}
}

func TestBuildWorkflowStepJob_RetryPolicyPrecedence(t *testing.T) {
	// When both RetryPolicy and Retry are set, RetryPolicy wins (matches the
	// original inline branch ordering across chain/group/chained-step enqueues).
	step := core.WorkflowJobRequest{
		Type: "task",
		Options: &core.EnqueueOptions{
			RetryPolicy: &core.RetryPolicy{MaxAttempts: 7},
			Retry:       &core.RetryPolicy{MaxAttempts: 3},
		},
	}

	job := buildWorkflowStepJob(step, "wf-3", 1, nil)

	if job.Retry == nil || job.Retry.MaxAttempts != 7 {
		t.Fatalf("expected RetryPolicy (MaxAttempts 7) to take precedence, got %+v", job.Retry)
	}
	if job.MaxAttempts == nil || *job.MaxAttempts != 7 {
		t.Fatalf("MaxAttempts = %v, want 7", job.MaxAttempts)
	}
}

func TestBuildWorkflowStepJob_RetryFallback(t *testing.T) {
	step := core.WorkflowJobRequest{
		Type:    "task",
		Options: &core.EnqueueOptions{Retry: &core.RetryPolicy{MaxAttempts: 5}},
	}

	job := buildWorkflowStepJob(step, "wf-4", 0, nil)

	if job.MaxAttempts == nil || *job.MaxAttempts != 5 {
		t.Fatalf("MaxAttempts = %v, want 5 (from Retry fallback)", job.MaxAttempts)
	}
}

func TestBuildWorkflowStepJob_ParentResults(t *testing.T) {
	parents := []json.RawMessage{json.RawMessage(`{"a":1}`), json.RawMessage(`{"b":2}`)}

	job := buildWorkflowStepJob(core.WorkflowJobRequest{Type: "task"}, "wf-5", 3, parents)

	if len(job.ParentResults) != 2 {
		t.Fatalf("ParentResults len = %d, want 2", len(job.ParentResults))
	}
}

func TestBuildWorkflowStepJob_PreservesAllSupportedOptions(t *testing.T) {
	priority := 9
	timeoutMs := 45000
	visibilityMs := 90000
	retry := &core.RetryPolicy{MaxAttempts: 5}
	step := core.WorkflowJobRequest{
		Type: "task",
		Options: &core.EnqueueOptions{
			Queue:               "critical",
			Priority:            &priority,
			TimeoutMs:           &timeoutMs,
			ScheduledAt:         "2026-08-12T00:00:00.000Z",
			ExpiresAt:           "2026-08-13T00:00:00.000Z",
			Retry:               retry,
			Unique:              &core.UniquePolicy{Keys: []string{"type"}},
			Tags:                []string{"one", "two"},
			VisibilityTimeoutMs: &visibilityMs,
			Metadata:            json.RawMessage(`{"trace_id":"trace-1"}`),
			RateLimit:           &core.RateLimitPolicy{MaxPerSecond: 3},
		},
	}

	job := buildWorkflowStepJob(step, "wf-options", 0, nil)

	if job.Queue != "critical" || job.Priority == nil || *job.Priority != priority {
		t.Fatalf("queue/priority = %q/%v", job.Queue, job.Priority)
	}
	if job.TimeoutMs == nil || *job.TimeoutMs != timeoutMs ||
		job.VisibilityTimeoutMs == nil || *job.VisibilityTimeoutMs != visibilityMs {
		t.Errorf("timeouts were not preserved: timeout=%v visibility=%v", job.TimeoutMs, job.VisibilityTimeoutMs)
	}
	if job.ScheduledAt != step.Options.ScheduledAt || job.ExpiresAt != step.Options.ExpiresAt {
		t.Errorf("schedule/expiry = %q/%q", job.ScheduledAt, job.ExpiresAt)
	}
	if job.Retry != retry || job.MaxAttempts == nil || *job.MaxAttempts != 5 {
		t.Errorf("retry = %+v, max attempts = %v", job.Retry, job.MaxAttempts)
	}
	if job.Unique == nil || !reflect.DeepEqual(job.Tags, []string{"one", "two"}) ||
		string(job.Meta) != `{"trace_id":"trace-1"}` ||
		job.RateLimit == nil || job.RateLimit.MaxPerSecond != 3 {
		t.Errorf("options were not preserved: %+v", job)
	}
}

type workflowCreateStore struct {
	state.Store
	saveErr   error
	appendErr error
	saved     map[string]any
	jobIDs    []string
}

func (s *workflowCreateStore) SaveWorkflow(_ context.Context, _ string, data map[string]any) error {
	s.saved = data
	return s.saveErr
}

func (s *workflowCreateStore) AppendWorkflowJob(_ context.Context, _ string, jobID string) error {
	s.jobIDs = append(s.jobIDs, jobID)
	return s.appendErr
}

func TestCreateWorkflowEnqueuesRepresentedJobs(t *testing.T) {
	tests := []struct {
		name      string
		request   *core.WorkflowRequest
		wantCount int
	}{
		{
			name: "one step chain",
			request: &core.WorkflowRequest{
				Type:  "chain",
				Name:  "single",
				Steps: []core.WorkflowJobRequest{{Name: "only", Type: "task.only"}},
			},
			wantCount: 1,
		},
		{
			name: "parallel group",
			request: &core.WorkflowRequest{
				Type: "group",
				Name: "parallel",
				Jobs: []core.WorkflowJobRequest{
					{Name: "a", Type: "task.a"},
					{Name: "b", Type: "task.b"},
				},
			},
			wantCount: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &workflowCreateStore{}
			var pushed []*core.Job
			backend := &KafkaBackend{
				store: store,
				pushJobFn: func(_ context.Context, job *core.Job) (*core.Job, error) {
					job.ID = "job-" + job.Type
					pushed = append(pushed, job)
					return job, nil
				},
			}

			workflow, err := backend.CreateWorkflow(context.Background(), test.request)
			if err != nil {
				t.Fatalf("CreateWorkflow() error = %v", err)
			}
			if workflow.State != "running" || len(pushed) != test.wantCount || len(store.jobIDs) != test.wantCount {
				t.Fatalf("workflow=%+v pushed=%d appended=%d", workflow, len(pushed), len(store.jobIDs))
			}
			if store.saved == nil || store.saved["total"] != strconv.Itoa(test.wantCount) {
				t.Fatalf("saved workflow = %+v", store.saved)
			}
		})
	}
}

func TestCreateWorkflowRejectsSuccessfulNoOp(t *testing.T) {
	tests := []*core.WorkflowRequest{
		nil,
		{Type: ""},
		{Type: "chain"},
		{Type: "group"},
		{Type: "batch"},
	}

	for i, request := range tests {
		store := &workflowCreateStore{}
		backend := &KafkaBackend{
			store: store,
			pushJobFn: func(context.Context, *core.Job) (*core.Job, error) {
				t.Fatal("push must not be called")
				return nil, nil
			},
		}
		if workflow, err := backend.CreateWorkflow(context.Background(), request); err == nil || workflow != nil {
			t.Fatalf("case %d returned workflow=%+v err=%v", i, workflow, err)
		}
		if store.saved != nil {
			t.Fatalf("case %d saved a workflow", i)
		}
	}
}

func TestCreateWorkflowRejectsUniqueJobs(t *testing.T) {
	uniqueOptions := &core.EnqueueOptions{
		Unique: &core.UniquePolicy{OnConflict: "ignore"},
	}
	tests := []struct {
		name    string
		request *core.WorkflowRequest
	}{
		{
			name: "chain",
			request: &core.WorkflowRequest{
				Type:  "chain",
				Steps: []core.WorkflowJobRequest{{Type: "task", Options: uniqueOptions}},
			},
		},
		{
			name: "group",
			request: &core.WorkflowRequest{
				Type: "group",
				Jobs: []core.WorkflowJobRequest{{Type: "task", Options: uniqueOptions}},
			},
		},
		{
			name: "batch",
			request: &core.WorkflowRequest{
				Type: "batch",
				Jobs: []core.WorkflowJobRequest{{Type: "task", Options: uniqueOptions}},
			},
		},
		{
			name: "batch callback",
			request: &core.WorkflowRequest{
				Type: "batch",
				Jobs: []core.WorkflowJobRequest{{Type: "task"}},
				Callbacks: &core.WorkflowCallbacks{
					OnComplete: &core.WorkflowCallback{Type: "callback", Options: uniqueOptions},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &workflowCreateStore{}
			backend := &KafkaBackend{
				store: store,
				pushJobFn: func(context.Context, *core.Job) (*core.Job, error) {
					t.Fatal("unique workflow validation must run before enqueue")
					return nil, nil
				},
			}
			workflow, err := backend.CreateWorkflow(context.Background(), test.request)
			if err == nil || workflow != nil {
				t.Fatalf("CreateWorkflow() = %+v, %v; want validation error", workflow, err)
			}
			var ojsErr *core.OJSError
			if !errors.As(err, &ojsErr) || ojsErr.Code != core.ErrCodeInvalidRequest {
				t.Fatalf("error = %v, want invalid_request", err)
			}
			if store.saved != nil {
				t.Fatalf("invalid workflow was persisted: %v", store.saved)
			}
		})
	}
}

func TestCreateWorkflowPropagatesStoreFailures(t *testing.T) {
	request := &core.WorkflowRequest{
		Type:  "chain",
		Steps: []core.WorkflowJobRequest{{Type: "task"}},
	}

	t.Run("save", func(t *testing.T) {
		store := &workflowCreateStore{saveErr: errors.New("save failed")}
		backend := &KafkaBackend{
			store: store,
			pushJobFn: func(context.Context, *core.Job) (*core.Job, error) {
				t.Fatal("push must not follow a failed save")
				return nil, nil
			},
		}
		if _, err := backend.CreateWorkflow(context.Background(), request); err == nil {
			t.Fatal("expected save error")
		}
	})

	t.Run("append", func(t *testing.T) {
		store := &workflowCreateStore{appendErr: errors.New("append failed")}
		backend := &KafkaBackend{
			store: store,
			pushJobFn: func(_ context.Context, job *core.Job) (*core.Job, error) {
				job.ID = "job-1"
				return job, nil
			},
		}
		if _, err := backend.CreateWorkflow(context.Background(), request); err == nil {
			t.Fatal("expected append error")
		}
	})
}
