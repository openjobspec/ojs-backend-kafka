package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ojsv1 "github.com/openjobspec/ojs-proto/gen/go/ojs/v1"
)

func TestConvertCreateWorkflowSingleStepPreservesOptions(t *testing.T) {
	delayUntil := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	meta, err := structpb.NewStruct(map[string]any{"tenant": "acme"})
	if err != nil {
		t.Fatalf("new meta: %v", err)
	}
	arg, err := structpb.NewValue(map[string]any{"source": "db"})
	if err != nil {
		t.Fatalf("new arg: %v", err)
	}
	before := time.Now()

	converted, err := convertCreateWorkflow(&ojsv1.CreateWorkflowRequest{
		Name: "pipeline",
		Steps: []*ojsv1.WorkflowStep{{
			Id:   "extract",
			Type: "data.extract",
			Args: []*structpb.Value{arg},
			Options: &ojsv1.EnqueueOptions{
				Queue:      "etl",
				Priority:   10,
				DelayUntil: timestamppb.New(delayUntil),
				Timeout:    durationpb.New(45 * time.Second),
				Retry: &ojsv1.RetryPolicy{
					MaxAttempts:        7,
					InitialInterval:    durationpb.New(2 * time.Second),
					BackoffCoefficient: 3,
					MaxInterval:        durationpb.New(time.Minute),
					Jitter:             true,
					NonRetryableErrors: []string{"fatal*"},
					OnExhaustion:       "dead_letter",
				},
				Ttl:               durationpb.New(30 * time.Minute),
				Tags:              []string{"critical", "etl"},
				TraceId:           "trace-123",
				Meta:              meta,
				MaxAttempts:       4,
				VisibilityTimeout: durationpb.New(90 * time.Second),
			},
		}},
	})
	after := time.Now()
	if err != nil {
		t.Fatalf("convertCreateWorkflow() error = %v", err)
	}
	if converted.Type != "chain" || len(converted.Steps) != 1 || len(converted.Jobs) != 0 {
		t.Fatalf("converted workflow = %+v, want one-step chain", converted)
	}

	step := converted.Steps[0]
	if step.Name != "extract" || step.Type != "data.extract" {
		t.Fatalf("step identity = %+v", step)
	}
	if string(step.Args) != `[{"source":"db"}]` {
		t.Fatalf("step args = %s", step.Args)
	}
	options := step.Options
	if options == nil {
		t.Fatal("step options were dropped")
	}
	if options.Queue != "etl" || options.Priority == nil || *options.Priority != 10 {
		t.Errorf("queue/priority = %q/%v", options.Queue, options.Priority)
	}
	if options.ScheduledAt != core.FormatTime(delayUntil) {
		t.Errorf("scheduled_at = %q, want %q", options.ScheduledAt, core.FormatTime(delayUntil))
	}
	if options.TimeoutMs == nil || *options.TimeoutMs != 45000 {
		t.Errorf("timeout_ms = %v, want 45000", options.TimeoutMs)
	}
	if options.Retry == nil ||
		options.Retry.MaxAttempts != 7 ||
		options.Retry.InitialInterval != "PT2S" ||
		options.Retry.MaxInterval != "PT1M" ||
		!reflect.DeepEqual(options.Retry.NonRetryableErrors, []string{"fatal*"}) ||
		options.Retry.OnExhaustion != "dead_letter" {
		t.Errorf("retry options were not preserved: %+v", options.Retry)
	}
	expiresAt, err := time.Parse(core.TimeFormat, options.ExpiresAt)
	if err != nil {
		t.Fatalf("parse expires_at %q: %v", options.ExpiresAt, err)
	}
	if expiresAt.Before(before.Add(30*time.Minute-time.Second)) || expiresAt.After(after.Add(30*time.Minute+time.Second)) {
		t.Errorf("expires_at %s is not based on the requested TTL", expiresAt)
	}
	if !reflect.DeepEqual(options.Tags, []string{"critical", "etl"}) {
		t.Errorf("tags = %v", options.Tags)
	}
	var convertedMeta map[string]any
	if err := json.Unmarshal(options.Metadata, &convertedMeta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if convertedMeta["tenant"] != "acme" || convertedMeta["trace_id"] != "trace-123" {
		t.Errorf("metadata = %v", convertedMeta)
	}
	if options.VisibilityTimeoutMs == nil || *options.VisibilityTimeoutMs != 90000 {
		t.Errorf("visibility_timeout_ms = %v, want 90000", options.VisibilityTimeoutMs)
	}
}

func TestConvertCreateWorkflowMaxAttemptsShorthandPreservesExactValue(t *testing.T) {
	for _, maxAttempts := range []int32{1, 7} {
		t.Run(fmt.Sprintf("max_attempts_%d", maxAttempts), func(t *testing.T) {
			converted, err := convertCreateWorkflow(&ojsv1.CreateWorkflowRequest{
				Name: "retry",
				Steps: []*ojsv1.WorkflowStep{{
					Id:   "step",
					Type: "task.run",
					Options: &ojsv1.EnqueueOptions{
						MaxAttempts: maxAttempts,
					},
				}},
			})
			if err != nil {
				t.Fatalf("convertCreateWorkflow() error = %v", err)
			}
			retry := converted.Steps[0].Options.Retry
			if retry == nil || retry.MaxAttempts != int(maxAttempts) {
				t.Fatalf("retry = %+v, want max_attempts %d", retry, maxAttempts)
			}
		})
	}
}

func TestConvertCreateWorkflowExplicitRetryTakesPrecedenceOverShorthand(t *testing.T) {
	for _, explicitMax := range []int32{0, 2} {
		t.Run(fmt.Sprintf("retry_max_attempts_%d", explicitMax), func(t *testing.T) {
			converted, err := convertCreateWorkflow(&ojsv1.CreateWorkflowRequest{
				Name: "retry",
				Steps: []*ojsv1.WorkflowStep{{
					Id:   "step",
					Type: "task.run",
					Options: &ojsv1.EnqueueOptions{
						MaxAttempts: 9,
						Retry:       &ojsv1.RetryPolicy{MaxAttempts: explicitMax},
					},
				}},
			})
			if err != nil {
				t.Fatalf("convertCreateWorkflow() error = %v", err)
			}
			retry := converted.Steps[0].Options.Retry
			if retry == nil || retry.MaxAttempts != int(explicitMax) {
				t.Fatalf("retry = %+v, want explicit max_attempts %d", retry, explicitMax)
			}
		})
	}
}

func TestProtoRetryPolicyPreservesExplicitZeroMaxAttempts(t *testing.T) {
	options := &ojsv1.EnqueueOptions{
		Retry: &ojsv1.RetryPolicy{MaxAttempts: 0},
	}

	job := enqueueRequestToJob(&ojsv1.EnqueueRequest{Type: "task.run", Options: options})
	if job.Retry == nil || job.Retry.MaxAttempts != 0 ||
		job.MaxAttempts == nil || *job.MaxAttempts != 0 {
		t.Fatalf("enqueue job retry mapping = Retry:%+v MaxAttempts:%v", job.Retry, job.MaxAttempts)
	}

	batchJob := enqueueJobRequestToJob(&ojsv1.BatchJobEntry{Type: "task.run", Options: options})
	if batchJob.Retry == nil || batchJob.Retry.MaxAttempts != 0 ||
		batchJob.MaxAttempts == nil || *batchJob.MaxAttempts != 0 {
		t.Fatalf("batch job retry mapping = Retry:%+v MaxAttempts:%v", batchJob.Retry, batchJob.MaxAttempts)
	}

	roundTrip := jobToProto(job)
	if roundTrip.RetryPolicy == nil || roundTrip.RetryPolicy.MaxAttempts != 0 {
		t.Fatalf("job proto retry mapping = %+v", roundTrip.RetryPolicy)
	}
}

func TestConvertCreateWorkflowGraphMapping(t *testing.T) {
	t.Run("independent steps become group jobs", func(t *testing.T) {
		converted, err := convertCreateWorkflow(&ojsv1.CreateWorkflowRequest{
			Name: "parallel",
			Steps: []*ojsv1.WorkflowStep{
				{Id: "a", Type: "task.a"},
				{Id: "b", Type: "task.b"},
			},
		})
		if err != nil {
			t.Fatalf("convert: %v", err)
		}
		if converted.Type != "group" || len(converted.Jobs) != 2 || converted.Jobs[0].Name != "a" || converted.Jobs[1].Name != "b" {
			t.Fatalf("converted = %+v", converted)
		}
	})

	t.Run("linear dependencies become ordered chain", func(t *testing.T) {
		converted, err := convertCreateWorkflow(&ojsv1.CreateWorkflowRequest{
			Name: "linear",
			Steps: []*ojsv1.WorkflowStep{
				{Id: "deploy", Type: "task.deploy", DependsOn: []string{"build"}},
				{Id: "build", Type: "task.build", DependsOn: []string{"test"}},
				{Id: "test", Type: "task.test"},
			},
		})
		if err != nil {
			t.Fatalf("convert: %v", err)
		}
		got := []string{converted.Steps[0].Name, converted.Steps[1].Name, converted.Steps[2].Name}
		if converted.Type != "chain" || !reflect.DeepEqual(got, []string{"test", "build", "deploy"}) {
			t.Fatalf("converted type/order = %q/%v", converted.Type, got)
		}
	})
}

func TestConvertCreateWorkflowRejectsInvalidOrUnsupportedGraphs(t *testing.T) {
	tests := []struct {
		name        string
		request     *ojsv1.CreateWorkflowRequest
		unsupported bool
	}{
		{
			name:    "empty",
			request: &ojsv1.CreateWorkflowRequest{Name: "empty"},
		},
		{
			name: "unknown dependency",
			request: &ojsv1.CreateWorkflowRequest{
				Name:  "unknown",
				Steps: []*ojsv1.WorkflowStep{{Id: "a", Type: "task.a", DependsOn: []string{"missing"}}},
			},
		},
		{
			name: "cycle",
			request: &ojsv1.CreateWorkflowRequest{
				Name: "cycle",
				Steps: []*ojsv1.WorkflowStep{
					{Id: "a", Type: "task.a", DependsOn: []string{"b"}},
					{Id: "b", Type: "task.b", DependsOn: []string{"a"}},
				},
			},
		},
		{
			name: "fork",
			request: &ojsv1.CreateWorkflowRequest{
				Name: "fork",
				Steps: []*ojsv1.WorkflowStep{
					{Id: "a", Type: "task.a"},
					{Id: "b", Type: "task.b", DependsOn: []string{"a"}},
					{Id: "c", Type: "task.c", DependsOn: []string{"a"}},
				},
			},
			unsupported: true,
		},
		{
			name: "unsupported unique selector",
			request: &ojsv1.CreateWorkflowRequest{
				Name: "unique",
				Steps: []*ojsv1.WorkflowStep{{
					Id:   "a",
					Type: "task.a",
					Options: &ojsv1.EnqueueOptions{
						Unique: &ojsv1.UniquePolicy{ArgsKeys: []string{"account_id"}},
					},
				}},
			},
			unsupported: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := convertCreateWorkflow(test.request)
			if err == nil {
				t.Fatal("expected conversion error")
			}
			if got := isUnsupportedWorkflowConversion(err); got != test.unsupported {
				t.Fatalf("isUnsupportedWorkflowConversion(%v) = %v, want %v", err, got, test.unsupported)
			}
		})
	}
}

func TestConvertCreateWorkflowRejectsUniqueStepsForChainAndGroup(t *testing.T) {
	tests := []struct {
		name  string
		steps []*ojsv1.WorkflowStep
	}{
		{
			name: "chain",
			steps: []*ojsv1.WorkflowStep{{
				Id:      "only",
				Type:    "task.only",
				Options: &ojsv1.EnqueueOptions{Unique: &ojsv1.UniquePolicy{Key: []string{"type"}}},
			}},
		},
		{
			name: "group",
			steps: []*ojsv1.WorkflowStep{
				{Id: "plain", Type: "task.plain"},
				{
					Id:      "unique",
					Type:    "task.unique",
					Options: &ojsv1.EnqueueOptions{Unique: &ojsv1.UniquePolicy{OnConflict: ojsv1.UniqueConflictAction_UNIQUE_CONFLICT_ACTION_IGNORE}},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted, err := convertCreateWorkflow(&ojsv1.CreateWorkflowRequest{
				Name:  test.name,
				Steps: test.steps,
			})
			if err == nil || converted != nil {
				t.Fatalf("conversion = %+v, %v; want precise uniqueness error", converted, err)
			}
			if isUnsupportedWorkflowConversion(err) {
				t.Fatalf("uniqueness error must be InvalidArgument, not Unimplemented: %v", err)
			}
		})
	}
}

type workflowBackendRecorder struct {
	core.Backend
	request *core.WorkflowRequest
	calls   int
}

func (b *workflowBackendRecorder) CreateWorkflow(_ context.Context, request *core.WorkflowRequest) (*core.Workflow, error) {
	b.calls++
	b.request = request
	return &core.Workflow{
		ID:        "0198a50d-4d2d-7000-8000-000000000001",
		Name:      request.Name,
		Type:      request.Type,
		State:     "running",
		CreatedAt: core.NowFormatted(),
	}, nil
}

func TestServerCreateWorkflowConvertsBeforeCallingBackend(t *testing.T) {
	backend := &workflowBackendRecorder{}
	server := New(backend)

	response, err := server.CreateWorkflow(context.Background(), &ojsv1.CreateWorkflowRequest{
		Name:  "single",
		Steps: []*ojsv1.WorkflowStep{{Id: "only", Type: "task.only"}},
	})
	if err != nil {
		t.Fatalf("CreateWorkflow() error = %v", err)
	}
	if backend.calls != 1 || backend.request == nil || backend.request.Type != "chain" || len(backend.request.Steps) != 1 {
		t.Fatalf("backend request = %+v, calls = %d", backend.request, backend.calls)
	}
	if response.Workflow == nil || response.Workflow.Id == "" {
		t.Fatalf("response = %+v", response)
	}
}

func TestServerCreateWorkflowReturnsPreciseGraphErrors(t *testing.T) {
	tests := []struct {
		name    string
		request *ojsv1.CreateWorkflowRequest
		code    codes.Code
	}{
		{
			name:    "invalid",
			request: &ojsv1.CreateWorkflowRequest{Name: "empty"},
			code:    codes.InvalidArgument,
		},
		{
			name: "unsupported",
			request: &ojsv1.CreateWorkflowRequest{
				Name: "fork",
				Steps: []*ojsv1.WorkflowStep{
					{Id: "a", Type: "task.a"},
					{Id: "b", Type: "task.b", DependsOn: []string{"a"}},
					{Id: "c", Type: "task.c", DependsOn: []string{"a"}},
				},
			},
			code: codes.Unimplemented,
		},
		{
			name: "workflow uniqueness",
			request: &ojsv1.CreateWorkflowRequest{
				Name: "unique",
				Steps: []*ojsv1.WorkflowStep{{
					Id:   "a",
					Type: "task.a",
					Options: &ojsv1.EnqueueOptions{
						Unique: &ojsv1.UniquePolicy{
							Key:        []string{"type"},
							OnConflict: ojsv1.UniqueConflictAction_UNIQUE_CONFLICT_ACTION_IGNORE,
						},
					},
				}},
			},
			code: codes.InvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &workflowBackendRecorder{}
			_, err := New(backend).CreateWorkflow(context.Background(), test.request)
			if status.Code(err) != test.code {
				t.Fatalf("status code = %s, want %s (err=%v)", status.Code(err), test.code, err)
			}
			if backend.calls != 0 {
				t.Fatalf("backend was called %d times", backend.calls)
			}
		})
	}
}
