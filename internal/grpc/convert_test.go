package grpc

import (
	"testing"
	"time"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"google.golang.org/protobuf/types/known/durationpb"

	ojsv1 "github.com/openjobspec/ojs-proto/gen/go/ojs/v1"
)

// TestJobToProtoParsesISO8601Durations verifies that jobToProto reads the
// core ISO-8601 duration strings (e.g. "PT2S") for retry intervals and unique
// period. The previous implementation parsed them with time.ParseDuration,
// which does not understand ISO-8601 and silently dropped the durations.
func TestJobToProtoParsesISO8601Durations(t *testing.T) {
	job := &core.Job{
		ID:    "job-1",
		Type:  "task.run",
		Queue: "default",
		State: core.StateAvailable,
		Retry: &core.RetryPolicy{
			MaxAttempts:        5,
			InitialInterval:    "PT2S",
			MaxInterval:        "PT1M30S",
			BackoffCoefficient: 2.5,
			Jitter:             true,
			NonRetryableErrors: []string{"fatal"},
			OnExhaustion:       "dead_letter",
		},
		Unique: &core.UniquePolicy{
			Keys:   []string{"type", "args"},
			Period: "PT10M",
		},
	}

	pj := jobToProto(job)

	if pj.RetryPolicy == nil {
		t.Fatal("retry policy was not converted")
	}
	if pj.RetryPolicy.InitialInterval == nil || pj.RetryPolicy.InitialInterval.AsDuration() != 2*time.Second {
		t.Fatalf("initial interval = %v, want 2s", pj.RetryPolicy.InitialInterval.AsDuration())
	}
	if pj.RetryPolicy.MaxInterval == nil || pj.RetryPolicy.MaxInterval.AsDuration() != 90*time.Second {
		t.Fatalf("max interval = %v, want 90s", pj.RetryPolicy.MaxInterval.AsDuration())
	}
	if pj.RetryPolicy.MaxAttempts != 5 || pj.RetryPolicy.BackoffCoefficient != 2.5 || !pj.RetryPolicy.Jitter {
		t.Fatalf("retry scalar fields = %+v", pj.RetryPolicy)
	}
	if pj.UniquePolicy == nil || pj.UniquePolicy.Period == nil || pj.UniquePolicy.Period.AsDuration() != 10*time.Minute {
		t.Fatalf("unique period = %v, want 10m", pj.UniquePolicy.GetPeriod().AsDuration())
	}
}

// TestRetryPolicyProtoRoundTrip verifies proto -> core -> proto preserves the
// retry intervals and unique period across the ISO-8601 boundary.
func TestRetryPolicyProtoRoundTrip(t *testing.T) {
	options := &ojsv1.EnqueueOptions{
		Retry: &ojsv1.RetryPolicy{
			MaxAttempts:        4,
			InitialInterval:    durationpb.New(3 * time.Second),
			MaxInterval:        durationpb.New(2 * time.Minute),
			BackoffCoefficient: 2,
			Jitter:             true,
		},
		Unique: &ojsv1.UniquePolicy{
			Key:    []string{"type"},
			Period: durationpb.New(15 * time.Minute),
		},
	}

	job := enqueueRequestToJob(&ojsv1.EnqueueRequest{Type: "task.run", Options: options})

	// Core representation uses ISO-8601 strings.
	if job.Retry == nil || job.Retry.InitialInterval != "PT3S" || job.Retry.MaxInterval != "PT2M" {
		t.Fatalf("core retry intervals = %+v", job.Retry)
	}
	if job.Unique == nil || job.Unique.Period != "PT15M" {
		t.Fatalf("core unique period = %+v", job.Unique)
	}

	back := jobToProto(job)
	if back.RetryPolicy.InitialInterval.AsDuration() != 3*time.Second {
		t.Fatalf("round-trip initial interval = %v", back.RetryPolicy.InitialInterval.AsDuration())
	}
	if back.RetryPolicy.MaxInterval.AsDuration() != 2*time.Minute {
		t.Fatalf("round-trip max interval = %v", back.RetryPolicy.MaxInterval.AsDuration())
	}
	if back.UniquePolicy.Period.AsDuration() != 15*time.Minute {
		t.Fatalf("round-trip unique period = %v", back.UniquePolicy.Period.AsDuration())
	}
}

// TestEnqueueScalarMaxAttemptsZeroFallsThroughToDefault documents the cross-repo
// contract limitation: proto3 scalar max_attempts has no field presence, so an
// explicit 0 is indistinguishable from an omitted value. Both leave the job's
// max attempts unset (the default policy applies). Disabling retries with zero
// requires the nested RetryPolicy, which is verified separately.
func TestEnqueueScalarMaxAttemptsZeroFallsThroughToDefault(t *testing.T) {
	job := enqueueRequestToJob(&ojsv1.EnqueueRequest{
		Type:    "task.run",
		Options: &ojsv1.EnqueueOptions{MaxAttempts: 0},
	})
	if job.MaxAttempts != nil {
		t.Fatalf("scalar max_attempts=0 set MaxAttempts=%v; proto3 scalar zero must be treated as omitted", *job.MaxAttempts)
	}
	if job.Retry != nil {
		t.Fatalf("scalar max_attempts=0 must not synthesize a retry policy, got %+v", job.Retry)
	}

	batchJob := enqueueJobRequestToJob(&ojsv1.BatchJobEntry{
		Type:    "task.run",
		Options: &ojsv1.EnqueueOptions{MaxAttempts: 0},
	})
	if batchJob.MaxAttempts != nil {
		t.Fatalf("batch scalar max_attempts=0 set MaxAttempts=%v", *batchJob.MaxAttempts)
	}

	// The nested retry policy carries presence, so an explicit zero disables retries.
	nested := enqueueRequestToJob(&ojsv1.EnqueueRequest{
		Type:    "task.run",
		Options: &ojsv1.EnqueueOptions{Retry: &ojsv1.RetryPolicy{MaxAttempts: 0}},
	})
	if nested.MaxAttempts == nil || *nested.MaxAttempts != 0 {
		t.Fatalf("nested RetryPolicy{max_attempts:0} must set MaxAttempts=0, got %v", nested.MaxAttempts)
	}
}

// TestWorkflowScalarMaxAttemptsZeroFallsThroughToDefault mirrors the enqueue
// path for workflow step conversion.
func TestWorkflowScalarMaxAttemptsZeroFallsThroughToDefault(t *testing.T) {
	converted, err := protoWorkflowOptionsToCore(&ojsv1.EnqueueOptions{MaxAttempts: 0}, time.Now())
	if err != nil {
		t.Fatalf("protoWorkflowOptionsToCore() error = %v", err)
	}
	if converted.Retry != nil {
		t.Fatalf("scalar max_attempts=0 must not synthesize a workflow retry policy, got %+v", converted.Retry)
	}

	nested, err := protoWorkflowOptionsToCore(&ojsv1.EnqueueOptions{Retry: &ojsv1.RetryPolicy{MaxAttempts: 0}}, time.Now())
	if err != nil {
		t.Fatalf("protoWorkflowOptionsToCore() nested error = %v", err)
	}
	if nested.Retry == nil || nested.Retry.MaxAttempts != 0 {
		t.Fatalf("nested workflow RetryPolicy{max_attempts:0} = %+v", nested.Retry)
	}
}
