package grpc

import (
	"encoding/json"
	"time"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	ojsv1 "github.com/openjobspec/ojs-proto/gen/go/ojs/v1"
)

// stateToProto maps core state strings to proto enum values.
var stateToProto = map[string]ojsv1.JobState{
	"scheduled": ojsv1.JobState_JOB_STATE_SCHEDULED,
	"available": ojsv1.JobState_JOB_STATE_AVAILABLE,
	"pending":   ojsv1.JobState_JOB_STATE_PENDING,
	"active":    ojsv1.JobState_JOB_STATE_ACTIVE,
	"completed": ojsv1.JobState_JOB_STATE_COMPLETED,
	"retryable": ojsv1.JobState_JOB_STATE_RETRYABLE,
	"cancelled": ojsv1.JobState_JOB_STATE_CANCELLED,
	"discarded": ojsv1.JobState_JOB_STATE_DISCARDED,
}

// jobToProto converts a core.Job to its protobuf representation.
func jobToProto(j *core.Job) *ojsv1.Job {
	if j == nil {
		return nil
	}

	pj := &ojsv1.Job{
		Id:      j.ID,
		Type:    j.Type,
		Queue:   j.Queue,
		State:   stateToProto[j.State],
		Attempt: int32(j.Attempt),
	}

	if j.Priority != nil {
		pj.Priority = int32(*j.Priority)
	}

	if j.Args != nil {
		var args []any
		if err := json.Unmarshal(j.Args, &args); err == nil {
			for _, a := range args {
				if v, err := structpb.NewValue(a); err == nil {
					pj.Args = append(pj.Args, v)
				}
			}
		}
	}

	if j.Meta != nil {
		var meta map[string]any
		if err := json.Unmarshal(j.Meta, &meta); err == nil {
			pj.Meta, _ = structpb.NewStruct(meta)
		}
	}

	if j.Result != nil {
		var result map[string]any
		if err := json.Unmarshal(j.Result, &result); err == nil {
			pj.Result, _ = structpb.NewStruct(result)
		}
	}

	pj.CreatedAt = parseRFC3339(j.CreatedAt)
	pj.EnqueuedAt = parseRFC3339(j.EnqueuedAt)
	pj.ScheduledAt = parseRFC3339(j.ScheduledAt)
	pj.StartedAt = parseRFC3339(j.StartedAt)
	pj.CompletedAt = parseRFC3339(j.CompletedAt)

	if j.Retry != nil {
		pj.RetryPolicy = &ojsv1.RetryPolicy{
			MaxAttempts:        int32(j.Retry.MaxAttempts),
			BackoffCoefficient: j.Retry.BackoffCoefficient,
			Jitter:             j.Retry.Jitter,
		}
		if d, err := time.ParseDuration(j.Retry.InitialInterval); err == nil {
			pj.RetryPolicy.InitialInterval = durationpb.New(d)
		}
		if d, err := time.ParseDuration(j.Retry.MaxInterval); err == nil {
			pj.RetryPolicy.MaxInterval = durationpb.New(d)
		}
	}

	if j.Unique != nil {
		pj.UniquePolicy = &ojsv1.UniquePolicy{
			Key: j.Unique.Keys,
		}
		if d, err := time.ParseDuration(j.Unique.Period); err == nil {
			pj.UniquePolicy.Period = durationpb.New(d)
		}
	}

	return pj
}

// enqueueRequestToJob converts an EnqueueRequest to a core.Job.
func enqueueRequestToJob(req *ojsv1.EnqueueRequest) *core.Job {
	job := &core.Job{
		Type: req.Type,
	}

	if len(req.Args) > 0 {
		args := valuesToInterface(req.Args)
		job.Args, _ = json.Marshal(args)
	}

	if opts := req.Options; opts != nil {
		if opts.Queue != "" {
			job.Queue = opts.Queue
		}
		if opts.Priority != 0 {
			p := int(opts.Priority)
			job.Priority = &p
		}
		if opts.Meta != nil {
			job.Meta, _ = json.Marshal(opts.Meta.AsMap())
		}
		if opts.Retry != nil {
			job.Retry = protoRetryToCore(opts.Retry)
		}
		if opts.Unique != nil {
			job.Unique = protoUniqueToCore(opts.Unique)
		}
		if opts.DelayUntil != nil {
			job.ScheduledAt = opts.DelayUntil.AsTime().UTC().Format(time.RFC3339)
		}
	}

	return job
}

// enqueueJobRequestToJob converts a batch job entry to a core.Job.
func enqueueJobRequestToJob(req *ojsv1.BatchJobEntry) *core.Job {
	job := &core.Job{
		Type: req.Type,
	}

	if len(req.Args) > 0 {
		args := valuesToInterface(req.Args)
		job.Args, _ = json.Marshal(args)
	}

	if opts := req.Options; opts != nil {
		if opts.Queue != "" {
			job.Queue = opts.Queue
		}
		if opts.Priority != 0 {
			p := int(opts.Priority)
			job.Priority = &p
		}
		if opts.Meta != nil {
			job.Meta, _ = json.Marshal(opts.Meta.AsMap())
		}
	}

	return job
}

// stateToWorkflowProto maps core workflow state strings to proto enum values.
var stateToWorkflowProto = map[string]ojsv1.WorkflowState{
	"running":   ojsv1.WorkflowState_WORKFLOW_STATE_RUNNING,
	"completed": ojsv1.WorkflowState_WORKFLOW_STATE_COMPLETED,
	"failed":    ojsv1.WorkflowState_WORKFLOW_STATE_FAILED,
	"cancelled": ojsv1.WorkflowState_WORKFLOW_STATE_CANCELLED,
}

// workflowToProto converts a core.Workflow to its protobuf representation.
func workflowToProto(wf *core.Workflow) *ojsv1.Workflow {
	if wf == nil {
		return nil
	}

	pw := &ojsv1.Workflow{
		Id:        wf.ID,
		Name:      wf.Name,
		State:     stateToWorkflowProto[wf.State],
		CreatedAt: parseRFC3339(wf.CreatedAt),
	}

	if wf.CompletedAt != "" {
		pw.CompletedAt = parseRFC3339(wf.CompletedAt)
	}

	return pw
}

// protoToWorkflowRequest converts a CreateWorkflowRequest to a core.WorkflowRequest.
func protoToWorkflowRequest(req *ojsv1.CreateWorkflowRequest) *core.WorkflowRequest {
	wfReq := &core.WorkflowRequest{
		Name: req.Name,
	}

	for _, s := range req.Steps {
		wfReq.Steps = append(wfReq.Steps, protoToWorkflowStep(s))
	}

	return wfReq
}

// protoToWorkflowStep converts a protobuf workflow step to a core.WorkflowJobRequest.
func protoToWorkflowStep(req *ojsv1.WorkflowStep) core.WorkflowJobRequest {
	wj := core.WorkflowJobRequest{
		Name: req.Id,
		Type: req.Type,
	}

	if len(req.Args) > 0 {
		args := valuesToInterface(req.Args)
		wj.Args, _ = json.Marshal(args)
	}

	return wj
}

// protoRetryToCore converts a proto RetryPolicy to a core RetryPolicy.
func protoRetryToCore(r *ojsv1.RetryPolicy) *core.RetryPolicy {
	cr := &core.RetryPolicy{
		MaxAttempts:        int(r.MaxAttempts),
		BackoffCoefficient: r.BackoffCoefficient,
		Jitter:             r.Jitter,
	}
	if r.InitialInterval != nil {
		cr.InitialInterval = r.InitialInterval.AsDuration().String()
	}
	if r.MaxInterval != nil {
		cr.MaxInterval = r.MaxInterval.AsDuration().String()
	}
	return cr
}

// protoUniqueToCore converts a proto UniquePolicy to a core UniquePolicy.
func protoUniqueToCore(u *ojsv1.UniquePolicy) *core.UniquePolicy {
	cu := &core.UniquePolicy{
		Keys: u.Key,
	}
	if u.Period != nil {
		cu.Period = u.Period.AsDuration().String()
	}
	return cu
}
