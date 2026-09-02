package grpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

	if j.MaxAttempts != nil {
		pj.MaxAttempts = int32(*j.MaxAttempts)
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
			if s, err := structpb.NewStruct(meta); err == nil {
				pj.Meta = s
			}
		}
	}

	if j.Result != nil {
		var result map[string]any
		if err := json.Unmarshal(j.Result, &result); err == nil {
			if s, err := structpb.NewStruct(result); err == nil {
				pj.Result = s
			}
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
			NonRetryableErrors: append([]string(nil), j.Retry.NonRetryableErrors...),
			OnExhaustion:       j.Retry.OnExhaustion,
		}
		// Core stores intervals as ISO-8601 duration strings (e.g. "PT5S"), not
		// Go duration syntax, so they must be parsed with the ISO-8601 parser
		// before being emitted as protobuf durations.
		if j.Retry.InitialInterval != "" {
			if d, err := core.ParseISO8601Duration(j.Retry.InitialInterval); err == nil {
				pj.RetryPolicy.InitialInterval = durationpb.New(d)
			}
		}
		if j.Retry.MaxInterval != "" {
			if d, err := core.ParseISO8601Duration(j.Retry.MaxInterval); err == nil {
				pj.RetryPolicy.MaxInterval = durationpb.New(d)
			}
		}
	}

	if j.Unique != nil {
		pj.UniquePolicy = &ojsv1.UniquePolicy{
			Key: j.Unique.Keys,
		}
		if j.Unique.Period != "" {
			if d, err := core.ParseISO8601Duration(j.Unique.Period); err == nil {
				pj.UniquePolicy.Period = durationpb.New(d)
			}
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
		if data, err := json.Marshal(args); err != nil {
			slog.Warn("failed to marshal gRPC args", "error", err)
		} else {
			job.Args = data
		}
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
			if data, err := json.Marshal(opts.Meta.AsMap()); err != nil {
				slog.Warn("failed to marshal gRPC meta", "error", err)
			} else {
				job.Meta = data
			}
		}
		if opts.Retry != nil {
			// An explicit nested retry policy carries presence, so an explicit
			// max_attempts of 0 (disable retries) is preserved here.
			job.Retry = protoRetryToCore(opts.Retry)
			job.MaxAttempts = &job.Retry.MaxAttempts
		} else if opts.MaxAttempts > 0 {
			// Cross-repo contract limitation: EnqueueOptions.max_attempts is a
			// proto3 scalar with no presence, so an explicit 0 is
			// indistinguishable from an omitted value and both fall through to
			// the default policy. Callers that need to disable retries with a
			// zero must use the nested RetryPolicy above. See jobToProto and the
			// ojs-proto schema; this backend must not alter the shared proto.
			maxAttempts := int(opts.MaxAttempts)
			job.MaxAttempts = &maxAttempts
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
		if data, err := json.Marshal(args); err != nil {
			slog.Warn("failed to marshal gRPC args", "error", err)
		} else {
			job.Args = data
		}
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
			if data, err := json.Marshal(opts.Meta.AsMap()); err != nil {
				slog.Warn("failed to marshal gRPC meta", "error", err)
			} else {
				job.Meta = data
			}
		}
		if opts.Retry != nil {
			// Explicit nested retry policy preserves an explicit max_attempts=0.
			job.Retry = protoRetryToCore(opts.Retry)
			job.MaxAttempts = &job.Retry.MaxAttempts
		} else if opts.MaxAttempts > 0 {
			// Proto3 scalar max_attempts has no presence: an explicit 0 cannot be
			// distinguished from omitted, so it falls through to the default
			// policy. Use the nested RetryPolicy to disable retries with zero.
			maxAttempts := int(opts.MaxAttempts)
			job.MaxAttempts = &maxAttempts
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

// convertCreateWorkflow maps the dependency-oriented gRPC request to the
// chain/group workflow forms implemented by the backend.
func convertCreateWorkflow(req *ojsv1.CreateWorkflowRequest) (*core.WorkflowRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("workflow request is required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("workflow name is required")
	}
	if len(req.Steps) == 0 {
		return nil, fmt.Errorf("workflow must include at least one step")
	}

	now := time.Now()
	steps := make([]core.WorkflowJobRequest, len(req.Steps))
	dependencies := make(map[string][]string, len(req.Steps))
	inputOrder := make([]string, len(req.Steps))
	seen := make(map[string]struct{}, len(req.Steps))

	for i, step := range req.Steps {
		if step == nil {
			return nil, fmt.Errorf("workflow step %d is required", i)
		}
		if step.Id == "" {
			return nil, fmt.Errorf("workflow step %d id is required", i)
		}
		if step.Type == "" {
			return nil, fmt.Errorf("workflow step %q type is required", step.Id)
		}
		if _, exists := seen[step.Id]; exists {
			return nil, fmt.Errorf("workflow step id %q is duplicated", step.Id)
		}
		seen[step.Id] = struct{}{}
		inputOrder[i] = step.Id

		converted, err := protoToWorkflowStep(step, now)
		if err != nil {
			return nil, fmt.Errorf("workflow step %q: %w", step.Id, err)
		}
		steps[i] = converted

		depSeen := make(map[string]struct{}, len(step.DependsOn))
		for _, dependency := range step.DependsOn {
			if dependency == "" {
				return nil, fmt.Errorf("workflow step %q has an empty dependency", step.Id)
			}
			if dependency == step.Id {
				return nil, fmt.Errorf("workflow step %q cannot depend on itself", step.Id)
			}
			if _, duplicate := depSeen[dependency]; duplicate {
				return nil, fmt.Errorf("workflow step %q repeats dependency %q", step.Id, dependency)
			}
			depSeen[dependency] = struct{}{}
			dependencies[step.Id] = append(dependencies[step.Id], dependency)
		}
	}

	for stepID, deps := range dependencies {
		for _, dependency := range deps {
			if _, exists := seen[dependency]; !exists {
				return nil, fmt.Errorf("workflow step %q depends on unknown step %q", stepID, dependency)
			}
		}
	}

	order, independent, err := classifyWorkflowGraph(inputOrder, dependencies)
	if err != nil {
		return nil, err
	}

	stepsByID := make(map[string]core.WorkflowJobRequest, len(steps))
	for _, step := range steps {
		stepsByID[step.Name] = step
	}

	wfReq := &core.WorkflowRequest{Name: req.Name}
	if independent && len(steps) > 1 {
		wfReq.Type = "group"
		wfReq.Jobs = append(wfReq.Jobs, steps...)
		return wfReq, nil
	}

	wfReq.Type = "chain"
	for _, stepID := range order {
		wfReq.Steps = append(wfReq.Steps, stepsByID[stepID])
	}
	return wfReq, nil
}

// classifyWorkflowGraph validates the graph and returns a backend-compatible
// linear order. independent is true when every step can run immediately.
func classifyWorkflowGraph(inputOrder []string, dependencies map[string][]string) (order []string, independent bool, err error) {
	children := make(map[string][]string, len(inputOrder))
	indegree := make(map[string]int, len(inputOrder))
	independent = true
	for _, stepID := range inputOrder {
		indegree[stepID] = len(dependencies[stepID])
		if indegree[stepID] > 0 {
			independent = false
		}
		for _, dependency := range dependencies[stepID] {
			children[dependency] = append(children[dependency], stepID)
		}
	}

	queue := make([]string, 0, len(inputOrder))
	for _, stepID := range inputOrder {
		if indegree[stepID] == 0 {
			queue = append(queue, stepID)
		}
	}
	for len(queue) > 0 {
		stepID := queue[0]
		queue = queue[1:]
		order = append(order, stepID)
		for _, child := range children[stepID] {
			indegree[child]--
			if indegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	if len(order) != len(inputOrder) {
		return nil, false, fmt.Errorf("workflow dependency graph contains a cycle")
	}
	if independent {
		return append([]string(nil), inputOrder...), true, nil
	}

	var root string
	rootCount := 0
	for _, stepID := range inputOrder {
		if len(dependencies[stepID]) == 0 {
			root = stepID
			rootCount++
		}
		if len(dependencies[stepID]) > 1 || len(children[stepID]) > 1 {
			return nil, false, errUnsupportedWorkflowGraph
		}
	}
	if rootCount != 1 {
		return nil, false, errUnsupportedWorkflowGraph
	}

	linear := make([]string, 0, len(inputOrder))
	for stepID := root; stepID != ""; {
		linear = append(linear, stepID)
		next := children[stepID]
		if len(next) == 0 {
			break
		}
		stepID = next[0]
	}
	if len(linear) != len(inputOrder) {
		return nil, false, errUnsupportedWorkflowGraph
	}
	return linear, false, nil
}

type unsupportedWorkflowConversionError struct {
	message string
}

func (e *unsupportedWorkflowConversionError) Error() string {
	return e.message
}

func unsupportedWorkflowConversion(message string) error {
	return &unsupportedWorkflowConversionError{message: message}
}

func isUnsupportedWorkflowConversion(err error) bool {
	var unsupported *unsupportedWorkflowConversionError
	return errors.As(err, &unsupported)
}

var errUnsupportedWorkflowGraph = unsupportedWorkflowConversion(
	"workflow dependency graph cannot be represented: only independent steps or one linear chain are supported",
)

// protoToWorkflowStep converts a protobuf workflow step to a core.WorkflowJobRequest.
func protoToWorkflowStep(req *ojsv1.WorkflowStep, now time.Time) (core.WorkflowJobRequest, error) {
	wj := core.WorkflowJobRequest{
		Name: req.Id,
		Type: req.Type,
	}

	if len(req.Args) > 0 {
		args := valuesToInterface(req.Args)
		data, err := json.Marshal(args)
		if err != nil {
			return core.WorkflowJobRequest{}, fmt.Errorf("invalid args: %w", err)
		}
		wj.Args = data
	}

	options, err := protoWorkflowOptionsToCore(req.Options, now)
	if err != nil {
		return core.WorkflowJobRequest{}, err
	}
	wj.Options = options

	return wj, nil
}

func protoWorkflowOptionsToCore(options *ojsv1.EnqueueOptions, now time.Time) (*core.EnqueueOptions, error) {
	if options == nil {
		return nil, nil
	}

	converted := &core.EnqueueOptions{Queue: options.Queue}
	if options.Priority != 0 {
		priority := int(options.Priority)
		converted.Priority = &priority
	}
	if options.DelayUntil != nil {
		if err := options.DelayUntil.CheckValid(); err != nil {
			return nil, fmt.Errorf("invalid delay_until: %w", err)
		}
		converted.ScheduledAt = core.FormatTime(options.DelayUntil.AsTime())
	}
	if options.Timeout != nil {
		timeout, err := validProtoDuration(options.Timeout, "timeout", false)
		if err != nil {
			return nil, err
		}
		timeoutMs := int(timeout.Milliseconds())
		converted.TimeoutMs = &timeoutMs
	}
	if options.Retry != nil {
		if err := validateProtoRetry(options.Retry); err != nil {
			return nil, err
		}
		converted.Retry = protoRetryToCore(options.Retry)
	}
	if options.MaxAttempts < 0 {
		return nil, fmt.Errorf("max_attempts must not be negative")
	}
	if options.Retry == nil && options.MaxAttempts > 0 {
		// Proto3 scalar max_attempts has no presence, so an explicit 0 is
		// indistinguishable from omitted and both defer to the default policy.
		// A workflow step that must disable retries has to use the nested retry
		// policy above; this backend must not change the shared ojs-proto schema.
		retry := core.DefaultRetryPolicy()
		retry.MaxAttempts = int(options.MaxAttempts)
		converted.Retry = &retry
	}
	if options.Unique != nil {
		return nil, fmt.Errorf("workflow step uniqueness is not supported by this backend")
	}
	if options.Ttl != nil {
		ttl, err := validProtoDuration(options.Ttl, "ttl", true)
		if err != nil {
			return nil, err
		}
		converted.ExpiresAt = core.FormatTime(now.Add(ttl))
	}
	converted.Tags = append(converted.Tags, options.Tags...)

	meta := map[string]any{}
	if options.Meta != nil {
		meta = options.Meta.AsMap()
	}
	if options.TraceId != "" {
		meta["trace_id"] = options.TraceId
	}
	if len(meta) > 0 {
		data, err := json.Marshal(meta)
		if err != nil {
			return nil, fmt.Errorf("invalid meta: %w", err)
		}
		converted.Metadata = data
	}
	if options.VisibilityTimeout != nil {
		visibility, err := validProtoDuration(options.VisibilityTimeout, "visibility_timeout", true)
		if err != nil {
			return nil, err
		}
		visibilityMs := int(visibility.Milliseconds())
		converted.VisibilityTimeoutMs = &visibilityMs
	}

	return converted, nil
}

func validateProtoRetry(retry *ojsv1.RetryPolicy) error {
	if retry.MaxAttempts < 0 {
		return fmt.Errorf("retry.max_attempts must not be negative")
	}
	if retry.InitialInterval != nil {
		if _, err := validProtoDuration(retry.InitialInterval, "retry.initial_interval", true); err != nil {
			return err
		}
	}
	if retry.MaxInterval != nil {
		if _, err := validProtoDuration(retry.MaxInterval, "retry.max_interval", true); err != nil {
			return err
		}
	}
	return nil
}

func validProtoDuration(duration *durationpb.Duration, field string, requirePositive bool) (time.Duration, error) {
	if err := duration.CheckValid(); err != nil {
		return 0, fmt.Errorf("invalid %s: %w", field, err)
	}
	value := duration.AsDuration()
	if value < 0 || requirePositive && value == 0 {
		requirement := "must not be negative"
		if requirePositive {
			requirement = "must be positive"
		}
		return 0, fmt.Errorf("%s %s", field, requirement)
	}
	return value, nil
}

// protoRetryToCore converts a proto RetryPolicy to a core RetryPolicy.
func protoRetryToCore(r *ojsv1.RetryPolicy) *core.RetryPolicy {
	cr := &core.RetryPolicy{
		MaxAttempts:        int(r.MaxAttempts),
		BackoffCoefficient: r.BackoffCoefficient,
		Jitter:             r.Jitter,
		NonRetryableErrors: append([]string(nil), r.NonRetryableErrors...),
		OnExhaustion:       r.OnExhaustion,
	}
	if r.InitialInterval != nil {
		cr.InitialInterval = core.FormatISO8601Duration(r.InitialInterval.AsDuration())
	}
	if r.MaxInterval != nil {
		cr.MaxInterval = core.FormatISO8601Duration(r.MaxInterval.AsDuration())
	}
	return cr
}

// protoUniqueToCore converts a proto UniquePolicy to a core UniquePolicy.
func protoUniqueToCore(u *ojsv1.UniquePolicy) *core.UniquePolicy {
	cu := &core.UniquePolicy{
		Keys: append([]string(nil), u.Key...),
	}
	if u.Period != nil {
		cu.Period = core.FormatISO8601Duration(u.Period.AsDuration())
	}
	switch u.OnConflict {
	case ojsv1.UniqueConflictAction_UNIQUE_CONFLICT_ACTION_REJECT:
		cu.OnConflict = "reject"
	case ojsv1.UniqueConflictAction_UNIQUE_CONFLICT_ACTION_REPLACE:
		cu.OnConflict = "replace"
	case ojsv1.UniqueConflictAction_UNIQUE_CONFLICT_ACTION_IGNORE:
		cu.OnConflict = "ignore"
	}
	for _, state := range u.States {
		if stateName := protoJobStateToCore(state); stateName != "" {
			cu.States = append(cu.States, stateName)
		}
	}
	return cu
}

func protoJobStateToCore(state ojsv1.JobState) string {
	switch state {
	case ojsv1.JobState_JOB_STATE_SCHEDULED:
		return core.StateScheduled
	case ojsv1.JobState_JOB_STATE_AVAILABLE:
		return core.StateAvailable
	case ojsv1.JobState_JOB_STATE_PENDING:
		return core.StatePending
	case ojsv1.JobState_JOB_STATE_ACTIVE:
		return core.StateActive
	case ojsv1.JobState_JOB_STATE_COMPLETED:
		return core.StateCompleted
	case ojsv1.JobState_JOB_STATE_RETRYABLE:
		return core.StateRetryable
	case ojsv1.JobState_JOB_STATE_CANCELLED:
		return core.StateCancelled
	case ojsv1.JobState_JOB_STATE_DISCARDED:
		return core.StateDiscarded
	default:
		return ""
	}
}
