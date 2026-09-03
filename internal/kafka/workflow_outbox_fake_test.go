package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-backend-kafka/internal/state"
)

// fakeWorkflowStore is an in-memory Store that faithfully mirrors the workflow
// outbox Lua scripts (advance, claim, complete, release) so the backend's
// dispatch logic can be exercised without a live Redis. It also tracks job
// lifecycle for the ACK/NACK recovery paths.
type fakeWorkflowStore struct {
	state.Store
	mu sync.Mutex

	workflows map[string]map[string]string // id -> hash
	results   map[string]map[string]string // id -> step -> result
	advanced  map[string]map[string]bool   // id -> jobID -> applied
	effects   map[string]map[string]string // id -> effectID -> status|... value
	pending   map[string]bool              // id -> has effects in the outbox
	jobsList  map[string][]string          // id -> appended job IDs
	jobs      map[string]*core.Job         // jobID -> job
	created   []*core.Job                  // jobs created by effect dispatch
	createErr map[string]error             // job type -> injected creation error
	// createAfterPersist makes the corresponding injected error ambiguous by
	// persisting the job before returning it once.
	createAfterPersist map[string]bool
	createAttempts     map[string]int

	// fault injection for the advance transition.
	advanceErr  error
	advanceErrN int
	advanceObs  int
	advanceHook func()
}

func newFakeWorkflowStore() *fakeWorkflowStore {
	return &fakeWorkflowStore{
		workflows:          make(map[string]map[string]string),
		results:            make(map[string]map[string]string),
		advanced:           make(map[string]map[string]bool),
		effects:            make(map[string]map[string]string),
		pending:            make(map[string]bool),
		jobsList:           make(map[string][]string),
		jobs:               make(map[string]*core.Job),
		createErr:          make(map[string]error),
		createAfterPersist: make(map[string]bool),
		createAttempts:     make(map[string]int),
	}
}

func (s *fakeWorkflowStore) putWorkflow(id string, hash map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workflows[id] = hash
}

func (s *fakeWorkflowStore) putJob(job *core.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *job
	s.jobs[job.ID] = &copy
}

func (s *fakeWorkflowStore) SaveJob(_ context.Context, job *core.Job) error {
	s.putJob(job)
	return nil
}

func (s *fakeWorkflowStore) GetJob(_ context.Context, jobID string) (*core.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.jobs[jobID]; ok {
		copy := *job
		return &copy, nil
	}
	return nil, core.NewNotFoundError("Job", jobID)
}

func (s *fakeWorkflowStore) SaveWorkflow(_ context.Context, id string, data map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hash := make(map[string]string, len(data))
	for k, v := range data {
		hash[k] = fmt.Sprint(v)
	}
	s.workflows[id] = hash
	return nil
}

func (s *fakeWorkflowStore) GetWorkflow(_ context.Context, id string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wf, ok := s.workflows[id]
	if !ok {
		return nil, core.NewNotFoundError("Workflow", id)
	}
	out := make(map[string]string, len(wf))
	for k, v := range wf {
		out[k] = v
	}
	return out, nil
}

func (s *fakeWorkflowStore) UpdateWorkflow(_ context.Context, id string, updates map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	wf := s.workflows[id]
	if wf == nil {
		wf = map[string]string{}
		s.workflows[id] = wf
	}
	for k, v := range updates {
		wf[k] = fmt.Sprint(v)
	}
	return nil
}

func (s *fakeWorkflowStore) GetWorkflowResults(_ context.Context, id string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for k, v := range s.results[id] {
		out[k] = v
	}
	return out, nil
}

func (s *fakeWorkflowStore) AppendWorkflowJob(_ context.Context, id string, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobsList[id] = append(s.jobsList[id], jobID)
	return nil
}

func (s *fakeWorkflowStore) GetWorkflowJobs(_ context.Context, id string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.jobsList[id]...), nil
}

func (s *fakeWorkflowStore) AtomicCancelWorkflow(_ context.Context, id string, completedAt string) (*state.WorkflowCancelResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wf := s.workflows[id]
	if wf == nil {
		return &state.WorkflowCancelResult{State: "missing"}, nil
	}
	if wf["state"] != "running" && wf["state"] != "cancelled" {
		return &state.WorkflowCancelResult{State: wf["state"]}, nil
	}

	seen := map[string]bool{}
	var effectJobIDs []string
	addID := func(jobID string) {
		if jobID != "" && !seen[jobID] {
			seen[jobID] = true
			effectJobIDs = append(effectJobIDs, jobID)
		}
	}
	for _, jobID := range strings.Split(wf["cancel_effect_job_ids"], ",") {
		addID(jobID)
	}
	for _, value := range s.effects[id] {
		parts := strings.Split(value, "|")
		if len(parts) >= 4 && parts[0] == "active" {
			addID(parts[3])
		} else if len(parts) >= 2 {
			addID(parts[1])
		}
	}

	applied := wf["state"] == "running"
	if applied {
		wf["state"] = "cancelled"
		wf["completed_at"] = completedAt
	}
	if len(effectJobIDs) > 0 {
		wf["cancel_effect_job_ids"] = strings.Join(effectJobIDs, ",")
	}
	delete(s.effects, id)
	delete(s.pending, id)
	return &state.WorkflowCancelResult{
		Applied:      applied,
		State:        "cancelled",
		EffectJobIDs: effectJobIDs,
	}, nil
}

func (s *fakeWorkflowStore) AtomicAdvanceWorkflow(ctx context.Context, input state.WorkflowAdvanceInput) (*state.WorkflowAdvanceResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanceObs++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.advanceErrN > 0 {
		s.advanceErrN--
		return nil, s.advanceErr
	}

	wf := s.workflows[input.WorkflowID]
	if wf == nil {
		return nil, errors.New("workflow is missing")
	}
	wtype := wf["type"]
	wstate := wf["state"]
	total, _ := strconv.Atoi(wf["total"])
	completed, _ := strconv.Atoi(wf["completed"])
	failedCount, _ := strconv.Atoi(wf["failed"])

	pendingFlag := func() bool { return len(s.effects[input.WorkflowID]) > 0 }

	if wstate != "running" {
		return &state.WorkflowAdvanceResult{
			WorkflowType: wtype, State: wstate, Completed: completed, Failed: failedCount,
			Total: total, NextStep: -1, HasPendingEffects: pendingFlag(),
		}, nil
	}
	adv := s.advanced[input.WorkflowID]
	if adv == nil {
		adv = map[string]bool{}
		s.advanced[input.WorkflowID] = adv
	}
	if adv[input.JobID] {
		return &state.WorkflowAdvanceResult{
			WorkflowType: wtype, State: wstate, Completed: completed, Failed: failedCount,
			Total: total, NextStep: -1, HasPendingEffects: pendingFlag(),
		}, nil
	}
	adv[input.JobID] = true

	if len(input.Result) > 0 {
		if s.results[input.WorkflowID] == nil {
			s.results[input.WorkflowID] = map[string]string{}
		}
		s.results[input.WorkflowID][strconv.Itoa(input.Step)] = string(input.Result)
	}
	if input.Failed {
		failedCount++
	} else {
		completed++
	}

	terminalOwner, enqueueNext, nextStep := false, false, -1
	finished := completed + failedCount
	switch {
	case wtype == "chain":
		if input.Failed {
			wstate = "failed"
			terminalOwner = true
		} else if finished >= total {
			wstate = "completed"
			terminalOwner = true
		} else {
			enqueueNext = true
			nextStep = input.Step + 1
		}
	case finished >= total:
		if failedCount > 0 {
			wstate = "failed"
		} else {
			wstate = "completed"
		}
		terminalOwner = true
	}

	wf["completed"] = strconv.Itoa(completed)
	wf["failed"] = strconv.Itoa(failedCount)
	wf["state"] = wstate
	if terminalOwner {
		wf["completed_at"] = input.CompletedAt
	}

	record := func(effectID, jobID string) {
		eff := s.effects[input.WorkflowID]
		if eff == nil {
			eff = map[string]string{}
			s.effects[input.WorkflowID] = eff
		}
		if _, ok := eff[effectID]; !ok {
			eff[effectID] = "pending|" + jobID
			s.pending[input.WorkflowID] = true
		}
	}
	if enqueueNext {
		record("chain:"+strconv.Itoa(nextStep), input.NextChainJobID)
	} else if terminalOwner && wtype == "batch" && wf["callbacks"] != "" {
		record("callback:on_complete", input.OnCompleteJobID)
		if failedCount > 0 {
			record("callback:on_failure", input.OnFailureJobID)
		} else {
			record("callback:on_success", input.OnSuccessJobID)
		}
	}

	return &state.WorkflowAdvanceResult{
		Applied: true, WorkflowType: wtype, State: wstate, Completed: completed, Failed: failedCount,
		Total: total, TerminalOwner: terminalOwner, EnqueueNext: enqueueNext, NextStep: nextStep,
		HasPendingEffects: pendingFlag(),
	}, s.afterAdvance()
}

// afterAdvance runs the optional post-advance hook (used by recovery tests to
// simulate a client disconnect right after the effect is durably recorded).
func (s *fakeWorkflowStore) afterAdvance() error {
	if s.advanceHook != nil {
		s.advanceHook()
	}
	return nil
}

func (s *fakeWorkflowStore) GetWorkflowEffects(_ context.Context, id string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for k, v := range s.effects[id] {
		out[k] = v
	}
	return out, nil
}

func (s *fakeWorkflowStore) GetWorkflowsWithPendingEffects(context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for id := range s.pending {
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *fakeWorkflowStore) ClaimWorkflowEffect(_ context.Context, id, effectID, owner string, nowMs, leaseMs int64) (*state.WorkflowEffectClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workflows[id]["state"] == "cancelled" {
		return &state.WorkflowEffectClaim{Status: state.WorkflowEffectFenced}, nil
	}
	eff := s.effects[id]
	cur, ok := eff[effectID]
	if !ok {
		return &state.WorkflowEffectClaim{Status: state.WorkflowEffectGone}, nil
	}
	parts := strings.Split(cur, "|")
	switch parts[0] {
	case "done":
		return &state.WorkflowEffectClaim{Status: state.WorkflowEffectDone, JobID: parts[1]}, nil
	case "pending":
		eff[effectID] = fmt.Sprintf("active|%s|%d|%s", owner, nowMs+leaseMs, parts[1])
		return &state.WorkflowEffectClaim{Status: state.WorkflowEffectClaimed, JobID: parts[1]}, nil
	case "active":
		leaseUntil, _ := strconv.ParseInt(parts[2], 10, 64)
		jobID := parts[3]
		if leaseUntil > nowMs {
			return &state.WorkflowEffectClaim{Status: state.WorkflowEffectBusy, JobID: jobID}, nil
		}
		eff[effectID] = fmt.Sprintf("active|%s|%d|%s", owner, nowMs+leaseMs, jobID)
		return &state.WorkflowEffectClaim{Status: state.WorkflowEffectClaimed, JobID: jobID}, nil
	}
	return nil, errors.New("invalid effect state")
}

func (s *fakeWorkflowStore) AtomicCreateWorkflowEffectJob(ctx context.Context, id, effectID, owner string, job *core.Job, _ float64, _ bool) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createAttempts[job.Type]++
	if s.workflows[id]["state"] == "cancelled" {
		return state.WorkflowEffectJobFenced, nil
	}
	eff := s.effects[id]
	cur, ok := eff[effectID]
	if !ok {
		return state.WorkflowEffectJobGone, nil
	}
	parts := strings.Split(cur, "|")
	if len(parts) < 4 || parts[0] != "active" || parts[1] != owner {
		return state.WorkflowEffectJobNotOwner, nil
	}
	if parts[3] != job.ID {
		return "", errors.New("stable job ID mismatch")
	}
	if _, exists := s.jobs[job.ID]; exists {
		return state.WorkflowEffectJobExisting, nil
	}
	injectedErr := s.createErr[job.Type]
	if injectedErr != nil && !s.createAfterPersist[job.Type] {
		return "", injectedErr
	}
	copyJob := *job
	s.jobs[job.ID] = &copyJob
	s.created = append(s.created, &copyJob)
	if injectedErr != nil {
		delete(s.createErr, job.Type)
		delete(s.createAfterPersist, job.Type)
		return "", injectedErr
	}
	return state.WorkflowEffectJobCreated, nil
}

func (s *fakeWorkflowStore) CompleteWorkflowEffect(_ context.Context, id, effectID, owner, jobID string, appendJob bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workflows[id]["state"] == "cancelled" {
		return false, nil
	}
	eff := s.effects[id]
	cleanup := func() {
		for _, v := range eff {
			if !strings.HasPrefix(v, "done|") {
				return
			}
		}
		delete(s.effects, id)
		delete(s.pending, id)
	}
	cur, ok := eff[effectID]
	if !ok {
		cleanup()
		return false, nil
	}
	parts := strings.Split(cur, "|")
	if parts[0] == "done" {
		cleanup()
		return false, nil
	}
	if parts[0] != "active" || parts[1] != owner {
		return false, nil
	}
	if appendJob {
		s.jobsList[id] = append(s.jobsList[id], jobID)
	}
	eff[effectID] = "done|" + jobID
	cleanup()
	return true, nil
}

func (s *fakeWorkflowStore) ReleaseWorkflowEffect(_ context.Context, id, effectID, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	eff := s.effects[id]
	cur, ok := eff[effectID]
	if !ok {
		return nil
	}
	parts := strings.Split(cur, "|")
	if parts[0] != "active" || parts[1] != owner {
		return nil
	}
	eff[effectID] = "pending|" + parts[3]
	return nil
}

// --- Job lifecycle used by ACK/NACK recovery tests ---

func (s *fakeWorkflowStore) AtomicAck(_ context.Context, jobID string, _ string, completedAt string, result string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.jobs[jobID]; ok {
		job.State = core.StateCompleted
		job.CompletedAt = completedAt
		if result != "" {
			job.Result = []byte(result)
		}
	}
	return nil
}

func (s *fakeWorkflowStore) AtomicNackDiscard(_ context.Context, jobID string, _ string, completedAt string, _ string, _ string, attempt string, _ bool, _ int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.jobs[jobID]; ok {
		job.State = core.StateDiscarded
		job.CompletedAt = completedAt
		job.Attempt, _ = strconv.Atoi(attempt)
	}
	return nil
}

func (s *fakeWorkflowStore) AtomicCancelJob(_ context.Context, jobID string, cancelledAt string) (*state.JobCancelResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return &state.JobCancelResult{}, nil
	}
	if core.IsTerminalState(job.State) {
		return &state.JobCancelResult{PreviousState: job.State}, nil
	}
	previous := job.State
	job.State = core.StateCancelled
	job.CancelledAt = cancelledAt
	return &state.JobCancelResult{Cancelled: true, PreviousState: previous}, nil
}

// appendedJobs returns a copy of the workflow's appended job IDs.
func (s *fakeWorkflowStore) appendedJobs(id string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.jobsList[id]...)
}

func (s *fakeWorkflowStore) createdJobs() []*core.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*core.Job, len(s.created))
	copy(out, s.created)
	return out
}

func (s *fakeWorkflowStore) creationAttempts(jobType string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createAttempts[jobType]
}
