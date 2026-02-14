package kafka

import (
	"strconv"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

func TestJobToHeaders_Required(t *testing.T) {
	job := &core.Job{
		ID:        "test-id",
		Type:      "email.send",
		Queue:     "default",
		State:     core.StateAvailable,
		Attempt:   2,
		CreatedAt: "2025-01-01T00:00:00.000Z",
	}

	headers := JobToHeaders(job)

	expected := map[string]string{
		HeaderSpecVersion: core.OJSVersion,
		HeaderJobID:       "test-id",
		HeaderJobType:     "email.send",
		HeaderQueue:       "default",
		HeaderState:       core.StateAvailable,
		HeaderAttempt:     "2",
		HeaderCreatedAt:   "2025-01-01T00:00:00.000Z",
	}

	headerMap := toHeaderMap(headers)
	for key, want := range expected {
		got, ok := headerMap[key]
		if !ok {
			t.Errorf("missing header %q", key)
			continue
		}
		if got != want {
			t.Errorf("header %q = %q, want %q", key, got, want)
		}
	}
}

func TestJobToHeaders_OptionalPriority(t *testing.T) {
	pri := 10
	job := &core.Job{
		ID:       "test-id",
		Type:     "test",
		Queue:    "default",
		State:    core.StateAvailable,
		Priority: &pri,
	}

	headers := JobToHeaders(job)
	headerMap := toHeaderMap(headers)

	if got := headerMap[HeaderPriority]; got != "10" {
		t.Errorf("priority header = %q, want %q", got, "10")
	}
}

func TestJobToHeaders_OptionalMaxAttempts(t *testing.T) {
	maxA := 5
	job := &core.Job{
		ID:          "test-id",
		Type:        "test",
		Queue:       "default",
		State:       core.StateAvailable,
		MaxAttempts: &maxA,
	}

	headers := JobToHeaders(job)
	headerMap := toHeaderMap(headers)

	if got := headerMap[HeaderMaxAttempts]; got != "5" {
		t.Errorf("max_attempts header = %q, want %q", got, "5")
	}
}

func TestJobToHeaders_Workflow(t *testing.T) {
	job := &core.Job{
		ID:           "test-id",
		Type:         "test",
		Queue:        "default",
		State:        core.StateAvailable,
		WorkflowID:   "wf-123",
		WorkflowStep: 3,
	}

	headers := JobToHeaders(job)
	headerMap := toHeaderMap(headers)

	if got := headerMap[HeaderWorkflowID]; got != "wf-123" {
		t.Errorf("workflow_id header = %q, want %q", got, "wf-123")
	}
	if got := headerMap[HeaderWorkflowStep]; got != "3" {
		t.Errorf("workflow_step header = %q, want %q", got, "3")
	}
}

func TestJobToHeaders_NoPriorityWhenNil(t *testing.T) {
	job := &core.Job{
		ID:    "test-id",
		Type:  "test",
		Queue: "default",
		State: core.StateAvailable,
	}

	headers := JobToHeaders(job)
	headerMap := toHeaderMap(headers)

	if _, ok := headerMap[HeaderPriority]; ok {
		t.Error("priority header should be absent when nil")
	}
	if _, ok := headerMap[HeaderWorkflowID]; ok {
		t.Error("workflow_id header should be absent when empty")
	}
	if _, ok := headerMap[HeaderScheduledAt]; ok {
		t.Error("scheduled_at header should be absent when empty")
	}
	if _, ok := headerMap[HeaderExpiresAt]; ok {
		t.Error("expires_at header should be absent when empty")
	}
}

func TestJobToHeaders_ScheduledAndExpires(t *testing.T) {
	job := &core.Job{
		ID:          "test-id",
		Type:        "test",
		Queue:       "default",
		State:       core.StateScheduled,
		ScheduledAt: "2025-06-15T10:00:00Z",
		ExpiresAt:   "2025-06-16T10:00:00Z",
	}

	headers := JobToHeaders(job)
	headerMap := toHeaderMap(headers)

	if got := headerMap[HeaderScheduledAt]; got != "2025-06-15T10:00:00Z" {
		t.Errorf("scheduled_at = %q, want %q", got, "2025-06-15T10:00:00Z")
	}
	if got := headerMap[HeaderExpiresAt]; got != "2025-06-16T10:00:00Z" {
		t.Errorf("expires_at = %q, want %q", got, "2025-06-16T10:00:00Z")
	}
}

func TestGetHeader(t *testing.T) {
	record := &kgo.Record{
		Headers: []kgo.RecordHeader{
			{Key: "key1", Value: []byte("value1")},
			{Key: "key2", Value: []byte("value2")},
		},
	}

	if got := GetHeader(record, "key1"); got != "value1" {
		t.Errorf("GetHeader(key1) = %q, want %q", got, "value1")
	}
	if got := GetHeader(record, "key2"); got != "value2" {
		t.Errorf("GetHeader(key2) = %q, want %q", got, "value2")
	}
	if got := GetHeader(record, "missing"); got != "" {
		t.Errorf("GetHeader(missing) = %q, want empty", got)
	}
}

func TestEventHeaders(t *testing.T) {
	headers := EventHeaders("job.completed", "job-123", "default")
	headerMap := toHeaderMap(headers)

	if got := headerMap[HeaderSpecVersion]; got != core.OJSVersion {
		t.Errorf("specversion = %q, want %q", got, core.OJSVersion)
	}
	if got := headerMap[HeaderEventType]; got != "job.completed" {
		t.Errorf("event_type = %q, want %q", got, "job.completed")
	}
	if got := headerMap[HeaderJobID]; got != "job-123" {
		t.Errorf("job_id = %q, want %q", got, "job-123")
	}
	if got := headerMap[HeaderQueue]; got != "default" {
		t.Errorf("queue = %q, want %q", got, "default")
	}

	if len(headers) != 4 {
		t.Errorf("expected 4 headers, got %d", len(headers))
	}
}

func TestJobToHeaders_AttemptValue(t *testing.T) {
	for _, attempt := range []int{0, 1, 5, 100} {
		job := &core.Job{
			ID:      "test-id",
			Type:    "test",
			Queue:   "default",
			State:   core.StateActive,
			Attempt: attempt,
		}
		headers := JobToHeaders(job)
		headerMap := toHeaderMap(headers)
		want := strconv.Itoa(attempt)
		if got := headerMap[HeaderAttempt]; got != want {
			t.Errorf("attempt %d: header = %q, want %q", attempt, got, want)
		}
	}
}

func toHeaderMap(headers []kgo.RecordHeader) map[string]string {
	m := make(map[string]string)
	for _, h := range headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
