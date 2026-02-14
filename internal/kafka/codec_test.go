package kafka

import (
	"encoding/json"
	"testing"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

func TestEncodeDecodeJob_Roundtrip(t *testing.T) {
	pri := 5
	maxA := 3
	original := &core.Job{
		ID:          "test-id-123",
		Type:        "email.send",
		State:       core.StateAvailable,
		Queue:       "default",
		Args:        json.RawMessage(`["hello@example.com","subject","body"]`),
		Priority:    &pri,
		MaxAttempts: &maxA,
		Attempt:     0,
		CreatedAt:   "2025-01-01T00:00:00.000Z",
		EnqueuedAt:  "2025-01-01T00:00:00.000Z",
	}

	encoded, err := EncodeJob(original)
	if err != nil {
		t.Fatalf("EncodeJob error: %v", err)
	}

	decoded, err := DecodeJob(encoded)
	if err != nil {
		t.Fatalf("DecodeJob error: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, original.ID)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type = %q, want %q", decoded.Type, original.Type)
	}
	if decoded.State != original.State {
		t.Errorf("State = %q, want %q", decoded.State, original.State)
	}
	if decoded.Queue != original.Queue {
		t.Errorf("Queue = %q, want %q", decoded.Queue, original.Queue)
	}
	if string(decoded.Args) != string(original.Args) {
		t.Errorf("Args = %s, want %s", decoded.Args, original.Args)
	}
}

func TestDecodeJob_InvalidJSON(t *testing.T) {
	_, err := DecodeJob([]byte("not json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestEncodeEvent(t *testing.T) {
	event := &LifecycleEvent{
		EventType: "job.enqueued",
		JobID:     "test-123",
		Queue:     "default",
		JobType:   "email.send",
		State:     core.StateAvailable,
		Timestamp: "2025-01-01T00:00:00.000Z",
	}

	data, err := EncodeEvent(event)
	if err != nil {
		t.Fatalf("EncodeEvent error: %v", err)
	}

	var decoded LifecycleEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if decoded.EventType != event.EventType {
		t.Errorf("EventType = %q, want %q", decoded.EventType, event.EventType)
	}
	if decoded.JobID != event.JobID {
		t.Errorf("JobID = %q, want %q", decoded.JobID, event.JobID)
	}
}

func TestEncodeEvent_WithOptionalFields(t *testing.T) {
	event := &LifecycleEvent{
		EventType:  "job.completed",
		JobID:      "test-123",
		Queue:      "default",
		JobType:    "email.send",
		State:      core.StateCompleted,
		Result:     json.RawMessage(`{"sent":true}`),
		WorkflowID: "wf-456",
		Timestamp:  "2025-01-01T00:00:00.000Z",
	}

	data, err := EncodeEvent(event)
	if err != nil {
		t.Fatalf("EncodeEvent error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if m["workflow_id"] != "wf-456" {
		t.Errorf("workflow_id = %v, want %q", m["workflow_id"], "wf-456")
	}
}
