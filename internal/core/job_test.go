package core

import (
	"encoding/json"
	"testing"
	"time"
)

func TestFormatTime(t *testing.T) {
	tm := time.Date(2025, 6, 15, 10, 30, 45, 123000000, time.UTC)
	got := FormatTime(tm)
	want := "2025-06-15T10:30:45.123Z"
	if got != want {
		t.Errorf("FormatTime() = %q, want %q", got, want)
	}
}

func TestFormatTime_NonUTC(t *testing.T) {
	loc, _ := time.LoadLocation("America/New_York")
	tm := time.Date(2025, 6, 15, 10, 30, 45, 0, loc)
	got := FormatTime(tm)
	// Should be converted to UTC
	if got[len(got)-1] != 'Z' {
		t.Errorf("FormatTime() should end with Z, got %q", got)
	}
}

func TestJob_MarshalJSON(t *testing.T) {
	pri := 5
	job := Job{
		ID:        "test-id",
		Type:      "email.send",
		State:     StateAvailable,
		Queue:     "default",
		Args:      json.RawMessage(`["hello"]`),
		Priority:  &pri,
		Attempt:   0,
		CreatedAt: "2025-01-01T00:00:00.000Z",
	}

	data, err := job.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("Unmarshal output error: %v", err)
	}

	if m["id"] != "test-id" {
		t.Errorf("id = %v, want %q", m["id"], "test-id")
	}
	if m["type"] != "email.send" {
		t.Errorf("type = %v, want %q", m["type"], "email.send")
	}
	if m["state"] != StateAvailable {
		t.Errorf("state = %v, want %q", m["state"], StateAvailable)
	}
	if m["priority"] != float64(5) {
		t.Errorf("priority = %v, want %v", m["priority"], 5)
	}
}

func TestJob_MarshalJSON_OmitsEmpty(t *testing.T) {
	job := Job{
		ID:    "test-id",
		Type:  "test",
		State: StateAvailable,
		Queue: "default",
	}

	data, err := job.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("Unmarshal output error: %v", err)
	}

	// These optional fields should not be present
	for _, key := range []string{"priority", "max_attempts", "timeout_ms", "started_at", "completed_at", "result", "error", "tags"} {
		if _, ok := m[key]; ok {
			t.Errorf("expected key %q to be omitted", key)
		}
	}
}

func TestJob_MarshalJSON_UnknownFields(t *testing.T) {
	job := Job{
		ID:    "test-id",
		Type:  "test",
		State: StateAvailable,
		Queue: "default",
		UnknownFields: map[string]json.RawMessage{
			"custom_field": json.RawMessage(`"custom_value"`),
		},
	}

	data, err := job.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("Unmarshal output error: %v", err)
	}

	if m["custom_field"] != "custom_value" {
		t.Errorf("custom_field = %v, want %q", m["custom_field"], "custom_value")
	}
}

func TestParseEnqueueRequest(t *testing.T) {
	input := `{
		"type": "email.send",
		"args": [1, 2, 3],
		"custom_extension": "value"
	}`

	req, err := ParseEnqueueRequest([]byte(input))
	if err != nil {
		t.Fatalf("ParseEnqueueRequest error: %v", err)
	}

	if req.Type != "email.send" {
		t.Errorf("type = %q, want %q", req.Type, "email.send")
	}
	if req.HasID {
		t.Error("expected HasID = false")
	}
	if _, ok := req.UnknownFields["custom_extension"]; !ok {
		t.Error("expected custom_extension in UnknownFields")
	}
}

func TestParseEnqueueRequest_WithID(t *testing.T) {
	input := `{
		"id": "01912345-6789-7abc-8def-0123456789ab",
		"type": "test",
		"args": [1]
	}`

	req, err := ParseEnqueueRequest([]byte(input))
	if err != nil {
		t.Fatalf("ParseEnqueueRequest error: %v", err)
	}

	if !req.HasID {
		t.Error("expected HasID = true")
	}
	if req.ID != "01912345-6789-7abc-8def-0123456789ab" {
		t.Errorf("id = %q, want specific ID", req.ID)
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	if p.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d, want 3", p.MaxAttempts)
	}
	if p.InitialInterval != "PT1S" {
		t.Errorf("InitialInterval = %q, want PT1S", p.InitialInterval)
	}
	if p.BackoffCoefficient != 2.0 {
		t.Errorf("BackoffCoefficient = %f, want 2.0", p.BackoffCoefficient)
	}
	if p.MaxInterval != "PT5M" {
		t.Errorf("MaxInterval = %q, want PT5M", p.MaxInterval)
	}
	if !p.Jitter {
		t.Error("expected Jitter = true")
	}
}
