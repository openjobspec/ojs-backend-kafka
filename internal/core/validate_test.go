package core

import (
	"encoding/json"
	"testing"
)

func TestValidateEnqueueRequest_Valid(t *testing.T) {
	req := &EnqueueRequest{
		Type: "email.send",
		Args: json.RawMessage(`["hello@example.com", "subject"]`),
	}
	if err := ValidateEnqueueRequest(req); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateEnqueueRequest_MissingType(t *testing.T) {
	req := &EnqueueRequest{
		Args: json.RawMessage(`[1]`),
	}
	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for missing type")
	}
	if err.Code != ErrCodeInvalidRequest {
		t.Errorf("got code %q, want %q", err.Code, ErrCodeInvalidRequest)
	}
}

func TestValidateEnqueueRequest_InvalidTypeFormat(t *testing.T) {
	tests := []string{
		"UPPERCASE",
		"with spaces",
		"123starts_with_number",
		"",
	}
	for _, typ := range tests {
		req := &EnqueueRequest{
			Type: typ,
			Args: json.RawMessage(`[1]`),
		}
		if typ == "" {
			// Empty type triggers "required" check first
			continue
		}
		err := ValidateEnqueueRequest(req)
		if err == nil {
			t.Errorf("expected error for type %q", typ)
		}
	}
}

func TestValidateEnqueueRequest_ValidTypeFormats(t *testing.T) {
	tests := []string{
		"email",
		"email.send",
		"worker.batch.process",
		"job-type",
		"my_job",
		"a1-b2.c3-d4",
	}
	for _, typ := range tests {
		req := &EnqueueRequest{
			Type: typ,
			Args: json.RawMessage(`[1]`),
		}
		err := ValidateEnqueueRequest(req)
		if err != nil {
			t.Errorf("unexpected error for type %q: %v", typ, err)
		}
	}
}

func TestValidateEnqueueRequest_MissingArgs(t *testing.T) {
	req := &EnqueueRequest{
		Type: "email.send",
	}
	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for missing args")
	}
}

func TestValidateEnqueueRequest_NullArgs(t *testing.T) {
	req := &EnqueueRequest{
		Type: "email.send",
		Args: json.RawMessage(`null`),
	}
	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for null args")
	}
}

func TestValidateEnqueueRequest_ArgsNotArray(t *testing.T) {
	req := &EnqueueRequest{
		Type: "email.send",
		Args: json.RawMessage(`{"key": "value"}`),
	}
	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for object args (not array)")
	}
}

func TestValidateEnqueueRequest_InvalidID(t *testing.T) {
	req := &EnqueueRequest{
		ID:    "not-a-uuid-v7",
		HasID: true,
		Type:  "email.send",
		Args:  json.RawMessage(`[1]`),
	}
	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for invalid ID")
	}
}

func TestValidateEnqueueRequest_InvalidQueueName(t *testing.T) {
	req := &EnqueueRequest{
		Type: "email.send",
		Args: json.RawMessage(`[1]`),
		Options: &EnqueueOptions{
			Queue: "INVALID QUEUE",
		},
	}
	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for invalid queue name")
	}
}

func TestValidateEnqueueRequest_ValidQueueNames(t *testing.T) {
	tests := []string{"default", "emails", "batch-queue", "queue.name", "q1"}
	for _, q := range tests {
		req := &EnqueueRequest{
			Type: "email.send",
			Args: json.RawMessage(`[1]`),
			Options: &EnqueueOptions{
				Queue: q,
			},
		}
		err := ValidateEnqueueRequest(req)
		if err != nil {
			t.Errorf("unexpected error for queue %q: %v", q, err)
		}
	}
}

func TestValidateEnqueueRequest_PriorityOutOfRange(t *testing.T) {
	p := 200
	req := &EnqueueRequest{
		Type: "email.send",
		Args: json.RawMessage(`[1]`),
		Options: &EnqueueOptions{
			Priority: &p,
		},
	}
	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for priority 200")
	}
}

func TestValidateEnqueueRequest_PriorityValid(t *testing.T) {
	for _, p := range []int{-100, 0, 50, 100} {
		pri := p
		req := &EnqueueRequest{
			Type: "email.send",
			Args: json.RawMessage(`[1]`),
			Options: &EnqueueOptions{
				Priority: &pri,
			},
		}
		err := ValidateEnqueueRequest(req)
		if err != nil {
			t.Errorf("unexpected error for priority %d: %v", p, err)
		}
	}
}

func TestValidateEnqueueRequest_RetryPolicy(t *testing.T) {
	req := &EnqueueRequest{
		Type: "email.send",
		Args: json.RawMessage(`[1]`),
		Options: &EnqueueOptions{
			Retry: &RetryPolicy{
				MaxAttempts:        -1,
			},
		},
	}
	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for negative max_attempts")
	}
}

func TestValidateEnqueueRequest_InvalidRetryInterval(t *testing.T) {
	req := &EnqueueRequest{
		Type: "email.send",
		Args: json.RawMessage(`[1]`),
		Options: &EnqueueOptions{
			Retry: &RetryPolicy{
				MaxAttempts:     3,
				InitialInterval: "invalid",
			},
		},
	}
	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for invalid initial_interval")
	}
}

func TestDetectJSONType(t *testing.T) {
	tests := []struct {
		input json.RawMessage
		want  string
	}{
		{json.RawMessage(`"hello"`), "string"},
		{json.RawMessage(`42`), "number"},
		{json.RawMessage(`true`), "boolean"},
		{json.RawMessage(`false`), "boolean"},
		{json.RawMessage(`null`), "null"},
		{json.RawMessage(`{}`), "object"},
		{json.RawMessage(`[]`), "array"},
		{json.RawMessage(``), "empty"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := detectJSONType(tt.input)
			if got != tt.want {
				t.Errorf("detectJSONType(%s) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
