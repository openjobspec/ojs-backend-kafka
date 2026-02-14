package core

import (
	"testing"
)

func TestNewInvalidRequestError(t *testing.T) {
	err := NewInvalidRequestError("bad input", map[string]any{"field": "type"})
	if err.Code != ErrCodeInvalidRequest {
		t.Errorf("got code %q, want %q", err.Code, ErrCodeInvalidRequest)
	}
	if err.Message != "bad input" {
		t.Errorf("got message %q, want %q", err.Message, "bad input")
	}
	if err.Retryable {
		t.Error("expected Retryable=false")
	}
	if err.Details["field"] != "type" {
		t.Errorf("got details[field] %v, want %q", err.Details["field"], "type")
	}
}

func TestNewNotFoundError(t *testing.T) {
	err := NewNotFoundError("Job", "abc-123")
	if err.Code != ErrCodeNotFound {
		t.Errorf("got code %q, want %q", err.Code, ErrCodeNotFound)
	}
	if err.Details["resource_type"] != "Job" {
		t.Errorf("got resource_type %v, want %q", err.Details["resource_type"], "Job")
	}
	if err.Details["resource_id"] != "abc-123" {
		t.Errorf("got resource_id %v, want %q", err.Details["resource_id"], "abc-123")
	}
}

func TestNewConflictError(t *testing.T) {
	err := NewConflictError("state conflict", map[string]any{"current_state": "active"})
	if err.Code != ErrCodeConflict {
		t.Errorf("got code %q, want %q", err.Code, ErrCodeConflict)
	}
	if err.Message != "state conflict" {
		t.Errorf("got message %q, want %q", err.Message, "state conflict")
	}
}

func TestNewValidationError(t *testing.T) {
	err := NewValidationError("invalid value", map[string]any{"field": "priority"})
	if err.Code != ErrCodeValidationError {
		t.Errorf("got code %q, want %q", err.Code, ErrCodeValidationError)
	}
}

func TestNewInternalError(t *testing.T) {
	err := NewInternalError("something broke")
	if err.Code != ErrCodeInternalError {
		t.Errorf("got code %q, want %q", err.Code, ErrCodeInternalError)
	}
	if !err.Retryable {
		t.Error("expected Retryable=true for internal errors")
	}
}

func TestOJSError_Error(t *testing.T) {
	err := NewConflictError("test message", nil)
	got := err.Error()
	want := "[conflict] test message"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
