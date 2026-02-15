package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

func TestWriteJSON_200WithStruct(t *testing.T) {
	w := httptest.NewRecorder()

	data := struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}{Name: "test", Count: 42}

	WriteJSON(w, http.StatusOK, data)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if ct := w.Header().Get("Content-Type"); ct != core.OJSMediaType {
		t.Errorf("Content-Type = %q, want %q", ct, core.OJSMediaType)
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp["name"] != "test" {
		t.Errorf("name = %v, want %q", resp["name"], "test")
	}
	if resp["count"] != float64(42) {
		t.Errorf("count = %v, want %v", resp["count"], 42)
	}
}

func TestWriteJSON_201WithMap(t *testing.T) {
	w := httptest.NewRecorder()

	data := map[string]any{"id": "abc-123", "created": true}

	WriteJSON(w, http.StatusCreated, data)

	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d", w.Code, http.StatusCreated)
	}
	if ct := w.Header().Get("Content-Type"); ct != core.OJSMediaType {
		t.Errorf("Content-Type = %q, want %q", ct, core.OJSMediaType)
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp["id"] != "abc-123" {
		t.Errorf("id = %v, want %q", resp["id"], "abc-123")
	}
}

func TestWriteJSON_200WithSlice(t *testing.T) {
	w := httptest.NewRecorder()

	data := []string{"alpha", "beta", "gamma"}

	WriteJSON(w, http.StatusOK, data)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if ct := w.Header().Get("Content-Type"); ct != core.OJSMediaType {
		t.Errorf("Content-Type = %q, want %q", ct, core.OJSMediaType)
	}

	var resp []string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if len(resp) != 3 {
		t.Errorf("len = %d, want 3", len(resp))
	}
}

func TestWriteError_400InvalidRequest(t *testing.T) {
	w := httptest.NewRecorder()

	ojsErr := core.NewInvalidRequestError("missing required field", nil)
	WriteError(w, http.StatusBadRequest, ojsErr)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	if ct := w.Header().Get("Content-Type"); ct != core.OJSMediaType {
		t.Errorf("Content-Type = %q, want %q", ct, core.OJSMediaType)
	}

	var resp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Error.Code != core.ErrCodeInvalidRequest {
		t.Errorf("code = %q, want %q", resp.Error.Code, core.ErrCodeInvalidRequest)
	}
}

func TestWriteError_404NotFound(t *testing.T) {
	w := httptest.NewRecorder()

	ojsErr := core.NewNotFoundError("Job", "abc-123")
	WriteError(w, http.StatusNotFound, ojsErr)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}

	var resp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Error.Code != core.ErrCodeNotFound {
		t.Errorf("code = %q, want %q", resp.Error.Code, core.ErrCodeNotFound)
	}
}

func TestWriteError_500InternalRetryable(t *testing.T) {
	w := httptest.NewRecorder()

	ojsErr := core.NewInternalError("connection lost")
	WriteError(w, http.StatusInternalServerError, ojsErr)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", w.Code, http.StatusInternalServerError)
	}

	var resp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Error.Code != core.ErrCodeInternalError {
		t.Errorf("code = %q, want %q", resp.Error.Code, core.ErrCodeInternalError)
	}
	if !resp.Error.Retryable {
		t.Error("expected retryable = true for internal error")
	}
}

func TestWriteError_IncludesRequestID(t *testing.T) {
	w := httptest.NewRecorder()
	w.Header().Set("X-Request-Id", "req-abc-789")

	ojsErr := core.NewInvalidRequestError("bad input", nil)
	WriteError(w, http.StatusBadRequest, ojsErr)

	var resp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Error.RequestID != "req-abc-789" {
		t.Errorf("request_id = %q, want %q", resp.Error.RequestID, "req-abc-789")
	}
}
