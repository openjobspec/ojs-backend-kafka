package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// ErrorResponse wraps an OJS error for JSON serialization.
type ErrorResponse struct {
	Error *core.OJSError `json:"error"`
}

// WriteError writes an OJS-formatted error response.
func WriteError(w http.ResponseWriter, status int, err *core.OJSError) {
	reqID := w.Header().Get("X-Request-Id")
	if reqID != "" {
		err.RequestID = reqID
	}

	w.Header().Set("Content-Type", core.OJSMediaType)
	w.WriteHeader(status)
	if encErr := json.NewEncoder(w).Encode(ErrorResponse{Error: err}); encErr != nil {
		slog.Error("failed to encode error response", "error", encErr)
	}
}

// WriteJSON writes a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", core.OJSMediaType)
	w.WriteHeader(status)
	if encErr := json.NewEncoder(w).Encode(data); encErr != nil {
		slog.Error("failed to encode JSON response", "error", encErr)
	}
}

// WriteOJSError maps an OJSError to the appropriate HTTP status code and writes it.
func WriteOJSError(w http.ResponseWriter, err *core.OJSError) {
	status := http.StatusInternalServerError
	switch err.Code {
	case core.ErrCodeNotFound:
		status = http.StatusNotFound
	case core.ErrCodeConflict, core.ErrCodeDuplicate:
		status = http.StatusConflict
	case core.ErrCodeInvalidRequest:
		status = http.StatusBadRequest
	case core.ErrCodeValidationError:
		status = http.StatusUnprocessableEntity
	case core.ErrCodeQueuePaused:
		status = http.StatusConflict
	case core.ErrCodeInternalError:
		status = http.StatusInternalServerError
	}
	WriteError(w, status, err)
}

// HandleError dispatches an error as an HTTP response. If the error is an OJSError,
// it maps to the appropriate status code. Otherwise, it wraps as an internal error.
func HandleError(w http.ResponseWriter, err error) {
	if ojsErr, ok := err.(*core.OJSError); ok {
		WriteOJSError(w, ojsErr)
		return
	}
	WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
}
