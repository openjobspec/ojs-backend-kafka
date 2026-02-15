package api

import (
	"encoding/json"
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
	json.NewEncoder(w).Encode(ErrorResponse{Error: err})
}

// WriteJSON writes a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", core.OJSMediaType)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
