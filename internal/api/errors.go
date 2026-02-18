package api

import (
	"net/http"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"github.com/openjobspec/ojs-go-backend-common/httputil"
)

// ErrorResponse wraps an OJS error for JSON serialization.
type ErrorResponse = httputil.ErrorResponse

// WriteError writes an OJS-formatted error response.
func WriteError(w http.ResponseWriter, status int, err *core.OJSError) {
	httputil.WriteError(w, status, err)
}

// WriteJSON writes a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, data any) {
	httputil.WriteJSON(w, status, data)
}

// WriteOJSError maps an OJSError to the appropriate HTTP status code and writes it.
func WriteOJSError(w http.ResponseWriter, err *core.OJSError) {
	httputil.WriteOJSError(w, err)
}

// HandleError dispatches an error as an HTTP response. If the error is an OJSError,
// it maps to the appropriate status code. Otherwise, it wraps as an internal error.
func HandleError(w http.ResponseWriter, err error) {
	httputil.HandleError(w, err)
}
