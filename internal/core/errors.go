package core

import "fmt"

// Standard error codes used in OJS error responses.
const (
	ErrCodeInvalidRequest  = "invalid_request"
	ErrCodeValidationError = "validation_error"
	ErrCodeNotFound        = "not_found"
	ErrCodeConflict        = "conflict"
	ErrCodeDuplicate       = "duplicate"
	ErrCodeInternalError   = "internal_error"
	ErrCodeUnsupported     = "unsupported"
	ErrCodeQueuePaused     = "queue_paused"
)

// OJSError represents a structured error conforming to the OJS error format.
type OJSError struct {
	Code      string         `json:"code,omitempty"`
	Type      string         `json:"type,omitempty"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details,omitempty"`
	RequestID string         `json:"request_id,omitempty"`
}

func (e *OJSError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func NewInvalidRequestError(message string, details map[string]any) *OJSError {
	return &OJSError{
		Code:      ErrCodeInvalidRequest,
		Message:   message,
		Retryable: false,
		Details:   details,
	}
}

func NewNotFoundError(resourceType, resourceID string) *OJSError {
	return &OJSError{
		Code:      ErrCodeNotFound,
		Message:   fmt.Sprintf("%s '%s' not found.", resourceType, resourceID),
		Retryable: false,
		Details: map[string]any{
			"resource_type": resourceType,
			"resource_id":   resourceID,
		},
	}
}

func NewConflictError(message string, details map[string]any) *OJSError {
	return &OJSError{
		Code:      ErrCodeConflict,
		Message:   message,
		Retryable: false,
		Details:   details,
	}
}

func NewValidationError(message string, details map[string]any) *OJSError {
	return &OJSError{
		Code:      ErrCodeValidationError,
		Type:      ErrCodeValidationError,
		Message:   message,
		Retryable: false,
		Details:   details,
	}
}

func NewInternalError(message string) *OJSError {
	return &OJSError{
		Code:      ErrCodeInternalError,
		Message:   message,
		Retryable: true,
	}
}
