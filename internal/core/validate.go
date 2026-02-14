package core

import (
	"encoding/json"
	"fmt"
	"regexp"
)

var (
	typePattern  = regexp.MustCompile(`^[a-z][a-z0-9_\-]*(\.[a-z][a-z0-9_\-]*)*$`)
	queuePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9\-\.]*$`)
)

// ValidateEnqueueRequest validates an enqueue request.
func ValidateEnqueueRequest(req *EnqueueRequest) *OJSError {
	// Type is required
	if req.Type == "" {
		return NewInvalidRequestError("The 'type' field is required.", map[string]any{
			"field":      "type",
			"validation": "required",
		})
	}

	// Type format validation
	if !typePattern.MatchString(req.Type) {
		return NewInvalidRequestError(
			fmt.Sprintf("The 'type' field must match pattern '^[a-z][a-z0-9_]*(\\.[a-z][a-z0-9_]*)*$'. Got: %q", req.Type),
			map[string]any{
				"field":    "type",
				"expected": "^[a-z][a-z0-9_]*(\\.[a-z][a-z0-9_]*)*$",
				"received": req.Type,
			},
		)
	}

	// Args is required and must not be null
	if req.Args == nil || string(req.Args) == "null" {
		return NewInvalidRequestError("The 'args' field is required and must be a JSON array.", map[string]any{
			"field":      "args",
			"validation": "required",
		})
	}

	// Args must be a JSON array
	var args []json.RawMessage
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return NewInvalidRequestError("The 'args' field must be a JSON array.", map[string]any{
			"field":    "args",
			"expected": "array",
			"received": detectJSONType(req.Args),
		})
	}

	// Validate args elements are JSON-native types
	for i, arg := range args {
		if !isValidJSONType(arg) {
			return NewInvalidRequestError(
				fmt.Sprintf("The 'args[%d]' field contains a non-JSON-native type.", i),
				map[string]any{
					"field":    fmt.Sprintf("args[%d]", i),
					"expected": "JSON-native type (string, number, boolean, null, object, array)",
				},
			)
		}
	}

	// Validate ID if provided (including empty string which is invalid)
	if req.HasID && !IsValidUUIDv7(req.ID) {
		return NewInvalidRequestError(
			fmt.Sprintf("The 'id' field must be a valid UUIDv7. Got: %q", req.ID),
			map[string]any{
				"field":    "id",
				"expected": "UUIDv7",
				"received": req.ID,
			},
		)
	}

	// Validate options
	if req.Options != nil {
		if err := validateOptions(req.Options); err != nil {
			return err
		}
	}

	return nil
}

func validateOptions(opts *EnqueueOptions) *OJSError {
	// Queue name validation
	if opts.Queue != "" && !queuePattern.MatchString(opts.Queue) {
		return NewInvalidRequestError(
			fmt.Sprintf("The 'queue' field must match pattern '^[a-z0-9][a-z0-9\\-\\.]*$'. Got: %q", opts.Queue),
			map[string]any{
				"field":    "queue",
				"expected": "^[a-z0-9][a-z0-9\\-\\.]*$",
				"received": opts.Queue,
			},
		)
	}

	// Priority validation (-100 to 100)
	if opts.Priority != nil {
		p := *opts.Priority
		if p < -100 || p > 100 {
			return NewInvalidRequestError(
				fmt.Sprintf("The 'priority' field must be between -100 and 100. Got: %d", p),
				map[string]any{
					"field":    "priority",
					"expected": "-100 to 100",
					"received": p,
				},
			)
		}
	}

	// Validate retry policy
	retryPolicy := opts.Retry
	if retryPolicy == nil {
		retryPolicy = opts.RetryPolicy
	}
	if retryPolicy != nil {
		if err := validateRetryPolicy(retryPolicy); err != nil {
			return err
		}
	}

	return nil
}

func validateRetryPolicy(r *RetryPolicy) *OJSError {
	if r.MaxAttempts < 0 {
		return NewValidationError(
			fmt.Sprintf("The 'retry.max_attempts' field must be non-negative. Got: %d", r.MaxAttempts),
			map[string]any{
				"field":    "retry.max_attempts",
				"expected": ">= 0",
				"received": r.MaxAttempts,
			},
		)
	}

	if r.BackoffCoefficient != 0 && r.BackoffCoefficient < 1.0 {
		return NewValidationError(
			fmt.Sprintf("The 'retry.backoff_coefficient' field must be >= 1.0. Got: %f", r.BackoffCoefficient),
			map[string]any{
				"field":    "retry.backoff_coefficient",
				"expected": ">= 1.0",
				"received": r.BackoffCoefficient,
			},
		)
	}

	if r.InitialInterval != "" {
		if _, err := ParseISO8601Duration(r.InitialInterval); err != nil {
			return NewInvalidRequestError(
				fmt.Sprintf("The 'retry.initial_interval' field must be a valid ISO 8601 duration. Got: %q", r.InitialInterval),
				map[string]any{
					"field":    "retry.initial_interval",
					"expected": "ISO 8601 duration (e.g., PT1S, PT5M)",
					"received": r.InitialInterval,
				},
			)
		}
	}

	if r.MaxInterval != "" {
		if _, err := ParseISO8601Duration(r.MaxInterval); err != nil {
			return NewInvalidRequestError(
				fmt.Sprintf("The 'retry.max_interval' field must be a valid ISO 8601 duration. Got: %q", r.MaxInterval),
				map[string]any{
					"field":    "retry.max_interval",
					"expected": "ISO 8601 duration (e.g., PT5M)",
					"received": r.MaxInterval,
				},
			)
		}
	}

	return nil
}

func detectJSONType(data json.RawMessage) string {
	if len(data) == 0 {
		return "empty"
	}
	switch data[0] {
	case '"':
		return "string"
	case '{':
		return "object"
	case '[':
		return "array"
	case 't', 'f':
		return "boolean"
	case 'n':
		return "null"
	default:
		return "number"
	}
}

func isValidJSONType(data json.RawMessage) bool {
	// All JSON types are valid: string, number, boolean, null, object, array
	var v any
	return json.Unmarshal(data, &v) == nil
}
