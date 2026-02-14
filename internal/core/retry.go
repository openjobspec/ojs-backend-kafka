package core

// RetryPolicy defines how failed jobs should be retried.
type RetryPolicy struct {
	MaxAttempts       int      `json:"max_attempts"`
	InitialInterval   string   `json:"initial_interval,omitempty"`
	BackoffCoefficient float64 `json:"backoff_coefficient,omitempty"`
	MaxInterval       string   `json:"max_interval,omitempty"`
	Jitter            bool     `json:"jitter,omitempty"`
	NonRetryableErrors []string `json:"non_retryable_errors,omitempty"`
	OnExhaustion      string   `json:"on_exhaustion,omitempty"`  // "discard" or "dead_letter"
	BackoffType       string   `json:"backoff_type,omitempty"`  // "exponential", "linear", "constant"
	BackoffStrategy   string   `json:"backoff_strategy,omitempty"` // alias for backoff_type
}

// DefaultRetryPolicy returns the default retry policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:        3,
		InitialInterval:   "PT1S",
		BackoffCoefficient: 2.0,
		MaxInterval:       "PT5M",
		Jitter:            true,
	}
}
