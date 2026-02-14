package core

import (
	"math"
	"math/rand"
	"time"
)

// CalculateBackoff computes the next retry delay based on the retry policy and attempt number.
func CalculateBackoff(policy *RetryPolicy, attempt int) time.Duration {
	if policy == nil {
		policy2 := DefaultRetryPolicy()
		policy = &policy2
	}

	initialInterval := time.Second
	if policy.InitialInterval != "" {
		if d, err := ParseISO8601Duration(policy.InitialInterval); err == nil {
			initialInterval = d
		}
	}

	coefficient := policy.BackoffCoefficient
	if coefficient <= 0 {
		coefficient = 2.0 // default exponential
	}

	var delay float64
	backoffType := policy.BackoffType
	if backoffType == "" {
		backoffType = policy.BackoffStrategy
	}
	if backoffType == "" {
		backoffType = "exponential"
	}

	switch backoffType {
	case "linear":
		// linear: initial_interval * attempt
		delay = float64(initialInterval) * float64(attempt)
	case "constant":
		// constant: always initial_interval
		delay = float64(initialInterval)
	default: // "exponential"
		// exponential: initial_interval * coefficient^(attempt-1)
		delay = float64(initialInterval) * math.Pow(coefficient, float64(attempt-1))
	}

	// Apply max interval cap
	if policy.MaxInterval != "" {
		if maxInterval, err := ParseISO8601Duration(policy.MaxInterval); err == nil {
			if delay > float64(maxInterval) {
				delay = float64(maxInterval)
			}
		}
	}

	result := time.Duration(delay)

	// Apply jitter (0.5x to 1.5x)
	if policy.Jitter {
		jitterFactor := 0.5 + rand.Float64() // 0.5 to 1.5
		result = time.Duration(float64(result) * jitterFactor)
	}

	return result
}
