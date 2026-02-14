package core

import (
	"testing"
	"time"
)

func TestCalculateBackoff_Default(t *testing.T) {
	delay := CalculateBackoff(nil, 1)
	// Default: exponential, initial=1s, coefficient=2, jitter enabled
	// Attempt 1: 1s * 2^0 = 1s, with jitter: 0.5s-1.5s
	if delay < 500*time.Millisecond || delay > 1500*time.Millisecond {
		t.Errorf("attempt 1 delay %v outside expected range [500ms, 1500ms]", delay)
	}
}

func TestCalculateBackoff_Exponential_NoJitter(t *testing.T) {
	policy := &RetryPolicy{
		InitialInterval:    "PT1S",
		BackoffCoefficient: 2.0,
		Jitter:             false,
		BackoffType:        "exponential",
	}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},  // 1s * 2^0
		{2, 2 * time.Second},  // 1s * 2^1
		{3, 4 * time.Second},  // 1s * 2^2
		{4, 8 * time.Second},  // 1s * 2^3
	}

	for _, tt := range tests {
		got := CalculateBackoff(policy, tt.attempt)
		if got != tt.want {
			t.Errorf("attempt %d: got %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestCalculateBackoff_Linear_NoJitter(t *testing.T) {
	policy := &RetryPolicy{
		InitialInterval: "PT2S",
		BackoffType:     "linear",
		Jitter:          false,
	}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 2 * time.Second},  // 2s * 1
		{2, 4 * time.Second},  // 2s * 2
		{3, 6 * time.Second},  // 2s * 3
	}

	for _, tt := range tests {
		got := CalculateBackoff(policy, tt.attempt)
		if got != tt.want {
			t.Errorf("attempt %d: got %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestCalculateBackoff_Constant_NoJitter(t *testing.T) {
	policy := &RetryPolicy{
		InitialInterval: "PT5S",
		BackoffType:     "constant",
		Jitter:          false,
	}

	for attempt := 1; attempt <= 5; attempt++ {
		got := CalculateBackoff(policy, attempt)
		if got != 5*time.Second {
			t.Errorf("attempt %d: got %v, want %v", attempt, got, 5*time.Second)
		}
	}
}

func TestCalculateBackoff_MaxInterval(t *testing.T) {
	policy := &RetryPolicy{
		InitialInterval:    "PT1S",
		BackoffCoefficient: 10.0,
		MaxInterval:        "PT30S",
		BackoffType:        "exponential",
		Jitter:             false,
	}

	// Attempt 3: 1s * 10^2 = 100s, capped at 30s
	got := CalculateBackoff(policy, 3)
	if got != 30*time.Second {
		t.Errorf("got %v, want %v (capped)", got, 30*time.Second)
	}
}

func TestCalculateBackoff_BackoffStrategy_Alias(t *testing.T) {
	policy := &RetryPolicy{
		InitialInterval: "PT1S",
		BackoffStrategy: "linear",
		Jitter:          false,
	}

	got := CalculateBackoff(policy, 2)
	if got != 2*time.Second {
		t.Errorf("got %v, want %v", got, 2*time.Second)
	}
}

func TestCalculateBackoff_WithJitter(t *testing.T) {
	policy := &RetryPolicy{
		InitialInterval:    "PT10S",
		BackoffCoefficient: 1.0,
		BackoffType:        "constant",
		Jitter:             true,
	}

	// With jitter factor [0.5, 1.5], delay should be in [5s, 15s]
	for i := 0; i < 100; i++ {
		got := CalculateBackoff(policy, 1)
		if got < 5*time.Second || got > 15*time.Second {
			t.Errorf("iteration %d: delay %v outside expected jitter range [5s, 15s]", i, got)
		}
	}
}
