package core

import "testing"

func TestIsValidTransition(t *testing.T) {
	tests := []struct {
		from, to string
		want     bool
	}{
		{StateScheduled, StateAvailable, true},
		{StateScheduled, StateCancelled, true},
		{StateScheduled, StateActive, false},
		{StateAvailable, StateActive, true},
		{StateAvailable, StateCancelled, true},
		{StateAvailable, StateCompleted, false},
		{StateActive, StateCompleted, true},
		{StateActive, StateRetryable, true},
		{StateActive, StateDiscarded, true},
		{StateActive, StateCancelled, true},
		{StateActive, StateAvailable, false},
		{StateRetryable, StateAvailable, true},
		{StateRetryable, StateCancelled, true},
		{StateRetryable, StateCompleted, false},
		{StateCompleted, StateActive, false},
		{StateCompleted, StateCancelled, false},
		{StateCancelled, StateAvailable, false},
		{StateDiscarded, StateAvailable, false},
		{"invalid_state", StateActive, false},
	}

	for _, tt := range tests {
		t.Run(tt.from+"->"+tt.to, func(t *testing.T) {
			got := IsValidTransition(tt.from, tt.to)
			if got != tt.want {
				t.Errorf("IsValidTransition(%q, %q) = %v, want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func TestIsTerminalState(t *testing.T) {
	tests := []struct {
		state string
		want  bool
	}{
		{StateCompleted, true},
		{StateCancelled, true},
		{StateDiscarded, true},
		{StateAvailable, false},
		{StateActive, false},
		{StateScheduled, false},
		{StateRetryable, false},
		{StatePending, false},
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			got := IsTerminalState(tt.state)
			if got != tt.want {
				t.Errorf("IsTerminalState(%q) = %v, want %v", tt.state, got, tt.want)
			}
		})
	}
}

func TestIsCancellableState(t *testing.T) {
	tests := []struct {
		state string
		want  bool
	}{
		{StateAvailable, true},
		{StateScheduled, true},
		{StatePending, true},
		{StateActive, true},
		{StateRetryable, true},
		{StateCompleted, false},
		{StateCancelled, false},
		{StateDiscarded, false},
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			got := IsCancellableState(tt.state)
			if got != tt.want {
				t.Errorf("IsCancellableState(%q) = %v, want %v", tt.state, got, tt.want)
			}
		})
	}
}
