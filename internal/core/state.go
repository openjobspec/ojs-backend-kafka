package core

// Job states as defined by the OJS specification.
const (
	StateAvailable = "available"
	StateScheduled = "scheduled"
	StatePending   = "pending"
	StateActive    = "active"
	StateRetryable = "retryable"
	StateCompleted = "completed"
	StateCancelled = "cancelled"
	StateDiscarded = "discarded"
)

// validTransitions defines the allowed state transitions.
var validTransitions = map[string][]string{
	StateAvailable: {StateActive, StateCancelled},
	StateScheduled: {StateAvailable, StateCancelled},
	StatePending:   {StateAvailable, StateCancelled},
	StateActive:    {StateCompleted, StateRetryable, StateDiscarded, StateCancelled},
	StateRetryable: {StateAvailable, StateCancelled},
	StateCompleted: {},
	StateCancelled: {},
	StateDiscarded: {},
}

// IsValidTransition checks if a state transition is allowed.
func IsValidTransition(from, to string) bool {
	targets, ok := validTransitions[from]
	if !ok {
		return false
	}
	for _, t := range targets {
		if t == to {
			return true
		}
	}
	return false
}

// IsTerminalState returns true if the state is terminal (no further transitions).
func IsTerminalState(state string) bool {
	return state == StateCompleted || state == StateCancelled || state == StateDiscarded
}

// IsCancellableState returns true if the job can be cancelled from this state.
func IsCancellableState(state string) bool {
	return state == StateAvailable || state == StateScheduled || state == StatePending ||
		state == StateActive || state == StateRetryable
}
