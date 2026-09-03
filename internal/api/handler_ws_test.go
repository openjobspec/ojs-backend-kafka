package api

import "testing"

// formatEventID must produce a stable, collision-free identifier for every
// uint64 sequence value. The previous implementation only emitted the last
// four decimal digits, so IDs wrapped/collided after 9999 (e.g. 10001 -> 0001).
func TestFormatEventID(t *testing.T) {
	tests := []struct {
		id   uint64
		want string
	}{
		{1, "evt_1"},
		{42, "evt_42"},
		{9999, "evt_9999"},
		{10000, "evt_10000"},
		{12345, "evt_12345"},
		{1234567890, "evt_1234567890"},
	}
	for _, tt := range tests {
		if got := formatEventID(tt.id); got != tt.want {
			t.Errorf("formatEventID(%d) = %q, want %q", tt.id, got, tt.want)
		}
	}
}

// Distinct sequence values must map to distinct event IDs, including across the
// 4-digit boundary that previously caused collisions.
func TestFormatEventID_NoCollisionAcrossBoundary(t *testing.T) {
	seen := make(map[string]uint64)
	for _, id := range []uint64{1, 9999, 10000, 10001, 20001} {
		got := formatEventID(id)
		if prev, ok := seen[got]; ok {
			t.Fatalf("formatEventID collision: id %d and %d both produced %q", prev, id, got)
		}
		seen[got] = id
	}
}
