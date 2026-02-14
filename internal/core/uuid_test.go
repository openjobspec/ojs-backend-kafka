package core

import "testing"

func TestNewUUIDv7(t *testing.T) {
	id := NewUUIDv7()
	if id == "" {
		t.Fatal("NewUUIDv7() returned empty string")
	}
	if !IsValidUUIDv7(id) {
		t.Errorf("NewUUIDv7() = %q, not a valid UUIDv7", id)
	}
	if !IsValidUUID(id) {
		t.Errorf("NewUUIDv7() = %q, not a valid UUID", id)
	}
}

func TestNewUUIDv7_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := NewUUIDv7()
		if seen[id] {
			t.Fatalf("duplicate UUIDv7 generated: %s", id)
		}
		seen[id] = true
	}
}

func TestIsValidUUIDv7(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{NewUUIDv7(), true},
		// Valid UUIDv7 format (version 7, variant 10xx)
		{"01912345-6789-7abc-8def-0123456789ab", true},
		// UUIDv4 (version 4 not 7)
		{"550e8400-e29b-41d4-a716-446655440000", false},
		{"", false},
		{"not-a-uuid", false},
		{"01912345-6789-7abc-0def-0123456789ab", false}, // variant bits wrong
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsValidUUIDv7(tt.input)
			if got != tt.want {
				t.Errorf("IsValidUUIDv7(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsValidUUID(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{NewUUIDv7(), true},
		{"550e8400-e29b-41d4-a716-446655440000", true},
		{"", false},
		{"not-a-uuid", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsValidUUID(tt.input)
			if got != tt.want {
				t.Errorf("IsValidUUID(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
