package core

import (
	"regexp"

	"github.com/google/uuid"
)

var uuidV7Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// NewUUIDv7 generates a new UUIDv7.
func NewUUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		// Fallback to UUID v4 if v7 fails (should not happen)
		return uuid.New().String()
	}
	return id.String()
}

// IsValidUUIDv7 checks if a string is a valid UUIDv7.
func IsValidUUIDv7(s string) bool {
	return uuidV7Pattern.MatchString(s)
}

// IsValidUUID checks if a string is a valid UUID (any version).
func IsValidUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}
