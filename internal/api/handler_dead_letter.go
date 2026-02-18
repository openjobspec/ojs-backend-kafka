package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// DeadLetterHandler handles dead letter queue HTTP endpoints.
// Delegates to the shared commonapi.DeadLetterHandler.
type DeadLetterHandler = commonapi.DeadLetterHandler

// NewDeadLetterHandler creates a new DeadLetterHandler.
func NewDeadLetterHandler(backend core.Backend) *DeadLetterHandler {
	return commonapi.NewDeadLetterHandler(backend)
}
