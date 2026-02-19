package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// QueueHandler handles queue-related HTTP endpoints.
// Delegates to the shared commonapi.QueueHandler.
type QueueHandler = commonapi.QueueHandler

// NewQueueHandler creates a new QueueHandler.
func NewQueueHandler(backend core.Backend) *QueueHandler {
	return commonapi.NewQueueHandler(backend)
}
