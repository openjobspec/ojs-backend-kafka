package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// WorkerHandler handles worker-related HTTP endpoints.
// Delegates to the shared commonapi.WorkerHandler.
type WorkerHandler = commonapi.WorkerHandler

// NewWorkerHandler creates a new WorkerHandler.
func NewWorkerHandler(backend core.Backend) *WorkerHandler {
	return commonapi.NewWorkerHandler(backend)
}
