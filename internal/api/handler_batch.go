package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// BatchHandler handles batch enqueue HTTP endpoints.
// Delegates to the shared commonapi.BatchHandler.
type BatchHandler = commonapi.BatchHandler

// NewBatchHandler creates a new BatchHandler.
func NewBatchHandler(backend core.Backend) *BatchHandler {
	return commonapi.NewBatchHandler(backend)
}
