package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// WorkflowHandler handles workflow-related HTTP endpoints.
// Delegates to the shared commonapi.WorkflowHandler.
type WorkflowHandler = commonapi.WorkflowHandler

// NewWorkflowHandler creates a new WorkflowHandler.
func NewWorkflowHandler(backend core.Backend) *WorkflowHandler {
	return commonapi.NewWorkflowHandler(backend)
}
