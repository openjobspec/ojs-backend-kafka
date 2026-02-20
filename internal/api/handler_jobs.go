package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// JobHandler handles job-related HTTP endpoints.
// Delegates to the shared commonapi.JobHandler.
type JobHandler = commonapi.JobHandler

// NewJobHandler creates a new JobHandler.
func NewJobHandler(backend core.Backend) *JobHandler {
	return commonapi.NewJobHandler(backend)
}

// RequestToJob converts an EnqueueRequest into a Job for backend Push.
var RequestToJob = commonapi.RequestToJob
