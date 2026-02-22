package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// CronHandler handles cron-related HTTP endpoints.
// Delegates to the shared commonapi.CronHandler.
type CronHandler = commonapi.CronHandler

// NewCronHandler creates a new CronHandler.
func NewCronHandler(backend core.Backend) *CronHandler {
	return commonapi.NewCronHandler(backend)
}
