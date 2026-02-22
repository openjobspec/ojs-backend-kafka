package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// AdminHandler handles /ojs/v1/admin/* control-plane endpoints.
// Delegates to the shared commonapi.AdminHandler.
type AdminHandler = commonapi.AdminHandler

// NewAdminHandler creates a new AdminHandler.
func NewAdminHandler(backend core.Backend) *AdminHandler {
	return commonapi.NewAdminHandler(backend)
}
