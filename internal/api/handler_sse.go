package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// SSEHandler handles Server-Sent Events endpoints for real-time job updates.
// Delegates to the shared commonapi.SSEHandler.
type SSEHandler = commonapi.SSEHandler

// NewSSEHandler creates a new SSEHandler.
func NewSSEHandler(backend core.Backend, subscriber core.EventSubscriber) *SSEHandler {
	return commonapi.NewSSEHandler(backend, subscriber)
}
