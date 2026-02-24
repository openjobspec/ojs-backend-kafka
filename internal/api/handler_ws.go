package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// WSBridgeHandler provides WebSocket-like real-time updates via SSE + POST commands.
type WSBridgeHandler = commonapi.WSBridgeHandler

// NewWSBridgeHandler creates a new WSBridgeHandler.
func NewWSBridgeHandler(subscriber core.EventSubscriber) *WSBridgeHandler {
	return commonapi.NewWSBridgeHandler(subscriber)
}
