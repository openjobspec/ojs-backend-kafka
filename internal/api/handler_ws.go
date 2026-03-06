package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"nhooyr.io/websocket"

	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	commoncore "github.com/openjobspec/ojs-go-backend-common/core"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// WSBridgeHandler provides WebSocket-like real-time updates via SSE + POST commands.
type WSBridgeHandler = commonapi.WSBridgeHandler

// NewWSBridgeHandler creates a new WSBridgeHandler.
func NewWSBridgeHandler(subscriber core.EventSubscriber) *WSBridgeHandler {
	return commonapi.NewWSBridgeHandler(subscriber)
}

const (
	wsPingInterval = 30 * time.Second
	wsPongTimeout  = 10 * time.Second
	wsWriteTimeout = 5 * time.Second
)

// WSHandler handles WebSocket connections for real-time job updates.
type WSHandler struct {
	backend    core.Backend
	subscriber core.EventSubscriber
	eventID    atomic.Uint64
}

// NewWSHandler creates a new WSHandler.
func NewWSHandler(backend core.Backend, subscriber core.EventSubscriber) *WSHandler {
	return &WSHandler{
		backend:    backend,
		subscriber: subscriber,
	}
}

type wsMessage struct {
	Action  string `json:"action,omitempty"`
	Channel string `json:"channel,omitempty"`
}

type wsResponse struct {
	Type      string          `json:"type"`
	Channel   string          `json:"channel,omitempty"`
	Event     string          `json:"event,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	ID        string          `json:"id,omitempty"`
	Timestamp string          `json:"timestamp,omitempty"`
	Code      string          `json:"code,omitempty"`
	Message   string          `json:"message,omitempty"`
}

type wsSubscription struct {
	channel string
	cancel  func()
}

// Handle handles GET /ojs/v1/ws
func (h *WSHandler) Handle(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{"ojs.v1"},
	})
	if err != nil {
		slog.Error("websocket accept failed", "error", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "connection closed")

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	subs := &sync.Map{}
	var mu sync.Mutex

	// Start ping loop
	go h.pingLoop(ctx, conn)

	for {
		_, msgBytes, err := conn.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) != -1 {
				slog.Debug("websocket closed", "status", websocket.CloseStatus(err))
			}
			// Clean up all subscriptions
			subs.Range(func(key, value any) bool {
				if sub, ok := value.(*wsSubscription); ok {
					sub.cancel()
				}
				return true
			})
			return
		}

		var msg wsMessage
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			h.writeJSON(ctx, conn, wsResponse{
				Type:    "error",
				Code:    "invalid_request",
				Message: "Invalid JSON message.",
			})
			continue
		}

		mu.Lock()
		switch msg.Action {
		case "subscribe":
			h.handleSubscribe(ctx, conn, subs, msg.Channel)
		case "unsubscribe":
			h.handleUnsubscribe(ctx, conn, subs, msg.Channel)
		default:
			h.writeJSON(ctx, conn, wsResponse{
				Type:    "error",
				Code:    "invalid_request",
				Message: "Unknown action. Use 'subscribe' or 'unsubscribe'.",
			})
		}
		mu.Unlock()
	}
}

func (h *WSHandler) handleSubscribe(ctx context.Context, conn *websocket.Conn, subs *sync.Map, channel string) {
	// Check if already subscribed
	if _, exists := subs.Load(channel); exists {
		h.writeJSON(ctx, conn, wsResponse{
			Type:    "subscribed",
			Channel: channel,
		})
		return
	}

	var (
		ch    <-chan *commoncore.JobEvent
		unsub func()
		err   error
	)

	switch {
	case strings.HasPrefix(channel, "job:"):
		jobID := strings.TrimPrefix(channel, "job:")
		if _, infoErr := h.backend.Info(ctx, jobID); infoErr != nil {
			h.writeJSON(ctx, conn, wsResponse{
				Type:    "error",
				Code:    "not_found",
				Message: "Job not found.",
			})
			return
		}
		ch, unsub, err = h.subscriber.SubscribeJob(jobID)
	case strings.HasPrefix(channel, "queue:"):
		queueName := strings.TrimPrefix(channel, "queue:")
		if _, statsErr := h.backend.QueueStats(ctx, queueName); statsErr != nil {
			h.writeJSON(ctx, conn, wsResponse{
				Type:    "error",
				Code:    "not_found",
				Message: "Queue not found.",
			})
			return
		}
		ch, unsub, err = h.subscriber.SubscribeQueue(queueName)
	case channel == "all":
		ch, unsub, err = h.subscriber.SubscribeAll()
	default:
		h.writeJSON(ctx, conn, wsResponse{
			Type:    "error",
			Code:    "invalid_request",
			Message: "Invalid channel. Use 'job:{id}', 'queue:{name}', or 'all'.",
		})
		return
	}

	if err != nil {
		h.writeJSON(ctx, conn, wsResponse{
			Type:    "error",
			Code:    "internal_error",
			Message: "Failed to subscribe.",
		})
		return
	}

	subs.Store(channel, &wsSubscription{
		channel: channel,
		cancel:  unsub,
	})

	h.writeJSON(ctx, conn, wsResponse{
		Type:    "subscribed",
		Channel: channel,
	})

	// Forward events to websocket
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				data, err := json.Marshal(event)
				if err != nil {
					slog.Warn("failed to marshal WebSocket event", "error", err)
					continue
				}
				id := h.eventID.Add(1)
				h.writeJSON(ctx, conn, wsResponse{
					Type:      "event",
					Channel:   channel,
					Event:     event.EventType,
					Data:      data,
					ID:        formatEventID(id),
					Timestamp: event.Timestamp,
				})
			}
		}
	}()
}

func (h *WSHandler) handleUnsubscribe(ctx context.Context, conn *websocket.Conn, subs *sync.Map, channel string) {
	if val, ok := subs.LoadAndDelete(channel); ok {
		if sub, ok := val.(*wsSubscription); ok {
			sub.cancel()
		}
	}

	h.writeJSON(ctx, conn, wsResponse{
		Type:    "unsubscribed",
		Channel: channel,
	})
}

func (h *WSHandler) writeJSON(ctx context.Context, conn *websocket.Conn, msg wsResponse) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
	defer cancel()
	if err := conn.Write(writeCtx, websocket.MessageText, data); err != nil {
		slog.Debug("websocket write failed", "error", err)
	}
}

func (h *WSHandler) pingLoop(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, wsPongTimeout)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				slog.Debug("websocket ping failed", "error", err)
				return
			}
		}
	}
}

func formatEventID(id uint64) string {
	return "evt_" + formatUint(id)
}

func formatUint(n uint64) string {
	s := ""
	for i := 3; i >= 0; i-- {
		digit := (n / pow10(uint64(i))) % 10
		s += string(rune('0' + digit))
	}
	return s
}

func pow10(n uint64) uint64 {
	result := uint64(1)
	for i := uint64(0); i < n; i++ {
		result *= 10
	}
	return result
}
