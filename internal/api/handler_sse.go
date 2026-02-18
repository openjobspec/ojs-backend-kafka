package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

const (
	sseHeartbeatInterval = 15 * time.Second
	sseRetryMs           = 3000
)

// SSEHandler handles Server-Sent Events endpoints for real-time job updates.
type SSEHandler struct {
	backend    core.Backend
	subscriber core.EventSubscriber
	eventID    atomic.Uint64
}

// NewSSEHandler creates a new SSEHandler.
func NewSSEHandler(backend core.Backend, subscriber core.EventSubscriber) *SSEHandler {
	return &SSEHandler{
		backend:    backend,
		subscriber: subscriber,
	}
}

// JobEvents handles GET /ojs/v1/jobs/{id}/events
func (h *SSEHandler) JobEvents(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")

	// Verify job exists
	job, err := h.backend.Info(r.Context(), jobID)
	if err != nil {
		HandleError(w, err)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		WriteError(w, http.StatusInternalServerError, core.NewInternalError("Streaming not supported."))
		return
	}

	ch, unsub, err := h.subscriber.SubscribeJob(jobID)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, core.NewInternalError("Failed to subscribe to job events."))
		return
	}
	defer unsub()

	h.initSSEHeaders(w)
	flusher.Flush()

	// If job is in terminal state, send current state and close
	if core.IsTerminalState(job.State) {
		evt := core.NewStateChangedEvent(job.ID, job.Queue, job.Type, job.State, job.State)
		h.writeSSEEvent(w, evt)
		flusher.Flush()
		return
	}

	h.streamEvents(w, r, flusher, ch)
}

// QueueEvents handles GET /ojs/v1/queues/{name}/events
func (h *SSEHandler) QueueEvents(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "name")

	// Verify queue exists
	_, err := h.backend.QueueStats(r.Context(), queueName)
	if err != nil {
		HandleError(w, err)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		WriteError(w, http.StatusInternalServerError, core.NewInternalError("Streaming not supported."))
		return
	}

	ch, unsub, err := h.subscriber.SubscribeQueue(queueName)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, core.NewInternalError("Failed to subscribe to queue events."))
		return
	}
	defer unsub()

	h.initSSEHeaders(w)
	flusher.Flush()

	h.streamEvents(w, r, flusher, ch)
}

func (h *SSEHandler) initSSEHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	fmt.Fprintf(w, "retry: %d\n\n", sseRetryMs)
}

func (h *SSEHandler) streamEvents(w http.ResponseWriter, r *http.Request, flusher http.Flusher, ch <-chan *core.JobEvent) {
	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			h.writeSSEEvent(w, event)
			flusher.Flush()
		case <-heartbeat.C:
			fmt.Fprint(w, ":heartbeat\n\n")
			flusher.Flush()
		}
	}
}

func (h *SSEHandler) writeSSEEvent(w http.ResponseWriter, event *core.JobEvent) {
	id := h.eventID.Add(1)
	data, _ := json.Marshal(event)
	fmt.Fprintf(w, "id: evt_%04d\nevent: %s\ndata: %s\n\n", id, event.EventType, data)
}
