package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

type QueueHandler struct {
	backend core.Backend
}

func NewQueueHandler(backend core.Backend) *QueueHandler {
	return &QueueHandler{backend: backend}
}

func (h *QueueHandler) List(w http.ResponseWriter, r *http.Request) {
	queues, err := h.backend.ListQueues(r.Context())
	if err != nil {
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"queues": queues,
		"pagination": map[string]any{
			"total":    len(queues),
			"limit":    50,
			"offset":   0,
			"has_more": false,
		},
	})
}

func (h *QueueHandler) Stats(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	stats, err := h.backend.QueueStats(r.Context(), name)
	if err != nil {
		if ojsErr, ok := err.(*core.OJSError); ok {
			if ojsErr.Code == core.ErrCodeNotFound {
				WriteError(w, http.StatusNotFound, ojsErr)
				return
			}
		}
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"queue": map[string]any{
			"name":      stats.Queue,
			"available": stats.Stats.Available,
			"active":    stats.Stats.Active,
			"completed": stats.Stats.Completed,
			"paused":    stats.Status == "paused",
		},
	})
}

func (h *QueueHandler) Pause(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	if err := h.backend.PauseQueue(r.Context(), name); err != nil {
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"queue": map[string]any{
			"name":   name,
			"paused": true,
		},
	})
}

func (h *QueueHandler) Resume(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	if err := h.backend.ResumeQueue(r.Context(), name); err != nil {
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"queue": map[string]any{
			"name":   name,
			"paused": false,
		},
	})
}
