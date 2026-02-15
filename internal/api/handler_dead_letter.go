package api

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

type DeadLetterHandler struct {
	backend core.Backend
}

func NewDeadLetterHandler(backend core.Backend) *DeadLetterHandler {
	return &DeadLetterHandler{backend: backend}
}

func (h *DeadLetterHandler) List(w http.ResponseWriter, r *http.Request) {
	limit := 50
	offset := 0

	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	jobs, total, err := h.backend.ListDeadLetter(r.Context(), limit, offset)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"jobs": jobs,
		"pagination": map[string]any{
			"total":    total,
			"limit":    limit,
			"offset":   offset,
			"has_more": offset+limit < total,
		},
	})
}

func (h *DeadLetterHandler) Retry(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	job, err := h.backend.RetryDeadLetter(r.Context(), id)
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

	WriteJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (h *DeadLetterHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	err := h.backend.DeleteDeadLetter(r.Context(), id)
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

	WriteJSON(w, http.StatusOK, map[string]any{"deleted": true, "job_id": id})
}
