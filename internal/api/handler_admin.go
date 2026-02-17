package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// AdminHandler handles /ojs/v1/admin/* control-plane endpoints.
type AdminHandler struct {
	backend   core.Backend
	startedAt time.Time
}

// NewAdminHandler creates a new AdminHandler.
func NewAdminHandler(backend core.Backend) *AdminHandler {
	return &AdminHandler{backend: backend, startedAt: time.Now()}
}

// --- Stats ---

// Stats handles GET /ojs/v1/admin/stats
func (h *AdminHandler) Stats(w http.ResponseWriter, r *http.Request) {
	queues, err := h.backend.ListQueues(r.Context())
	if err != nil {
		HandleError(w, err)
		return
	}

	var totalStats core.Stats
	for _, q := range queues {
		qs, err := h.backend.QueueStats(r.Context(), q.Name)
		if err != nil {
			continue
		}
		totalStats.Available += qs.Stats.Available
		totalStats.Active += qs.Stats.Active
		totalStats.Completed += qs.Stats.Completed
		totalStats.Scheduled += qs.Stats.Scheduled
		totalStats.Retryable += qs.Stats.Retryable
		totalStats.Dead += qs.Stats.Dead
	}

	_, workerSummary, err := h.backend.ListWorkers(r.Context(), 1, 0)
	if err != nil {
		workerSummary = core.WorkerSummary{}
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"queues":  len(queues),
		"workers": workerSummary.Total,
		"jobs": map[string]any{
			"available": totalStats.Available,
			"active":    totalStats.Active,
			"scheduled": totalStats.Scheduled,
			"retryable": totalStats.Retryable,
			"completed": totalStats.Completed,
			"discarded": totalStats.Dead,
			"cancelled": 0,
		},
		"throughput": map[string]any{
			"processed_per_minute": 0,
			"failed_per_minute":    0,
		},
		"uptime_seconds": int64(time.Since(h.startedAt).Seconds()),
	})
}

// --- Queues ---

// ListQueues handles GET /ojs/v1/admin/queues
func (h *AdminHandler) ListQueues(w http.ResponseWriter, r *http.Request) {
	queues, err := h.backend.ListQueues(r.Context())
	if err != nil {
		HandleError(w, err)
		return
	}

	items := make([]map[string]any, 0, len(queues))
	for _, q := range queues {
		qs, err := h.backend.QueueStats(r.Context(), q.Name)
		entry := map[string]any{
			"name":   q.Name,
			"paused": q.Status == "paused",
			"counts": map[string]any{
				"available": 0, "active": 0, "scheduled": 0,
				"retryable": 0, "completed": 0, "discarded": 0, "cancelled": 0,
			},
		}
		if err == nil {
			entry["counts"] = map[string]any{
				"available": qs.Stats.Available,
				"active":    qs.Stats.Active,
				"scheduled": qs.Stats.Scheduled,
				"retryable": qs.Stats.Retryable,
				"completed": qs.Stats.Completed,
				"discarded": qs.Stats.Dead,
				"cancelled": 0,
			}
		}
		items = append(items, entry)
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"pagination": map[string]any{
			"total":    len(items),
			"page":     1,
			"per_page": len(items),
		},
	})
}

// GetQueue handles GET /ojs/v1/admin/queues/{name}
func (h *AdminHandler) GetQueue(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	qs, err := h.backend.QueueStats(r.Context(), name)
	if err != nil {
		HandleError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"name":   qs.Queue,
		"paused": qs.Status == "paused",
		"counts": map[string]any{
			"available": qs.Stats.Available,
			"active":    qs.Stats.Active,
			"scheduled": qs.Stats.Scheduled,
			"retryable": qs.Stats.Retryable,
			"completed": qs.Stats.Completed,
			"discarded": qs.Stats.Dead,
			"cancelled": 0,
		},
	})
}

// PauseQueue handles POST /ojs/v1/admin/queues/{name}/pause
func (h *AdminHandler) PauseQueue(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := h.backend.PauseQueue(r.Context(), name); err != nil {
		HandleError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, map[string]any{"name": name, "paused": true})
}

// ResumeQueue handles POST /ojs/v1/admin/queues/{name}/resume
func (h *AdminHandler) ResumeQueue(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := h.backend.ResumeQueue(r.Context(), name); err != nil {
		HandleError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, map[string]any{"name": name, "paused": false})
}

// --- Jobs ---

// ListJobs handles GET /ojs/v1/admin/jobs
func (h *AdminHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	perPage, _ := strconv.Atoi(q.Get("per_page"))
	if perPage < 1 || perPage > 100 {
		perPage = 25
	}
	offset := (page - 1) * perPage

	filters := core.JobListFilters{
		State:    q.Get("state"),
		Queue:    q.Get("queue"),
		Type:     q.Get("type"),
		WorkerID: q.Get("worker_id"),
	}

	jobs, total, err := h.backend.ListJobs(r.Context(), filters, perPage, offset)
	if err != nil {
		HandleError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"items": jobs,
		"pagination": map[string]any{
			"total":    total,
			"page":     page,
			"per_page": perPage,
		},
	})
}

// GetJob handles GET /ojs/v1/admin/jobs/{id}
func (h *AdminHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := h.backend.Info(r.Context(), id)
	if err != nil {
		HandleError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, job)
}

// RetryJob handles POST /ojs/v1/admin/jobs/{id}/retry
func (h *AdminHandler) RetryJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := h.backend.RetryDeadLetter(r.Context(), id)
	if err != nil {
		HandleError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, job)
}

// CancelJob handles POST /ojs/v1/admin/jobs/{id}/cancel
func (h *AdminHandler) CancelJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := h.backend.Cancel(r.Context(), id)
	if err != nil {
		HandleError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, job)
}

// BulkRetry handles POST /ojs/v1/admin/jobs/bulk/retry
func (h *AdminHandler) BulkRetry(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Filter  map[string]any `json:"filter"`
		Confirm bool           `json:"confirm"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("invalid request body", nil))
		return
	}
	if !req.Confirm {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("confirm must be true", nil))
		return
	}

	// Retry all dead-letter jobs (filter not yet supported at the backend level)
	jobs, total, err := h.backend.ListDeadLetter(r.Context(), 1000, 0)
	if err != nil {
		HandleError(w, err)
		return
	}

	succeeded := 0
	for _, j := range jobs {
		if _, err := h.backend.RetryDeadLetter(r.Context(), j.ID); err == nil {
			succeeded++
		}
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"action":    "retry",
		"matched":   total,
		"succeeded": succeeded,
		"failed":    total - succeeded,
	})
}

// --- Workers ---

// ListWorkers handles GET /ojs/v1/admin/workers
func (h *AdminHandler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	perPage, _ := strconv.Atoi(q.Get("per_page"))
	if perPage < 1 || perPage > 100 {
		perPage = 50
	}
	offset := (page - 1) * perPage

	items, summary, err := h.backend.ListWorkers(r.Context(), perPage, offset)
	if err != nil {
		HandleError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"summary": map[string]any{
			"total":   summary.Total,
			"running": summary.Running,
			"quiet":   summary.Quiet,
			"stale":   summary.Stale,
		},
		"pagination": map[string]any{
			"total":    summary.Total,
			"page":     page,
			"per_page": perPage,
		},
	})
}

// QuietWorker handles POST /ojs/v1/admin/workers/{id}/quiet
func (h *AdminHandler) QuietWorker(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.backend.SetWorkerState(r.Context(), id, "quiet"); err != nil {
		HandleError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, map[string]any{"worker_id": id, "state": "quiet"})
}

// --- Dead Letter ---

// ListDeadLetter handles GET /ojs/v1/admin/dead-letter
func (h *AdminHandler) ListDeadLetter(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	perPage, _ := strconv.Atoi(q.Get("per_page"))
	if perPage < 1 || perPage > 100 {
		perPage = 25
	}

	offset := (page - 1) * perPage
	jobs, total, err := h.backend.ListDeadLetter(r.Context(), perPage, offset)
	if err != nil {
		HandleError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"items": jobs,
		"pagination": map[string]any{
			"total":    total,
			"page":     page,
			"per_page": perPage,
		},
	})
}

// DeadLetterStats handles GET /ojs/v1/admin/dead-letter/stats
func (h *AdminHandler) DeadLetterStats(w http.ResponseWriter, r *http.Request) {
	_, total, err := h.backend.ListDeadLetter(r.Context(), 0, 0)
	if err != nil {
		HandleError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, map[string]any{
		"total": total,
	})
}

// RetryDeadLetter handles POST /ojs/v1/admin/dead-letter/{id}/retry
func (h *AdminHandler) RetryDeadLetter(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := h.backend.RetryDeadLetter(r.Context(), id)
	if err != nil {
		HandleError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, job)
}

// DeleteDeadLetter handles DELETE /ojs/v1/admin/dead-letter/{id}
func (h *AdminHandler) DeleteDeadLetter(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.backend.DeleteDeadLetter(r.Context(), id); err != nil {
		HandleError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// BulkRetryDeadLetter handles POST /ojs/v1/admin/dead-letter/retry
func (h *AdminHandler) BulkRetryDeadLetter(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Filter  map[string]any `json:"filter"`
		Confirm bool           `json:"confirm"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("invalid request body", nil))
		return
	}

	jobs, total, err := h.backend.ListDeadLetter(r.Context(), 1000, 0)
	if err != nil {
		HandleError(w, err)
		return
	}

	succeeded := 0
	for _, j := range jobs {
		if _, err := h.backend.RetryDeadLetter(r.Context(), j.ID); err == nil {
			succeeded++
		}
	}

	WriteJSON(w, http.StatusOK, map[string]any{
		"action":    "retry",
		"matched":   total,
		"succeeded": succeeded,
		"failed":    total - succeeded,
	})
}
