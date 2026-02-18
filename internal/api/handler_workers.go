package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

type WorkerHandler struct {
	backend   core.Backend
	publisher core.EventPublisher
}

func NewWorkerHandler(backend core.Backend) *WorkerHandler {
	return &WorkerHandler{backend: backend}
}

// SetEventPublisher sets the event publisher for real-time notifications.
func (h *WorkerHandler) SetEventPublisher(pub core.EventPublisher) {
	h.publisher = pub
}

func (h *WorkerHandler) Fetch(w http.ResponseWriter, r *http.Request) {
	var req core.FetchRequest
	if err := decodeBody(r, &req); err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Invalid JSON in request body.", nil))
		return
	}

	if len(req.Queues) == 0 {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'queues' field is required and must not be empty.", nil))
		return
	}

	count := req.Count
	if count <= 0 {
		count = 1
	}

	visTimeout := 0
	if req.VisibilityTimeoutMs != nil && *req.VisibilityTimeoutMs > 0 {
		visTimeout = *req.VisibilityTimeoutMs
	}

	jobs, err := h.backend.Fetch(r.Context(), req.Queues, count, req.WorkerID, visTimeout)
	if err != nil {
		if ojsErr, ok := err.(*core.OJSError); ok {
			WriteError(w, http.StatusInternalServerError, ojsErr)
			return
		}
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	if jobs == nil {
		jobs = []*core.Job{}
	}

	// Publish real-time events for state transition to active
	for _, j := range jobs {
		if h.publisher != nil {
			_ = h.publisher.PublishJobEvent(core.NewStateChangedEvent(
				j.ID, j.Queue, j.Type, core.StateAvailable, core.StateActive,
			))
		}
	}

	resp := map[string]any{"jobs": jobs}
	if len(jobs) > 0 {
		resp["job"] = jobs[0]
	}

	WriteJSON(w, http.StatusOK, resp)
}

func (h *WorkerHandler) Ack(w http.ResponseWriter, r *http.Request) {
	var req core.AckRequest
	if err := decodeBody(r, &req); err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Invalid JSON in request body.", nil))
		return
	}

	if req.JobID == "" {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'job_id' field is required.", nil))
		return
	}

	resp, err := h.backend.Ack(r.Context(), req.JobID, req.Result)
	if err != nil {
		if ojsErr, ok := err.(*core.OJSError); ok {
			switch ojsErr.Code {
			case core.ErrCodeNotFound:
				WriteError(w, http.StatusNotFound, ojsErr)
				return
			case core.ErrCodeInvalidRequest:
				WriteError(w, http.StatusConflict, ojsErr)
				return
			}
		}
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	// Publish real-time event
	if h.publisher != nil && resp.Job != nil {
		_ = h.publisher.PublishJobEvent(core.NewStateChangedEvent(
			resp.JobID, resp.Job.Queue, resp.Job.Type, core.StateActive, core.StateCompleted,
		))
	}

	WriteJSON(w, http.StatusOK, resp)
}

func (h *WorkerHandler) Nack(w http.ResponseWriter, r *http.Request) {
	var req core.NackRequest
	if err := decodeBody(r, &req); err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Invalid JSON in request body.", nil))
		return
	}

	if req.JobID == "" {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'job_id' field is required.", nil))
		return
	}

	resp, err := h.backend.Nack(r.Context(), req.JobID, req.Error, req.Requeue)
	if err != nil {
		if ojsErr, ok := err.(*core.OJSError); ok {
			switch ojsErr.Code {
			case core.ErrCodeNotFound:
				WriteError(w, http.StatusNotFound, ojsErr)
				return
			case core.ErrCodeInvalidRequest:
				WriteError(w, http.StatusConflict, ojsErr)
				return
			}
		}
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	// Publish real-time event
	if h.publisher != nil && resp.Job != nil {
		_ = h.publisher.PublishJobEvent(core.NewStateChangedEvent(
			resp.JobID, resp.Job.Queue, resp.Job.Type, core.StateActive, resp.State,
		))
	}

	WriteJSON(w, http.StatusOK, resp)
}

func (h *WorkerHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	var req core.HeartbeatRequest
	if err := decodeBody(r, &req); err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Invalid JSON in request body.", nil))
		return
	}

	if req.WorkerID == "" {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'worker_id' field is required.", nil))
		return
	}

	activeJobs := req.ActiveJobs
	if req.JobID != "" && len(activeJobs) == 0 {
		activeJobs = []string{req.JobID}
	}

	visTimeout := core.DefaultVisibilityTimeoutMs
	if req.VisibilityTimeoutMs != nil && *req.VisibilityTimeoutMs > 0 {
		visTimeout = *req.VisibilityTimeoutMs
	}

	resp, err := h.backend.Heartbeat(r.Context(), req.WorkerID, activeJobs, visTimeout)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	WriteJSON(w, http.StatusOK, resp)
}

func decodeBody(r *http.Request, v any) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, v)
}
