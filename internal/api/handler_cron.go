package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

type CronHandler struct {
	backend core.Backend
}

func NewCronHandler(backend core.Backend) *CronHandler {
	return &CronHandler{backend: backend}
}

func (h *CronHandler) List(w http.ResponseWriter, r *http.Request) {
	crons, err := h.backend.ListCron(r.Context())
	if err != nil {
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	if crons == nil {
		crons = []*core.CronJob{}
	}

	WriteJSON(w, http.StatusOK, map[string]any{"crons": crons})
}

func (h *CronHandler) Register(w http.ResponseWriter, r *http.Request) {
	var cronReq core.CronJob
	if err := decodeBody(r, &cronReq); err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Invalid JSON in request body.", nil))
		return
	}

	if cronReq.Name == "" {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'name' field is required.", nil))
		return
	}
	if cronReq.Expression == "" {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'expression' field is required.", nil))
		return
	}
	if cronReq.JobTemplate == nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'job_template' field is required.", nil))
		return
	}

	cronReq.JobType = cronReq.JobTemplate.Type
	cronReq.Args = cronReq.JobTemplate.Args
	if cronReq.JobTemplate.Options != nil {
		cronReq.Queue = cronReq.JobTemplate.Options.Queue
	}
	if cronReq.Queue == "" {
		cronReq.Queue = "default"
	}
	cronReq.Schedule = cronReq.Expression

	cronReq.Enabled = true

	created, err := h.backend.RegisterCron(r.Context(), &cronReq)
	if err != nil {
		if ojsErr, ok := err.(*core.OJSError); ok {
			WriteError(w, http.StatusBadRequest, ojsErr)
			return
		}
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	WriteJSON(w, http.StatusCreated, map[string]any{"cron": created})
}

func (h *CronHandler) Delete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	cron, err := h.backend.DeleteCron(r.Context(), name)
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

	WriteJSON(w, http.StatusOK, map[string]any{"cron": cron})
}
