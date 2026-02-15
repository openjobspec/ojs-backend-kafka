package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

type WorkflowHandler struct {
	backend core.Backend
}

func NewWorkflowHandler(backend core.Backend) *WorkflowHandler {
	return &WorkflowHandler{backend: backend}
}

func (h *WorkflowHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req core.WorkflowRequest
	if err := decodeBody(r, &req); err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Invalid JSON in request body.", nil))
		return
	}

	if req.Type == "" {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'type' field is required (chain, group, or batch).", nil))
		return
	}

	switch req.Type {
	case "chain":
		if len(req.Steps) == 0 {
			WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'steps' field is required for chain workflows.", nil))
			return
		}
	case "group", "batch":
		if len(req.Jobs) == 0 {
			WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'jobs' field is required for group/batch workflows.", nil))
			return
		}
	default:
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Invalid workflow type. Must be 'chain', 'group', or 'batch'.", nil))
		return
	}

	created, err := h.backend.CreateWorkflow(r.Context(), &req)
	if err != nil {
		if ojsErr, ok := err.(*core.OJSError); ok {
			WriteError(w, http.StatusBadRequest, ojsErr)
			return
		}
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	WriteJSON(w, http.StatusCreated, map[string]any{"workflow": created})
}

func (h *WorkflowHandler) Get(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	wf, err := h.backend.GetWorkflow(r.Context(), id)
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

	WriteJSON(w, http.StatusOK, map[string]any{"workflow": wf})
}

func (h *WorkflowHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	wf, err := h.backend.CancelWorkflow(r.Context(), id)
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

	WriteJSON(w, http.StatusOK, map[string]any{"workflow": wf})
}
