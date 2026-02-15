package api

import (
	"net/http"
	"strconv"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

type BatchHandler struct {
	backend core.Backend
}

func NewBatchHandler(backend core.Backend) *BatchHandler {
	return &BatchHandler{backend: backend}
}

func (h *BatchHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req core.BatchEnqueueRequest
	if err := decodeBody(r, &req); err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Invalid JSON in request body.", nil))
		return
	}

	if len(req.Jobs) == 0 {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'jobs' field is required and must not be empty.", nil))
		return
	}

	var jobs []*core.Job
	for i, raw := range req.Jobs {
		enqReq, err := core.ParseEnqueueRequest(raw)
		if err != nil {
			WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError(
				"Invalid JSON in job at index "+strconv.Itoa(i)+".",
				map[string]any{"index": i},
			))
			return
		}

		if ojsErr := core.ValidateEnqueueRequest(enqReq); ojsErr != nil {
			ojsErr.Message = "Batch validation failed: job at index " + intToStr(i) + " - " + ojsErr.Message
			if ojsErr.Details == nil {
				ojsErr.Details = make(map[string]any)
			}
			ojsErr.Details["index"] = i
			WriteError(w, http.StatusBadRequest, ojsErr)
			return
		}

		jobs = append(jobs, requestToJob(enqReq))
	}

	created, err := h.backend.PushBatch(r.Context(), jobs)
	if err != nil {
		if ojsErr, ok := err.(*core.OJSError); ok {
			WriteError(w, http.StatusBadRequest, ojsErr)
			return
		}
		WriteError(w, http.StatusInternalServerError, core.NewInternalError(err.Error()))
		return
	}

	WriteJSON(w, http.StatusCreated, map[string]any{
		"jobs":  created,
		"count": len(created),
	})
}

func intToStr(i int) string {
	return strconv.Itoa(i)
}
