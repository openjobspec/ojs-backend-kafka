package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

func newAdminTestRouter(backend core.Backend) *chi.Mux {
	r := chi.NewRouter()
	h := NewAdminHandler(backend)
	r.Get("/ojs/v1/admin/stats", h.Stats)
	r.Get("/ojs/v1/admin/jobs", h.ListJobs)
	r.Get("/ojs/v1/admin/workers", h.ListWorkers)
	r.Post("/ojs/v1/admin/jobs/bulk/retry", h.BulkRetry)
	return r
}

func TestAdminListJobs_UsesBackendFiltersAndPagination(t *testing.T) {
	var gotFilters core.JobListFilters
	gotLimit := 0
	gotOffset := 0
	backend := &mockBackend{
		listJobsFunc: func(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error) {
			gotFilters = filters
			gotLimit = limit
			gotOffset = offset
			return []*core.Job{
				{ID: "job-1", Type: "email.send", State: core.StateActive, Queue: "default"},
			}, 3, nil
		},
	}
	router := newAdminTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/admin/jobs?page=2&per_page=10&state=active&queue=default&type=email.send&worker_id=w-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	items, _ := resp["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d items", len(items))
	}

	pagination, _ := resp["pagination"].(map[string]any)
	if pagination["page"] != float64(2) {
		t.Errorf("page = %v, want 2", pagination["page"])
	}
	if pagination["per_page"] != float64(10) {
		t.Errorf("per_page = %v, want 10", pagination["per_page"])
	}
	if pagination["total"] != float64(3) {
		t.Errorf("total = %v, want 3", pagination["total"])
	}
	if gotLimit != 10 {
		t.Errorf("backend limit = %d, want 10", gotLimit)
	}
	if gotOffset != 10 {
		t.Errorf("backend offset = %d, want 10", gotOffset)
	}
	if gotFilters.State != "active" || gotFilters.Queue != "default" || gotFilters.Type != "email.send" || gotFilters.WorkerID != "w-1" {
		t.Errorf("unexpected filters: %+v", gotFilters)
	}
}

func TestAdminListJobs_BackendError(t *testing.T) {
	backend := &mockBackend{
		listJobsFunc: func(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error) {
			return nil, 0, errors.New("boom")
		},
	}
	router := newAdminTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/admin/jobs", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestAdminListWorkers_UsesBackendSummaryAndPagination(t *testing.T) {
	gotLimit := 0
	gotOffset := 0
	backend := &mockBackend{
		listWorkersFunc: func(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error) {
			gotLimit = limit
			gotOffset = offset
			return []*core.WorkerInfo{
					{ID: "w-2", State: "quiet", Directive: "quiet", ActiveJobs: 1},
				}, core.WorkerSummary{
					Total:   3,
					Running: 1,
					Quiet:   1,
					Stale:   1,
				}, nil
		},
	}
	router := newAdminTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/admin/workers?page=2&per_page=1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	items, _ := resp["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 workers item, got %d items", len(items))
	}

	summary, _ := resp["summary"].(map[string]any)
	if summary["total"] != float64(3) {
		t.Errorf("summary.total = %v, want 3", summary["total"])
	}
	if summary["running"] != float64(1) {
		t.Errorf("summary.running = %v, want 1", summary["running"])
	}

	pagination, _ := resp["pagination"].(map[string]any)
	if pagination["page"] != float64(2) || pagination["per_page"] != float64(1) {
		t.Errorf("unexpected pagination: %+v", pagination)
	}

	if gotLimit != 1 {
		t.Errorf("backend limit = %d, want 1", gotLimit)
	}
	if gotOffset != 1 {
		t.Errorf("backend offset = %d, want 1", gotOffset)
	}
}

func TestAdminBulkRetry_RequiresConfirm(t *testing.T) {
	router := newAdminTestRouter(&mockBackend{})
	body := `{"confirm":false}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/admin/jobs/bulk/retry", strings.NewReader(body))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAdminBulkRetry_Summary(t *testing.T) {
	backend := &mockBackend{
		listDeadLetterFunc: func(ctx context.Context, limit, offset int) ([]*core.Job, int, error) {
			return []*core.Job{
				{ID: "dead-1"},
				{ID: "dead-2"},
			}, 2, nil
		},
		retryDeadLetterFunc: func(ctx context.Context, jobID string) (*core.Job, error) {
			if jobID == "dead-2" {
				return nil, errors.New("boom")
			}
			return &core.Job{ID: jobID, State: core.StateAvailable}, nil
		},
	}
	router := newAdminTestRouter(backend)
	body := `{"confirm":true}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/admin/jobs/bulk/retry", strings.NewReader(body))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp["matched"] != float64(2) {
		t.Errorf("matched = %v, want 2", resp["matched"])
	}
	if resp["succeeded"] != float64(1) {
		t.Errorf("succeeded = %v, want 1", resp["succeeded"])
	}
	if resp["failed"] != float64(1) {
		t.Errorf("failed = %v, want 1", resp["failed"])
	}
}

func TestAdminStats_SkipsQueueStatsErrors(t *testing.T) {
	backend := &mockBackend{
		listQueuesFunc: func(ctx context.Context) ([]core.QueueInfo, error) {
			return []core.QueueInfo{
				{Name: "q1", Status: "active"},
				{Name: "q2", Status: "active"},
			}, nil
		},
		queueStatsFunc: func(ctx context.Context, name string) (*core.QueueStats, error) {
			if name == "q2" {
				return nil, errors.New("transient")
			}
			return &core.QueueStats{
				Queue:  "q1",
				Status: "active",
				Stats:  core.Stats{Available: 3, Active: 1, Scheduled: 2, Retryable: 4, Completed: 5, Dead: 6},
			}, nil
		},
		listWorkersFunc: func(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error) {
			return []*core.WorkerInfo{}, core.WorkerSummary{Total: 2}, nil
		},
	}

	router := newAdminTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/admin/stats", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	jobs, _ := resp["jobs"].(map[string]any)
	if jobs["available"] != float64(3) {
		t.Errorf("available = %v, want 3", jobs["available"])
	}
	if jobs["active"] != float64(1) {
		t.Errorf("active = %v, want 1", jobs["active"])
	}
	if jobs["discarded"] != float64(6) {
		t.Errorf("discarded = %v, want 6", jobs["discarded"])
	}
	if resp["workers"] != float64(2) {
		t.Errorf("workers = %v, want 2", resp["workers"])
	}
}
