package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

func BenchmarkJobCreate(b *testing.B) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"email.send","args":["user@example.com"],"options":{"queue":"default"}}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

func BenchmarkJobGet(b *testing.B) {
	router := newTestRouter(&mockBackend{})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/test-id", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

func BenchmarkWorkerFetch(b *testing.B) {
	backend := &mockBackend{
		fetchFunc: func(ctx context.Context, queues []string, count int, workerID string, visMs int) ([]*core.Job, error) {
			return []*core.Job{{ID: "job-1", Type: "test", State: "active", Queue: "default"}}, nil
		},
	}
	router := newTestRouter(backend)
	body := `{"queues":["default"],"worker_id":"w-1"}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

func BenchmarkWorkerAck(b *testing.B) {
	router := newTestRouter(&mockBackend{})
	body := `{"job_id":"job-1","result":{"ok":true}}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/ack", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

func BenchmarkBatchCreate(b *testing.B) {
	router := newTestRouter(&mockBackend{})
	body := `{"jobs":[{"type":"email.send","args":["a@b.com"]},{"type":"email.send","args":["c@d.com"]},{"type":"email.send","args":["e@f.com"]}]}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

func BenchmarkWorkerNack(b *testing.B) {
	router := newTestRouter(&mockBackend{})
	body := `{"job_id":"job-1","error":{"message":"timeout"}}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/nack", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

func BenchmarkQueueStats(b *testing.B) {
	router := newTestRouter(&mockBackend{})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/ojs/v1/queues/default/stats", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

func BenchmarkHealthCheck(b *testing.B) {
	router := newTestRouter(&mockBackend{})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/ojs/v1/health", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}
