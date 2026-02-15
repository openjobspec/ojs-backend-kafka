package server

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/openjobspec/ojs-backend-kafka/internal/admin"
	"github.com/openjobspec/ojs-backend-kafka/internal/api"
	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// NewRouter creates and configures the HTTP router with all OJS routes.
func NewRouter(backend core.Backend) http.Handler {
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.Recoverer)
	r.Use(api.PrometheusMiddleware)
	r.Use(api.OJSHeaders)
	r.Use(api.ValidateContentType)

	// Metrics endpoint (outside OJS middleware)
	r.Handle("/metrics", promhttp.Handler())

	// Create handlers
	jobHandler := api.NewJobHandler(backend)
	workerHandler := api.NewWorkerHandler(backend)
	systemHandler := api.NewSystemHandler(backend)
	queueHandler := api.NewQueueHandler(backend)
	deadLetterHandler := api.NewDeadLetterHandler(backend)
	cronHandler := api.NewCronHandler(backend)
	workflowHandler := api.NewWorkflowHandler(backend)
	batchHandler := api.NewBatchHandler(backend)

	// System endpoints
	r.Get("/ojs/manifest", systemHandler.Manifest)
	r.Get("/ojs/v1/health", systemHandler.Health)

	// Job endpoints
	r.Post("/ojs/v1/jobs", jobHandler.Create)
	r.Get("/ojs/v1/jobs/{id}", jobHandler.Get)
	r.Delete("/ojs/v1/jobs/{id}", jobHandler.Cancel)

	// Batch enqueue
	r.Post("/ojs/v1/jobs/batch", batchHandler.Create)

	// Worker endpoints
	r.Post("/ojs/v1/workers/fetch", workerHandler.Fetch)
	r.Post("/ojs/v1/workers/ack", workerHandler.Ack)
	r.Post("/ojs/v1/workers/nack", workerHandler.Nack)
	r.Post("/ojs/v1/workers/heartbeat", workerHandler.Heartbeat)

	// Queue endpoints
	r.Get("/ojs/v1/queues", queueHandler.List)
	r.Get("/ojs/v1/queues/{name}/stats", queueHandler.Stats)
	r.Post("/ojs/v1/queues/{name}/pause", queueHandler.Pause)
	r.Post("/ojs/v1/queues/{name}/resume", queueHandler.Resume)

	// Dead letter endpoints
	r.Get("/ojs/v1/dead-letter", deadLetterHandler.List)
	r.Post("/ojs/v1/dead-letter/{id}/retry", deadLetterHandler.Retry)
	r.Delete("/ojs/v1/dead-letter/{id}", deadLetterHandler.Delete)

	// Cron endpoints
	r.Get("/ojs/v1/cron", cronHandler.List)
	r.Post("/ojs/v1/cron", cronHandler.Register)
	r.Delete("/ojs/v1/cron/{name}", cronHandler.Delete)

	// Workflow endpoints
	r.Post("/ojs/v1/workflows", workflowHandler.Create)
	r.Get("/ojs/v1/workflows/{id}", workflowHandler.Get)
	r.Delete("/ojs/v1/workflows/{id}", workflowHandler.Cancel)

	// Admin UI
	r.Handle("/ojs/admin", http.RedirectHandler("/ojs/admin/", http.StatusMovedPermanently))
	r.Mount("/ojs/admin/", http.StripPrefix("/ojs/admin/", admin.Handler()))

	return r
}
