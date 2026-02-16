// Package metrics provides Prometheus instrumentation for the OJS server.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// JobsEnqueued counts total jobs enqueued.
	JobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_enqueued_total",
		Help:      "Total number of jobs enqueued.",
	}, []string{"queue", "type"})

	// JobsFetched counts total jobs fetched by workers.
	JobsFetched = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_fetched_total",
		Help:      "Total number of jobs fetched.",
	}, []string{"queue"})

	// JobsCompleted counts total jobs acknowledged (completed).
	JobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_completed_total",
		Help:      "Total number of jobs completed.",
	}, []string{"queue", "type"})

	// JobsFailed counts total jobs that failed (nacked).
	JobsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_failed_total",
		Help:      "Total number of jobs failed.",
	}, []string{"queue", "type"})

	// JobsCancelled counts total jobs cancelled.
	JobsCancelled = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_cancelled_total",
		Help:      "Total number of jobs cancelled.",
	}, []string{"queue", "type"})

	// JobsDiscarded counts total jobs discarded (exhausted retries).
	JobsDiscarded = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_discarded_total",
		Help:      "Total number of jobs discarded.",
	})

	// JobDuration tracks total job execution duration (active to completed/failed).
	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ojs",
		Name:      "job_duration_seconds",
		Help:      "Duration of job execution in seconds.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"queue", "type"})

	// JobWaitTime tracks time a job waits in queue before being fetched.
	JobWaitTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ojs",
		Name:      "job_wait_seconds",
		Help:      "Time a job waited in queue before being fetched.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
	}, []string{"queue", "type"})

	// FetchDuration tracks job fetch latency.
	FetchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ojs",
		Name:      "fetch_duration_seconds",
		Help:      "Duration of fetch operations in seconds.",
		Buckets:   prometheus.DefBuckets,
	})

	// QueueDepth tracks the number of jobs waiting in each queue.
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ojs",
		Name:      "queue_depth",
		Help:      "Number of jobs waiting in queue.",
	}, []string{"queue"})

	// WorkersActive tracks the number of active workers per queue.
	WorkersActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ojs",
		Name:      "workers_active",
		Help:      "Number of active workers.",
	}, []string{"queue"})

	// JobsActive tracks the total number of currently active (in-flight) jobs.
	JobsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ojs",
		Name:      "jobs_active",
		Help:      "Total number of currently active jobs.",
	})

	// ActiveJobs tracks currently active (in-flight) jobs per queue.
	ActiveJobs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ojs",
		Name:      "active_jobs",
		Help:      "Number of currently active jobs per queue.",
	}, []string{"queue"})

	// ServerInfo exposes static server metadata as labels.
	ServerInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ojs",
		Name:      "server_info",
		Help:      "Static server metadata.",
	}, []string{"version", "backend"})
)

// Init sets static server metadata on the info metric.
func Init(version, backend string) {
	ServerInfo.WithLabelValues(version, backend).Set(1)
}
