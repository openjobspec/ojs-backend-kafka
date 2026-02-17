package kafka

import (
	"context"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// ListJobs returns a paginated, filtered list of jobs from the Redis state store.
func (b *KafkaBackend) ListJobs(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error) {
	return b.store.ListJobs(ctx, filters, limit, offset)
}

// ListWorkers returns a paginated list of workers from the Redis state store.
func (b *KafkaBackend) ListWorkers(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error) {
	return b.store.ListWorkers(ctx, limit, offset)
}
