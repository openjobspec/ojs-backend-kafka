package state

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// ListJobs scans all jobs from the Redis state store with filtering and pagination.
func (s *RedisStore) ListJobs(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error) {
	allJobs, err := s.scanAllJobs(ctx)
	if err != nil {
		return nil, 0, err
	}

	var filtered []*core.Job
	for _, j := range allJobs {
		if filters.State != "" && j.State != filters.State {
			continue
		}
		if filters.Queue != "" && j.Queue != filters.Queue {
			continue
		}
		if filters.Type != "" && j.Type != filters.Type {
			continue
		}
		filtered = append(filtered, j)
	}

	total := len(filtered)
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CreatedAt > filtered[j].CreatedAt
	})

	if offset >= len(filtered) {
		return []*core.Job{}, total, nil
	}
	end := offset + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[offset:end], total, nil
}

// ListWorkers scans all workers from the Redis state store with pagination.
func (s *RedisStore) ListWorkers(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error) {
	workers, err := s.scanAllWorkers(ctx)
	if err != nil {
		return nil, core.WorkerSummary{}, err
	}

	summary := core.WorkerSummary{Total: len(workers)}
	for _, w := range workers {
		switch w.State {
		case "running":
			summary.Running++
		case "quiet":
			summary.Quiet++
		default:
			summary.Stale++
		}
	}

	if offset >= len(workers) {
		return []*core.WorkerInfo{}, summary, nil
	}
	end := offset + limit
	if end > len(workers) {
		end = len(workers)
	}
	return workers[offset:end], summary, nil
}

// scanAllJobs scans Redis for all job keys and loads them.
func (s *RedisStore) scanAllJobs(ctx context.Context) ([]*core.Job, error) {
	var allJobs []*core.Job
	var cursor uint64
	for {
		keys, next, err := s.client.Scan(ctx, cursor, keyPrefix+"job:*", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			data, err := s.client.HGetAll(ctx, key).Result()
			if err != nil || len(data) == 0 {
				continue
			}
			allJobs = append(allJobs, hashToJob(data))
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return allJobs, nil
}

// scanAllWorkers scans Redis for all registered workers and loads their data.
func (s *RedisStore) scanAllWorkers(ctx context.Context) ([]*core.WorkerInfo, error) {
	workerIDs, err := s.client.SMembers(ctx, workersKey()).Result()
	if err != nil {
		return nil, err
	}

	var workers []*core.WorkerInfo
	now := time.Now()
	for _, id := range workerIDs {
		data, err := s.client.HGetAll(ctx, workerKey(id)).Result()
		if err != nil || len(data) == 0 {
			continue
		}

		w := &core.WorkerInfo{
			ID:        id,
			State:     data["state"],
			Directive: data["directive"],
		}
		if v, ok := data["last_heartbeat"]; ok {
			w.LastHeartbeat = v
		}
		if v, ok := data["active_jobs"]; ok {
			w.ActiveJobs, _ = strconv.Atoi(v)
		}

		// Mark workers as stale if no heartbeat for 60 seconds
		if w.State == "" || w.State == "running" {
			if w.LastHeartbeat != "" {
				if t, err := time.Parse(time.RFC3339, w.LastHeartbeat); err == nil {
					if now.Sub(t) > 60*time.Second {
						w.State = "stale"
					}
				}
			}
			if w.State == "" {
				w.State = "running"
			}
		}

		workers = append(workers, w)
	}
	return workers, nil
}
