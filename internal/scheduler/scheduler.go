package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	kafkabackend "github.com/openjobspec/ojs-backend-kafka/internal/kafka"
)

// Scheduler runs background tasks for the OJS Kafka server.
type Scheduler struct {
	backend  *kafkabackend.KafkaBackend
	stop     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// New creates a new Scheduler.
func New(backend *kafkabackend.KafkaBackend) *Scheduler {
	return &Scheduler{
		backend: backend,
		stop:    make(chan struct{}),
	}
}

// Start begins all background scheduling goroutines.
func (s *Scheduler) Start() {
	s.wg.Add(4)
	go s.runLoop("scheduled-promoter", 1*time.Second, s.backend.PromoteScheduled)
	go s.runLoop("retry-promoter", 200*time.Millisecond, s.backend.PromoteRetries)
	go s.runLoop("stalled-reaper", 500*time.Millisecond, s.backend.RequeueStalled)
	go s.runLoop("cron-scheduler", 10*time.Second, s.backend.FireCronJobs)
}

// Stop signals all background goroutines to stop and waits for them to finish.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.stop)
	})
	s.wg.Wait()
}

func (s *Scheduler) runLoop(name string, interval time.Duration, fn func(context.Context) error) {
	defer s.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := fn(ctx); err != nil {
				slog.Error("scheduler task failed", "task", name, "error", err)
			}
		}
	}
}
