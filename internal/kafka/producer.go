package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// Producer handles publishing OJS messages to Kafka topics.
type Producer struct {
	client        *kgo.Client
	useQueueKey   bool
	eventsEnabled bool
}

// NewProducer creates a new Kafka producer.
func NewProducer(client *kgo.Client, useQueueKey bool, eventsEnabled bool) *Producer {
	return &Producer{
		client:        client,
		useQueueKey:   useQueueKey,
		eventsEnabled: eventsEnabled,
	}
}

// ProduceJob publishes a job message to the queue's Kafka topic.
func (p *Producer) ProduceJob(ctx context.Context, job *core.Job) error {
	value, err := EncodeJob(job)
	if err != nil {
		return fmt.Errorf("encoding job: %w", err)
	}

	record := &kgo.Record{
		Topic:   QueueTopic(job.Queue),
		Key:     PartitionKey(job.Type, job.Queue, p.useQueueKey),
		Value:   value,
		Headers: JobToHeaders(job),
	}

	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("producing job to kafka: %w", err)
	}

	return nil
}

// ProduceJobAsync publishes a job message without waiting for acknowledgment.
func (p *Producer) ProduceJobAsync(ctx context.Context, job *core.Job) {
	value, err := EncodeJob(job)
	if err != nil {
		slog.Error("failed to encode job for kafka", "job_id", job.ID, "error", err)
		return
	}

	record := &kgo.Record{
		Topic:   QueueTopic(job.Queue),
		Key:     PartitionKey(job.Type, job.Queue, p.useQueueKey),
		Value:   value,
		Headers: JobToHeaders(job),
	}

	p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			slog.Error("async produce failed", "job_id", job.ID, "topic", r.Topic, "error", err)
		}
	})
}

// ProduceEvent publishes a lifecycle event to the events topic.
func (p *Producer) ProduceEvent(ctx context.Context, event *LifecycleEvent) {
	if !p.eventsEnabled {
		return
	}

	value, err := EncodeEvent(event)
	if err != nil {
		slog.Error("failed to encode lifecycle event", "event_type", event.EventType, "job_id", event.JobID, "error", err)
		return
	}

	record := &kgo.Record{
		Topic:   TopicEvents,
		Key:     []byte(event.JobID),
		Value:   value,
		Headers: EventHeaders(event.EventType, event.JobID, event.Queue),
	}

	p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			slog.Error("event produce failed", "event_type", event.EventType, "job_id", event.JobID, "error", err)
		}
	})
}

// ProduceToDeadLetter publishes a job to its queue's dead letter topic.
func (p *Producer) ProduceToDeadLetter(ctx context.Context, job *core.Job) error {
	value, err := EncodeJob(job)
	if err != nil {
		return fmt.Errorf("encoding job for DLQ: %w", err)
	}

	record := &kgo.Record{
		Topic:   DeadTopic(job.Queue),
		Key:     []byte(job.ID),
		Value:   value,
		Headers: JobToHeaders(job),
	}

	results := p.client.ProduceSync(ctx, record)
	return results.FirstErr()
}

// ProduceRetry publishes a job to the retry topic with backoff metadata.
func (p *Producer) ProduceRetry(ctx context.Context, job *core.Job, retryDelayMs int64) error {
	value, err := EncodeJob(job)
	if err != nil {
		return fmt.Errorf("encoding retry job: %w", err)
	}

	headers := JobToHeaders(job)
	headers = append(headers, kgo.RecordHeader{
		Key:   HeaderRetryAfter,
		Value: []byte(fmt.Sprintf("%d", retryDelayMs)),
	})

	record := &kgo.Record{
		Topic:   RetryTopic(job.Queue, job.Attempt),
		Key:     []byte(job.ID),
		Value:   value,
		Headers: headers,
	}

	results := p.client.ProduceSync(ctx, record)
	return results.FirstErr()
}

// EmitJobEnqueued emits an enqueue lifecycle event.
func (p *Producer) EmitJobEnqueued(ctx context.Context, job *core.Job) {
	p.ProduceEvent(ctx, &LifecycleEvent{
		EventType: "job.enqueued",
		JobID:     job.ID,
		Queue:     job.Queue,
		JobType:   job.Type,
		State:     job.State,
		Timestamp: core.NowFormatted(),
	})
}

// EmitJobStarted emits a started lifecycle event.
func (p *Producer) EmitJobStarted(ctx context.Context, job *core.Job) {
	p.ProduceEvent(ctx, &LifecycleEvent{
		EventType: "job.started",
		JobID:     job.ID,
		Queue:     job.Queue,
		JobType:   job.Type,
		State:     core.StateActive,
		Timestamp: core.NowFormatted(),
	})
}

// EmitJobCompleted emits a completed lifecycle event.
func (p *Producer) EmitJobCompleted(ctx context.Context, jobID string, queue string, jobType string, result json.RawMessage) {
	p.ProduceEvent(ctx, &LifecycleEvent{
		EventType: "job.completed",
		JobID:     jobID,
		Queue:     queue,
		JobType:   jobType,
		State:     core.StateCompleted,
		Result:    result,
		Timestamp: core.NowFormatted(),
	})
}

// EmitJobFailed emits a failed lifecycle event.
func (p *Producer) EmitJobFailed(ctx context.Context, jobID string, queue string, jobType string, attempt int, jobErr json.RawMessage) {
	p.ProduceEvent(ctx, &LifecycleEvent{
		EventType: "job.failed",
		JobID:     jobID,
		Queue:     queue,
		JobType:   jobType,
		State:     core.StateRetryable,
		Attempt:   attempt,
		Error:     jobErr,
		Timestamp: core.NowFormatted(),
	})
}

// EmitJobDiscarded emits a discarded lifecycle event.
func (p *Producer) EmitJobDiscarded(ctx context.Context, jobID string, queue string, jobType string) {
	p.ProduceEvent(ctx, &LifecycleEvent{
		EventType: "job.discarded",
		JobID:     jobID,
		Queue:     queue,
		JobType:   jobType,
		State:     core.StateDiscarded,
		Timestamp: core.NowFormatted(),
	})
}

// EmitJobCancelled emits a cancelled lifecycle event.
func (p *Producer) EmitJobCancelled(ctx context.Context, jobID string, queue string, jobType string) {
	p.ProduceEvent(ctx, &LifecycleEvent{
		EventType: "job.cancelled",
		JobID:     jobID,
		Queue:     queue,
		JobType:   jobType,
		State:     core.StateCancelled,
		Timestamp: core.NowFormatted(),
	})
}
