package core

import "time"

// Event types for real-time notifications.
const (
	EventJobStateChanged = "job.state_changed"
	EventJobProgress     = "job.progress"
	EventServerShutdown  = "server.shutdown"
)

// JobEvent represents a real-time job event.
type JobEvent struct {
	EventType string `json:"event"`
	JobID     string `json:"job_id"`
	Queue     string `json:"queue"`
	JobType   string `json:"type"`
	From      string `json:"from,omitempty"`
	To        string `json:"to,omitempty"`
	Progress  int    `json:"progress,omitempty"`
	Message   string `json:"message,omitempty"`
	Timestamp string `json:"timestamp"`
}

// NewStateChangedEvent creates a job.state_changed event.
func NewStateChangedEvent(jobID, queue, jobType, from, to string) *JobEvent {
	return &JobEvent{
		EventType: EventJobStateChanged,
		JobID:     jobID,
		Queue:     queue,
		JobType:   jobType,
		From:      from,
		To:        to,
		Timestamp: FormatTime(time.Now()),
	}
}

// EventPublisher defines the interface for publishing real-time events.
type EventPublisher interface {
	// PublishJobEvent publishes a job event to all interested subscribers.
	PublishJobEvent(event *JobEvent) error
	// Close shuts down the publisher.
	Close() error
}

// EventSubscriber defines the interface for subscribing to real-time events.
type EventSubscriber interface {
	// SubscribeJob subscribes to events for a specific job.
	SubscribeJob(jobID string) (<-chan *JobEvent, func(), error)
	// SubscribeQueue subscribes to events for all jobs in a queue.
	SubscribeQueue(queue string) (<-chan *JobEvent, func(), error)
	// SubscribeAll subscribes to all events.
	SubscribeAll() (<-chan *JobEvent, func(), error)
}
