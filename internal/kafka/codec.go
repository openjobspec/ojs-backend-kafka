package kafka

import (
	"encoding/json"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// EncodeJob serializes a Job to JSON for Kafka message values.
func EncodeJob(job *core.Job) ([]byte, error) {
	return json.Marshal(job)
}

// DecodeJob deserializes a Kafka message value into a Job.
func DecodeJob(data []byte) (*core.Job, error) {
	var job core.Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// EncodeEvent serializes a lifecycle event for the events topic.
func EncodeEvent(event *LifecycleEvent) ([]byte, error) {
	return json.Marshal(event)
}

// LifecycleEvent represents an OJS job lifecycle event published to Kafka.
type LifecycleEvent struct {
	EventType   string          `json:"event_type"`
	JobID       string          `json:"job_id"`
	Queue       string          `json:"queue"`
	JobType     string          `json:"job_type"`
	State       string          `json:"state"`
	Attempt     int             `json:"attempt,omitempty"`
	Timestamp   string          `json:"timestamp"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       json.RawMessage `json:"error,omitempty"`
	WorkflowID  string          `json:"workflow_id,omitempty"`
}
