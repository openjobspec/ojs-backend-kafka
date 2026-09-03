package kafka

import (
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// OJS attribute keys for Kafka headers.
const (
	HeaderSpecVersion  = "ojs-specversion"
	HeaderJobID        = "ojs-job-id"
	HeaderJobType      = "ojs-type"
	HeaderQueue        = "ojs-queue"
	HeaderPriority     = "ojs-priority"
	HeaderScheduledAt  = "ojs-scheduled-at"
	HeaderAttempt      = "ojs-attempt"
	HeaderMaxAttempts  = "ojs-max-attempts"
	HeaderRetryAfter   = "ojs-retry-after"
	HeaderCreatedAt    = "ojs-created-at"
	HeaderState        = "ojs-state"
	HeaderWorkflowID   = "ojs-workflow-id"
	HeaderWorkflowStep = "ojs-workflow-step"
	HeaderExpiresAt    = "ojs-expires-at"
	HeaderEventType    = "ojs-event-type"
)

// JobToHeaders converts OJS job attributes to Kafka headers.
func JobToHeaders(job *core.Job) []kgo.RecordHeader {
	headers := []kgo.RecordHeader{
		{Key: HeaderSpecVersion, Value: []byte(core.OJSVersion)},
		{Key: HeaderJobID, Value: []byte(job.ID)},
		{Key: HeaderJobType, Value: []byte(job.Type)},
		{Key: HeaderQueue, Value: []byte(job.Queue)},
		{Key: HeaderState, Value: []byte(job.State)},
		{Key: HeaderAttempt, Value: []byte(strconv.Itoa(job.Attempt))},
	}

	if job.Priority != nil {
		headers = append(headers, kgo.RecordHeader{
			Key: HeaderPriority, Value: []byte(strconv.Itoa(*job.Priority)),
		})
	}
	if job.MaxAttempts != nil {
		headers = append(headers, kgo.RecordHeader{
			Key: HeaderMaxAttempts, Value: []byte(strconv.Itoa(*job.MaxAttempts)),
		})
	}
	if job.ScheduledAt != "" {
		headers = append(headers, kgo.RecordHeader{
			Key: HeaderScheduledAt, Value: []byte(job.ScheduledAt),
		})
	}
	if job.CreatedAt != "" {
		headers = append(headers, kgo.RecordHeader{
			Key: HeaderCreatedAt, Value: []byte(job.CreatedAt),
		})
	}
	if job.WorkflowID != "" {
		headers = append(headers, kgo.RecordHeader{
			Key: HeaderWorkflowID, Value: []byte(job.WorkflowID),
		})
		headers = append(headers, kgo.RecordHeader{
			Key: HeaderWorkflowStep, Value: []byte(strconv.Itoa(job.WorkflowStep)),
		})
	}
	if job.ExpiresAt != "" {
		headers = append(headers, kgo.RecordHeader{
			Key: HeaderExpiresAt, Value: []byte(job.ExpiresAt),
		})
	}

	return headers
}

// GetHeader retrieves a header value by key from a Kafka record.
func GetHeader(record *kgo.Record, key string) string {
	for _, h := range record.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// EventHeaders creates Kafka headers for a lifecycle event.
func EventHeaders(eventType string, jobID string, queue string) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: HeaderSpecVersion, Value: []byte(core.OJSVersion)},
		{Key: HeaderEventType, Value: []byte(eventType)},
		{Key: HeaderJobID, Value: []byte(jobID)},
		{Key: HeaderQueue, Value: []byte(queue)},
	}
}
