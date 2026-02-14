package kafka

// Topic naming conventions for OJS-to-Kafka mapping.
const (
	// TopicPrefixQueue is the prefix for main job topics per OJS queue.
	TopicPrefixQueue = "ojs.queue."

	// TopicPrefixDead is the prefix for dead letter topics per queue.
	TopicPrefixDead = "ojs.dead."

	// TopicScheduled is the topic for delayed jobs awaiting their time.
	TopicScheduled = "ojs.scheduled"

	// TopicEvents is the topic for lifecycle events.
	TopicEvents = "ojs.events"
)

// QueueTopic returns the Kafka topic name for an OJS queue.
func QueueTopic(queue string) string {
	return TopicPrefixQueue + queue
}

// DeadTopic returns the Kafka dead letter topic for an OJS queue.
func DeadTopic(queue string) string {
	return TopicPrefixDead + queue
}

// RetryTopic returns the Kafka retry topic for a queue and retry level.
func RetryTopic(queue string, level int) string {
	return TopicPrefixQueue + queue + ".retry"
}

// PartitionKey returns the Kafka partition key for a job.
// By default uses job type for ordering within type.
func PartitionKey(jobType string, queue string, useQueueKey bool) []byte {
	if useQueueKey {
		return []byte(queue)
	}
	return []byte(jobType)
}
