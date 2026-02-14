package kafka

import "testing"

func TestQueueTopic(t *testing.T) {
	tests := []struct {
		queue string
		want  string
	}{
		{"default", "ojs.queue.default"},
		{"emails", "ojs.queue.emails"},
		{"batch-processing", "ojs.queue.batch-processing"},
	}

	for _, tt := range tests {
		t.Run(tt.queue, func(t *testing.T) {
			got := QueueTopic(tt.queue)
			if got != tt.want {
				t.Errorf("QueueTopic(%q) = %q, want %q", tt.queue, got, tt.want)
			}
		})
	}
}

func TestDeadTopic(t *testing.T) {
	tests := []struct {
		queue string
		want  string
	}{
		{"default", "ojs.dead.default"},
		{"emails", "ojs.dead.emails"},
	}

	for _, tt := range tests {
		t.Run(tt.queue, func(t *testing.T) {
			got := DeadTopic(tt.queue)
			if got != tt.want {
				t.Errorf("DeadTopic(%q) = %q, want %q", tt.queue, got, tt.want)
			}
		})
	}
}

func TestRetryTopic(t *testing.T) {
	tests := []struct {
		queue string
		level int
		want  string
	}{
		{"default", 1, "ojs.queue.default.retry"},
		{"emails", 3, "ojs.queue.emails.retry"},
	}

	for _, tt := range tests {
		t.Run(tt.queue, func(t *testing.T) {
			got := RetryTopic(tt.queue, tt.level)
			if got != tt.want {
				t.Errorf("RetryTopic(%q, %d) = %q, want %q", tt.queue, tt.level, got, tt.want)
			}
		})
	}
}

func TestPartitionKey_DefaultByType(t *testing.T) {
	got := PartitionKey("email.send", "default", false)
	want := []byte("email.send")
	if string(got) != string(want) {
		t.Errorf("PartitionKey(default mode) = %q, want %q", got, want)
	}
}

func TestPartitionKey_UseQueueKey(t *testing.T) {
	got := PartitionKey("email.send", "emails", true)
	want := []byte("emails")
	if string(got) != string(want) {
		t.Errorf("PartitionKey(queue mode) = %q, want %q", got, want)
	}
}

func TestTopicConstants(t *testing.T) {
	if TopicPrefixQueue != "ojs.queue." {
		t.Errorf("TopicPrefixQueue = %q", TopicPrefixQueue)
	}
	if TopicPrefixDead != "ojs.dead." {
		t.Errorf("TopicPrefixDead = %q", TopicPrefixDead)
	}
	if TopicScheduled != "ojs.scheduled" {
		t.Errorf("TopicScheduled = %q", TopicScheduled)
	}
	if TopicEvents != "ojs.events" {
		t.Errorf("TopicEvents = %q", TopicEvents)
	}
}
