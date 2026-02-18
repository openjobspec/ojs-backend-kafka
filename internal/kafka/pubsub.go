package kafka

import (
	"log/slog"
	"sync"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// subscription represents a single subscriber channel with its filter.
type subscription struct {
	ch     chan *core.JobEvent
	filter func(*core.JobEvent) bool
}

// PubSubBroker implements core.EventPublisher and core.EventSubscriber
// using in-memory fan-out (Kafka lacks native pub/sub for real-time SSE).
type PubSubBroker struct {
	mu   sync.RWMutex
	subs map[*subscription]struct{}
	done chan struct{}
}

// NewPubSubBroker creates a new in-memory PubSubBroker.
func NewPubSubBroker() *PubSubBroker {
	return &PubSubBroker{
		subs: make(map[*subscription]struct{}),
		done: make(chan struct{}),
	}
}

// PublishJobEvent publishes a job event to all matching subscribers.
func (b *PubSubBroker) PublishJobEvent(event *core.JobEvent) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for sub := range b.subs {
		if sub.filter == nil || sub.filter(event) {
			select {
			case sub.ch <- event:
			default:
				slog.Warn("dropping event, subscriber channel full",
					"job_id", event.JobID, "event", event.EventType)
			}
		}
	}
	return nil
}

// SubscribeJob subscribes to events for a specific job.
func (b *PubSubBroker) SubscribeJob(jobID string) (<-chan *core.JobEvent, func(), error) {
	return b.subscribe(func(e *core.JobEvent) bool {
		return e.JobID == jobID
	})
}

// SubscribeQueue subscribes to events for all jobs in a queue.
func (b *PubSubBroker) SubscribeQueue(queue string) (<-chan *core.JobEvent, func(), error) {
	return b.subscribe(func(e *core.JobEvent) bool {
		return e.Queue == queue
	})
}

// SubscribeAll subscribes to all events.
func (b *PubSubBroker) SubscribeAll() (<-chan *core.JobEvent, func(), error) {
	return b.subscribe(nil)
}

func (b *PubSubBroker) subscribe(filter func(*core.JobEvent) bool) (<-chan *core.JobEvent, func(), error) {
	ch := make(chan *core.JobEvent, 64)
	sub := &subscription{ch: ch, filter: filter}

	b.mu.Lock()
	b.subs[sub] = struct{}{}
	b.mu.Unlock()

	unsubscribe := func() {
		b.mu.Lock()
		delete(b.subs, sub)
		b.mu.Unlock()
		close(ch)
	}

	return ch, unsubscribe, nil
}

// Close shuts down the broker and removes all subscriptions.
func (b *PubSubBroker) Close() error {
	close(b.done)
	b.mu.Lock()
	defer b.mu.Unlock()
	for sub := range b.subs {
		close(sub.ch)
	}
	b.subs = make(map[*subscription]struct{})
	return nil
}
