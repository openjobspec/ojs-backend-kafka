package kafka

import (
	"context"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer manages Kafka consumer groups for OJS queue consumption.
// In the hybrid architecture, the consumer is used for:
// 1. External consumption by distributed workers (not the HTTP API path)
// 2. Monitoring and replay capabilities
// The HTTP API fetch path reads from the state store directly.
type Consumer struct {
	client *kgo.Client
	stop   chan struct{}
	wg     sync.WaitGroup
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(client *kgo.Client) *Consumer {
	return &Consumer{
		client: client,
		stop:   make(chan struct{}),
	}
}

// Start begins consuming from configured topics.
func (c *Consumer) Start(handler func(context.Context, *kgo.Record) error) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.stop:
				return
			default:
				fetches := c.client.PollFetches(context.Background())
				if fetches.IsClientClosed() {
					return
				}

				fetches.EachError(func(t string, p int32, err error) {
					slog.Error("consumer fetch error", "topic", t, "partition", p, "error", err)
				})

				fetches.EachRecord(func(record *kgo.Record) {
					if err := handler(context.Background(), record); err != nil {
						slog.Error("consumer handler error", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
					}
				})
			}
		}
	}()
}

// Stop stops the consumer and waits for it to finish.
func (c *Consumer) Stop() {
	close(c.stop)
	c.wg.Wait()
	c.client.Close()
}
