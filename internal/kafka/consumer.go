package kafka

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type consumerClient interface {
	PollFetches(context.Context) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	SetOffsets(map[string]map[int32]kgo.EpochOffset)
	AllowRebalance()
	CloseAllowingRebalance()
}

// Consumer manages Kafka consumer groups for OJS queue consumption.
// In the hybrid architecture, the consumer is used for:
// 1. External consumption by distributed workers (not the HTTP API path)
// 2. Monitoring and replay capabilities
// The HTTP API fetch path reads from the state store directly.
type Consumer struct {
	client    consumerClient
	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	stopOnce  sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewConsumer creates a new Kafka consumer and takes ownership of client.
//
// Because the client is externally configured, callers MUST use
// kgo.DisableAutoCommit() and kgo.BlockRebalanceOnPoll(). Any custom
// OnPartitionsRevoked callback must not commit offsets beyond the processing
// commits owned by Consumer. Stop drains in-flight processing and then closes
// the supplied client with CloseAllowingRebalance.
func NewConsumer(client *kgo.Client) *Consumer {
	if client == nil {
		panic("kafka consumer requires a non-nil client")
	}
	autoCommitDisabled, _ := client.OptValue(kgo.DisableAutoCommit).(bool)
	if !autoCommitDisabled {
		panic("kafka consumer client must be configured with kgo.DisableAutoCommit()")
	}
	blockRebalance, _ := client.OptValue(kgo.BlockRebalanceOnPoll).(bool)
	if !blockRebalance {
		panic("kafka consumer client must be configured with kgo.BlockRebalanceOnPoll()")
	}
	return newConsumer(client)
}

func newConsumer(client consumerClient) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{
		client: client,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins consuming from configured topics.
func (c *Consumer) Start(handler func(context.Context, *kgo.Record) error) {
	if handler == nil {
		panic("kafka consumer handler must not be nil")
	}

	c.startOnce.Do(func() {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for {
				fetches := c.client.PollFetches(c.ctx)
				if fetches.IsClientClosed() || errors.Is(fetches.Err0(), context.Canceled) {
					return
				}

				fetches.EachError(func(topic string, partition int32, err error) {
					slog.Error("consumer fetch error", "topic", topic, "partition", partition, "error", err)
				})
				c.processFetches(handler, fetches)
			}
		}()
	})
}

type topicPartition struct {
	topic     string
	partition int32
}

func (c *Consumer) processFetches(handler func(context.Context, *kgo.Record) error, fetches kgo.Fetches) {
	defer c.client.AllowRebalance()

	recordsByPartition := make(map[topicPartition][]*kgo.Record)
	fetches.EachRecord(func(record *kgo.Record) {
		key := topicPartition{topic: record.Topic, partition: record.Partition}
		recordsByPartition[key] = append(recordsByPartition[key], record)
	})

	partitions := make([]topicPartition, 0, len(recordsByPartition))
	for key := range recordsByPartition {
		partitions = append(partitions, key)
	}
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].topic == partitions[j].topic {
			return partitions[i].partition < partitions[j].partition
		}
		return partitions[i].topic < partitions[j].topic
	})

	for _, key := range partitions {
		records := recordsByPartition[key]
		sort.SliceStable(records, func(i, j int) bool {
			return records[i].Offset < records[j].Offset
		})
		c.processPartition(handler, records)
	}
}

func (c *Consumer) processPartition(handler func(context.Context, *kgo.Record) error, records []*kgo.Record) {
	if len(records) == 0 {
		return
	}

	var lastSuccessful *kgo.Record
	var failed *kgo.Record
	for _, record := range records {
		if err := handler(c.ctx, record); err != nil {
			slog.Error(
				"consumer handler error",
				"topic", record.Topic,
				"partition", record.Partition,
				"offset", record.Offset,
				"error", err,
			)
			failed = record
			break
		}
		lastSuccessful = record
	}

	if lastSuccessful != nil {
		if err := c.client.CommitRecords(c.ctx, lastSuccessful); err != nil {
			slog.Error(
				"consumer commit error",
				"topic", lastSuccessful.Topic,
				"partition", lastSuccessful.Partition,
				"offset", lastSuccessful.Offset,
				"error", err,
			)
			c.rewind(records[0])
			return
		}
	}
	if failed != nil {
		c.rewind(failed)
	}
}

func (c *Consumer) rewind(record *kgo.Record) {
	c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		record.Topic: {
			record.Partition: {
				Epoch:  record.LeaderEpoch,
				Offset: record.Offset,
			},
		},
	})
}

// Stop stops the consumer and releases the Kafka client. The sequence is
// ordered so that a client using BlockRebalanceOnPoll never has Close called
// while an in-flight processFetches still owns the poll rebalance block:
//
//  1. Cancel the context. This unblocks an idle PollFetches and signals any
//     in-flight processing to wind down. Cancellation is idempotent.
//  2. Wait for the processing goroutine to exit. Its deferred AllowRebalance
//     runs for any in-flight batch, releasing the rebalance block before it
//     returns, so no block is held once Wait returns.
//  3. Close the client with CloseAllowingRebalance so the final leave-group
//     revoke is not blocked by the poll rebalance guard.
//
// It is safe to call repeatedly and safe to call before Start.
func (c *Consumer) Stop() {
	c.stopOnce.Do(c.cancel)
	c.wg.Wait()
	c.closeOnce.Do(c.client.CloseAllowingRebalance)
}
