package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type mockConsumerClient struct {
	pollStarted chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once

	fetchQueue chan kgo.Fetches

	mu         sync.Mutex
	closeCalls int
	commits    []*kgo.Record
	commitErrs map[topicPartition]error
	rewinds    []map[string]map[int32]kgo.EpochOffset
	allowCalls int
	events     []string
}

func newMockConsumerClient() *mockConsumerClient {
	return &mockConsumerClient{
		pollStarted: make(chan struct{}),
		closed:      make(chan struct{}),
		commitErrs:  make(map[topicPartition]error),
		fetchQueue:  make(chan kgo.Fetches, 8),
	}
}

func (c *mockConsumerClient) PollFetches(ctx context.Context) kgo.Fetches {
	c.closeOnce.Do(func() {
		close(c.pollStarted)
	})
	select {
	case <-ctx.Done():
		return kgo.NewErrFetch(ctx.Err())
	case <-c.closed:
		return kgo.NewErrFetch(kgo.ErrClientClosed)
	case fetches := <-c.fetchQueue:
		return fetches
	}
}

func (c *mockConsumerClient) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, record := range records {
		copyRecord := *record
		c.commits = append(c.commits, &copyRecord)
		if err := c.commitErrs[topicPartition{topic: record.Topic, partition: record.Partition}]; err != nil {
			return err
		}
	}
	return nil
}

func (c *mockConsumerClient) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rewinds = append(c.rewinds, offsets)
}

func (c *mockConsumerClient) AllowRebalance() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.allowCalls++
	c.events = append(c.events, "allow")
}

func (c *mockConsumerClient) CloseAllowingRebalance() {
	c.mu.Lock()
	c.closeCalls++
	c.events = append(c.events, "close")
	c.mu.Unlock()
	c.closeOnce.Do(func() {
		close(c.pollStarted)
	})
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
}

func (c *mockConsumerClient) eventOrder() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.events...)
}

func TestConsumerStopUnblocksIdlePollAndIsIdempotent(t *testing.T) {
	client := newMockConsumerClient()
	consumer := newConsumer(client)
	consumer.Start(func(context.Context, *kgo.Record) error {
		t.Fatal("handler must not be called")
		return nil
	})

	select {
	case <-client.pollStarted:
	case <-time.After(time.Second):
		t.Fatal("consumer did not enter PollFetches")
	}

	stopped := make(chan struct{})
	go func() {
		consumer.Stop()
		consumer.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop blocked on an idle PollFetches")
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closeCalls != 1 {
		t.Fatalf("Close calls = %d, want 1", client.closeCalls)
	}
}

// TestConsumerStopWaitsForInFlightProcessingBeforeClose verifies that Stop lets
// an in-flight processFetches reach its deferred AllowRebalance before the
// client is closed. With BlockRebalanceOnPoll, closing while processing still
// owns the poll rebalance block would deadlock the client's final revoke.
func TestConsumerStopWaitsForInFlightProcessingBeforeClose(t *testing.T) {
	client := newMockConsumerClient()
	consumer := newConsumer(client)

	processing := make(chan struct{})
	var handlerCtxCancelled bool
	consumer.Start(func(ctx context.Context, _ *kgo.Record) error {
		close(processing)
		<-ctx.Done() // block until Stop cancels, simulating in-flight work
		handlerCtxCancelled = true
		return ctx.Err()
	})

	// Deliver one batch so a processFetches is in-flight and owns the block.
	client.fetchQueue <- testFetches(testRecord("jobs", 0, 1))

	select {
	case <-processing:
	case <-time.After(time.Second):
		t.Fatal("handler did not begin in-flight processing")
	}

	stopped := make(chan struct{})
	go func() {
		consumer.Stop()
		consumer.Stop() // repeated Stop must be safe
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked on in-flight processing")
	}

	if !handlerCtxCancelled {
		t.Fatal("handler did not observe cancellation during shutdown")
	}

	order := client.eventOrder()
	if len(order) == 0 || order[len(order)-1] != "close" {
		t.Fatalf("event order = %v, want close last", order)
	}
	allowSeen := false
	for _, event := range order {
		if event == "allow" {
			allowSeen = true
		}
		if event == "close" && !allowSeen {
			t.Fatalf("close happened before in-flight AllowRebalance: %v", order)
		}
	}
	if !allowSeen {
		t.Fatalf("in-flight processing never reached AllowRebalance: %v", order)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closeCalls != 1 {
		t.Fatalf("Close calls = %d, want 1", client.closeCalls)
	}
}

func TestConsumerCommitsSuccessfulPartitionProgress(t *testing.T) {
	client := newMockConsumerClient()
	consumer := newConsumer(client)
	fetches := testFetches(
		testRecord("jobs", 0, 4),
		testRecord("jobs", 0, 5),
	)
	var handled []int64

	consumer.processFetches(func(_ context.Context, record *kgo.Record) error {
		handled = append(handled, record.Offset)
		return nil
	}, fetches)

	if len(handled) != 2 || handled[0] != 4 || handled[1] != 5 {
		t.Fatalf("handled offsets = %v", handled)
	}
	if len(client.commits) != 1 || client.commits[0].Offset != 5 {
		t.Fatalf("commits = %+v, want offset 5", client.commits)
	}
	if len(client.rewinds) != 0 || client.allowCalls != 1 {
		t.Fatalf("rewinds=%v allow=%d", client.rewinds, client.allowCalls)
	}
}

func TestConsumerStopsPartitionAtFirstFailure(t *testing.T) {
	client := newMockConsumerClient()
	consumer := newConsumer(client)
	fetches := testFetches(
		testRecord("jobs", 0, 10),
		testRecord("jobs", 0, 11),
		testRecord("jobs", 0, 12),
	)
	var handled []int64

	consumer.processFetches(func(_ context.Context, record *kgo.Record) error {
		handled = append(handled, record.Offset)
		if record.Offset == 11 {
			return errors.New("handler failed")
		}
		return nil
	}, fetches)

	if len(handled) != 2 || handled[0] != 10 || handled[1] != 11 {
		t.Fatalf("handled offsets = %v; offset 12 must not run", handled)
	}
	if len(client.commits) != 1 || client.commits[0].Offset != 10 {
		t.Fatalf("commits = %+v, want only offset 10", client.commits)
	}
	assertRewind(t, client.rewinds, "jobs", 0, 11)
}

func TestConsumerHandlesPartitionsIndependently(t *testing.T) {
	client := newMockConsumerClient()
	consumer := newConsumer(client)
	fetches := testFetches(
		testRecord("jobs", 0, 0),
		testRecord("jobs", 0, 1),
		testRecord("jobs", 0, 2),
		testRecord("jobs", 1, 7),
		testRecord("jobs", 1, 8),
	)

	consumer.processFetches(func(_ context.Context, record *kgo.Record) error {
		if record.Partition == 0 && record.Offset == 1 {
			return errors.New("partition zero failed")
		}
		return nil
	}, fetches)

	committed := map[int32]int64{}
	for _, record := range client.commits {
		committed[record.Partition] = record.Offset
	}
	if committed[0] != 0 || committed[1] != 8 {
		t.Fatalf("committed offsets = %v, want p0=0 p1=8", committed)
	}
	assertRewind(t, client.rewinds, "jobs", 0, 1)
}

func TestConsumerCommitFailureRewindsEntirePartitionBatch(t *testing.T) {
	client := newMockConsumerClient()
	client.commitErrs[topicPartition{topic: "jobs", partition: 2}] = errors.New("commit failed")
	consumer := newConsumer(client)

	consumer.processFetches(func(context.Context, *kgo.Record) error {
		return nil
	}, testFetches(
		testRecord("jobs", 2, 20),
		testRecord("jobs", 2, 21),
	))

	assertRewind(t, client.rewinds, "jobs", 2, 20)
}

func TestNewConsumerEnforcesManualCommitContract(t *testing.T) {
	unconfigured, err := kgo.NewClient()
	if err != nil {
		t.Fatalf("new unconfigured client: %v", err)
	}
	defer unconfigured.Close()

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("NewConsumer must reject auto-commit clients")
			}
		}()
		NewConsumer(unconfigured)
	}()

	configured, err := kgo.NewClient(
		kgo.ConsumerGroup("consumer-test"),
		kgo.ConsumeTopics("jobs"),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		t.Fatalf("new configured client: %v", err)
	}
	consumer := NewConsumer(configured)
	consumer.Stop()
	consumer.Stop()
}

func testRecord(topic string, partition int32, offset int64) *kgo.Record {
	return &kgo.Record{
		Topic:       topic,
		Partition:   partition,
		Offset:      offset,
		LeaderEpoch: 3,
	}
}

func testFetches(records ...*kgo.Record) kgo.Fetches {
	partitions := make(map[topicPartition][]*kgo.Record)
	for _, record := range records {
		key := topicPartition{topic: record.Topic, partition: record.Partition}
		partitions[key] = append(partitions[key], record)
	}
	topics := make(map[string][]kgo.FetchPartition)
	for key, partitionRecords := range partitions {
		topics[key.topic] = append(topics[key.topic], kgo.FetchPartition{
			Partition: key.partition,
			Records:   partitionRecords,
		})
	}
	var fetchTopics []kgo.FetchTopic
	for topic, fetchPartitions := range topics {
		fetchTopics = append(fetchTopics, kgo.FetchTopic{Topic: topic, Partitions: fetchPartitions})
	}
	return kgo.Fetches{{Topics: fetchTopics}}
}

func assertRewind(t *testing.T, rewinds []map[string]map[int32]kgo.EpochOffset, topic string, partition int32, offset int64) {
	t.Helper()
	if len(rewinds) != 1 {
		t.Fatalf("rewinds = %v, want one", rewinds)
	}
	got, ok := rewinds[0][topic][partition]
	if !ok || got.Offset != offset {
		t.Fatalf("rewind = %v, want %s/%d offset %d", rewinds[0], topic, partition, offset)
	}
}
