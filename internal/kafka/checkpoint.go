package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

type checkpointStore struct {
	mu    sync.RWMutex
	store map[string]*core.Checkpoint
}

func newCheckpointStore() *checkpointStore {
	return &checkpointStore{store: make(map[string]*core.Checkpoint)}
}

// SaveCheckpoint persists a checkpoint for a running job.
func (b *KafkaBackend) SaveCheckpoint(ctx context.Context, jobID string, state json.RawMessage) error {
	b.cpStore.mu.Lock()
	defer b.cpStore.mu.Unlock()

	seq := 1
	if existing, ok := b.cpStore.store[jobID]; ok {
		seq = existing.Sequence + 1
	}

	b.cpStore.store[jobID] = &core.Checkpoint{
		JobID:     jobID,
		State:     state,
		Sequence:  seq,
		CreatedAt: time.Now(),
	}
	return nil
}

// GetCheckpoint retrieves the last checkpoint for a job.
func (b *KafkaBackend) GetCheckpoint(ctx context.Context, jobID string) (*core.Checkpoint, error) {
	b.cpStore.mu.RLock()
	defer b.cpStore.mu.RUnlock()

	cp, ok := b.cpStore.store[jobID]
	if !ok {
		return nil, fmt.Errorf("checkpoint for job '%s' not found", jobID)
	}
	return cp, nil
}

// DeleteCheckpoint removes a checkpoint for a job.
func (b *KafkaBackend) DeleteCheckpoint(ctx context.Context, jobID string) error {
	b.cpStore.mu.Lock()
	defer b.cpStore.mu.Unlock()
	delete(b.cpStore.store, jobID)
	return nil
}
