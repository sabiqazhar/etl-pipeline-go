package pipeline

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aula-id/etl-pipeline-go/pkg/model"
)

// mockStreamReader for isolated consumer testing.
type mockStreamReader struct {
	batches     []model.RecordBatch
	index       int
	commitCalls []model.CheckpointToken
}

func newMockStreamReader(batches []model.RecordBatch) *mockStreamReader {
	return &mockStreamReader{batches: batches, commitCalls: make([]model.CheckpointToken, 0)}
}

func (m *mockStreamReader) Read(ctx context.Context) (model.RecordBatch, error) {
	if m.index >= len(m.batches) {
		return model.RecordBatch{}, io.EOF
	}
	batch := m.batches[m.index]
	m.index++
	return batch, nil
}

func (m *mockStreamReader) Commit(ctx context.Context, token model.CheckpointToken) error {
	m.commitCalls = append(m.commitCalls, token)
	return nil
}

// TestConsumer_CheckpointCommit verifies that Consumer commits checkpoint
// AFTER successful Load, with correct componentID format.
func TestConsumer_CheckpointCommit(t *testing.T) {
	logger := testLogger()

	batches := []model.RecordBatch{
		{ID: "b1", Records: []model.Record{{Key: []byte("k1")}}, Checkpoint: []byte("seq:1")},
		{ID: "b2", Records: []model.Record{{Key: []byte("k2")}}, Checkpoint: []byte("seq:2")},
		{ID: "b3", Records: []model.Record{{Key: []byte("k3")}}, Checkpoint: []byte("seq:3")},
	}

	reader := newMockStreamReader(batches)
	sink := newMockSink()
	ckptStore := newMockCheckpointStore() // from producer_test.go

	// Create consumer with checkpoint store
	consumer := NewConsumer(
		"analytics_db", // sink ID
		"orders",       // pipeline ID
		reader,
		sink,
		ckptStore,
		logger,
	)

	ctx := context.Background()

	// Init
	if err := consumer.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Run (will finish when reader returns io.EOF)
	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify: all 3 batches were loaded
	if len(sink.LoadCalls()) != 3 {
		t.Fatalf("Expected 3 Load calls, got %d", len(sink.LoadCalls()))
	}

	// Verify: checkpoint store has exactly 1 entry (consumer's checkpoint)
	if len(ckptStore.saved) != 1 {
		t.Fatalf("Expected 1 checkpoint entry, got %d", len(ckptStore.saved))
	}

	// Verify: correct componentID format: "consumer/<pipeline_id>/<sink_id>"
	expectedKey := "consumer/orders/analytics_db"
	token, ok := ckptStore.saved[expectedKey]
	if !ok {
		t.Fatalf("Checkpoint store missing key %q. Got keys: %v", expectedKey, ckptStore.saved)
	}

	// Verify: last checkpoint is from the last batch (seq:3)
	if string(token) != "seq:3" {
		t.Errorf("Expected checkpoint 'seq:3', got %q", string(token))
	}
}

// TestConsumer_NoCommitOnLoadFailure verifies that checkpoint is NOT committed
// when Sink.Load() fails. This ensures at-least-once delivery.
// TestConsumer_NoCommitOnLoadFailure verifies that checkpoint is NOT committed
// when Sink.Load() fails. This ensures at-least-once delivery.
func TestConsumer_NoCommitOnLoadFailure(t *testing.T) {
	logger := testLogger()

	batches := []model.RecordBatch{
		{ID: "b1", Records: []model.Record{{Key: []byte("k1")}}, Checkpoint: []byte("seq:1")},
		{ID: "b2", Records: []model.Record{{Key: []byte("k2")}}, Checkpoint: []byte("seq:2")},
	}

	reader := newMockStreamReader(batches)
	sink := newMockSink()
	ckptStore := newMockCheckpointStore()

	// Track loaded batches manually via external variable
	var loadedBatches []model.RecordBatch
	callCount := 0
	sink.loadFunc = func(ctx context.Context, batch RecordBatch) error {
		callCount++
		if callCount == 2 {
			return errors.New("simulated sink failure")
		}
		loadedBatches = append(loadedBatches, batch)
		return nil
	}

	consumer := NewConsumer("failing_sink", "orders", reader, sink, ckptStore, logger)

	ctx := context.Background()
	_ = consumer.Init(ctx)

	// Run should return error (Load failed)
	err := consumer.Run(ctx)
	if err == nil {
		t.Fatal("Expected error from Run, got nil")
	}

	// Verify: only 1 batch was loaded (first one succeeded)
	if len(loadedBatches) != 1 {
		t.Fatalf("Expected 1 successful Load, got %d", len(loadedBatches))
	}

	// Verify: checkpoint was committed for the first batch only
	expectedKey := "consumer/orders/failing_sink"
	token, ok := ckptStore.saved[expectedKey]
	if !ok {
		t.Fatalf("Checkpoint store missing key %q", expectedKey)
	}
	if string(token) != "seq:1" {
		t.Errorf("Expected checkpoint 'seq:1' (first batch), got %q", string(token))
	}
}

// TestConsumer_IndependentCheckpoints verifies that multiple consumers
// for the same pipeline have independent checkpoints.
func TestConsumer_IndependentCheckpoints(t *testing.T) {
	logger := testLogger()

	batches := []model.RecordBatch{
		{ID: "b1", Records: []model.Record{{Key: []byte("k1")}}, Checkpoint: []byte("seq:1")},
	}

	// Consumer A: processes all batches
	readerA := newMockStreamReader(batches)
	sinkA := newMockSink()
	ckptStore := newMockCheckpointStore()

	consumerA := NewConsumer("sink_a", "orders", readerA, sinkA, ckptStore, logger)
	ctx := context.Background()
	_ = consumerA.Init(ctx)
	_ = consumerA.Run(ctx)

	// Consumer B: processes same batches but with different sink ID
	readerB := newMockStreamReader(batches)
	sinkB := newMockSink()

	consumerB := NewConsumer("sink_b", "orders", readerB, sinkB, ckptStore, logger)
	_ = consumerB.Init(ctx)
	_ = consumerB.Run(ctx)

	// Verify: TWO independent checkpoint entries
	if len(ckptStore.saved) != 2 {
		t.Fatalf("Expected 2 checkpoint entries (one per consumer), got %d", len(ckptStore.saved))
	}

	// Verify: correct keys
	tokenA, okA := ckptStore.saved["consumer/orders/sink_a"]
	tokenB, okB := ckptStore.saved["consumer/orders/sink_b"]

	if !okA {
		t.Fatal("Missing checkpoint for consumer/orders/sink_a")
	}
	if !okB {
		t.Fatal("Missing checkpoint for consumer/orders/sink_b")
	}

	if string(tokenA) != "seq:1" || string(tokenB) != "seq:1" {
		t.Errorf("Both consumers should have checkpoint 'seq:1', got A=%q B=%q", tokenA, tokenB)
	}
}

// TestConsumer_NilCheckpointStore verifies backward compatibility:
// nil checkpoint store should not panic, just skip checkpointing.
func TestConsumer_NilCheckpointStore(t *testing.T) {
	logger := testLogger()

	batches := []model.RecordBatch{
		{ID: "b1", Records: []model.Record{{Key: []byte("k1")}}, Checkpoint: []byte("seq:1")},
	}

	reader := newMockStreamReader(batches)
	sink := newMockSink()

	// nil checkpoint store
	consumer := NewConsumer("test_sink", "orders", reader, sink, nil, logger)

	ctx := context.Background()
	_ = consumer.Init(ctx)

	// Should complete without error
	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("Run with nil checkpoint store failed: %v", err)
	}

	// Verify: batch was still loaded
	if len(sink.LoadCalls()) != 1 {
		t.Fatalf("Expected 1 Load call, got %d", len(sink.LoadCalls()))
	}
}

// TestConsumer_NilCheckpointToken verifies that batches without checkpoint
// tokens are handled gracefully (no commit, no error).
func TestConsumer_NilCheckpointToken(t *testing.T) {
	logger := testLogger()

	batches := []model.RecordBatch{
		{ID: "b1", Records: []model.Record{{Key: []byte("k1")}}, Checkpoint: nil}, // ← nil checkpoint
	}

	reader := newMockStreamReader(batches)
	sink := newMockSink()
	ckptStore := newMockCheckpointStore()

	consumer := NewConsumer("test_sink", "orders", reader, sink, ckptStore, logger)

	ctx := context.Background()
	_ = consumer.Init(ctx)

	// Should complete without error
	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("Run with nil checkpoint token failed: %v", err)
	}

	// Verify: batch was loaded
	if len(sink.LoadCalls()) != 1 {
		t.Fatalf("Expected 1 Load call, got %d", len(sink.LoadCalls()))
	}

	// Verify: NO checkpoint was saved (token was nil)
	if len(ckptStore.saved) != 0 {
		t.Fatalf("Expected 0 checkpoint entries (nil token), got %d", len(ckptStore.saved))
	}
}
