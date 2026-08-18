package pipeline

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/aula-id/etl-pipeline-go/pkg/model"
	"github.com/aula-id/etl-pipeline-go/pkg/stream"
)

func TestProducer_CheckpointCommit(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Setup: source with 2 batches
	batches := []model.RecordBatch{
		{ID: "b1", Records: []model.Record{{Key: []byte("k1")}}, Checkpoint: []byte("seq:1")},
		{ID: "b2", Records: []model.Record{{Key: []byte("k2")}}, Checkpoint: []byte("seq:2")},
	}
	src := newMockSource(batches)

	// Setup: stream
	str, err := stream.NewInProcStream(dir, 10)
	if err != nil {
		t.Fatalf("NewInProcStream failed: %v", err)
	}

	// Setup: mock checkpoint store
	ckptStore := newMockCheckpointStore()

	// Setup: producer
	producer := NewProducer(src, str, ckptStore, "test-pipeline", logger)

	ctx := context.Background()

	// Init
	if err := producer.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if err := str.Init(ctx); err != nil {
		t.Fatalf("Stream Init failed: %v", err)
	}

	// Attach a reader so Publish doesn't block
	_ = str.Reader()

	// Run producer (will finish when source exhausted)
	if err := producer.Run(ctx); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify: checkpoint store received saves
	if len(ckptStore.saved) != 1 {
		t.Fatalf("Expected 1 checkpoint entry, got %d", len(ckptStore.saved))
	}

	// Verify: correct componentID format
	expectedKey := "producer/test-pipeline"
	token, ok := ckptStore.saved[expectedKey]
	if !ok {
		t.Fatalf("Checkpoint store missing key %q. Got keys: %v", expectedKey, ckptStore.saved)
	}

	// Verify: last checkpoint is from the last batch
	if string(token) != "seq:2" {
		t.Errorf("Expected checkpoint 'seq:2', got %q", string(token))
	}

	str.Close()
}

func TestProducer_NilCheckpointStore(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	batches := []model.RecordBatch{
		{ID: "b1", Records: []model.Record{{Key: []byte("k1")}}, Checkpoint: []byte("seq:1")},
	}
	src := newMockSource(batches)

	str, _ := stream.NewInProcStream(dir, 10)

	// nil checkpoint store → should not panic, just skip checkpointing
	producer := NewProducer(src, str, nil, "test-pipeline", logger)

	ctx := context.Background()
	_ = producer.Init(ctx)
	_ = str.Init(ctx)
	_ = str.Reader()

	// Should complete without error
	if err := producer.Run(ctx); err != nil {
		t.Fatalf("Run with nil checkpoint store failed: %v", err)
	}

	str.Close()
}
