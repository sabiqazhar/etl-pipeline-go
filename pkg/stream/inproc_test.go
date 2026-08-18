package stream

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aula-id/etl-pipeline-go/pkg/checkpoint"
	"github.com/aula-id/etl-pipeline-go/pkg/model"
)

func TestInProcStream_CrashRecovery(t *testing.T) {
	dir := t.TempDir()
	spillDir := filepath.Join(dir, "spill")

	// Setup BoltDB checkpoint store (Phase 2 style)
	ckptPath := filepath.Join(dir, "checkpoints.db")
	ckptStore, err := checkpoint.NewBoltStore(ckptPath)
	if err != nil {
		t.Fatalf("NewBoltStore failed: %v", err)
	}
	defer ckptStore.Close()

	ctx := context.Background()

	t.Log("Scenario 1: Writing data and simulating crash...")

	s1, err := NewInProcStream(spillDir, 10, ckptStore, "test-pipeline", nil)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	if err := s1.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	writer := s1.Writer()
	reader := s1.Reader()

	batch1 := model.RecordBatch{ID: "batch-1", Records: []model.Record{{Key: []byte("key1")}}, Checkpoint: []byte("seq:1")}
	batch2 := model.RecordBatch{ID: "batch-2", Records: []model.Record{{Key: []byte("key2")}}, Checkpoint: []byte("seq:2")}

	if err := writer.Publish(ctx, batch1); err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}
	if err := writer.Publish(ctx, batch2); err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}

	// Read & "commit" batch-1 via BoltDB (simulating Consumer commit)
	readBatch, _ := reader.Read(ctx)
	if readBatch.ID != "batch-1" {
		t.Fatalf("Expected batch-1, got %s", readBatch.ID)
	}
	if err := ckptStore.Save(ctx, "consumer/test-pipeline/sink-1", []byte("seq:1")); err != nil {
		t.Fatalf("Save checkpoint failed: %v", err)
	}

	// Simulate crash: close stream without committing batch-2
	s1.Drain(ctx)
	s1.Close()
	t.Log("Crash simulated. batch-1 committed, batch-2 uncommitted.")

	t.Log("Scenario 2: Restarting stream and verifying replay...")

	s2, err := NewInProcStream(spillDir, 10, ckptStore, "test-pipeline", nil)
	if err != nil {
		t.Fatalf("Failed to create stream 2: %v", err)
	}

	if err := s2.Init(ctx); err != nil {
		t.Fatalf("Init 2 failed: %v", err)
	}

	// Should only replay batch-2 (batch-1 already committed by consumer)
	select {
	case b := <-s2.ch:
		if b.ID != "batch-2" {
			t.Fatalf("Expected replayed batch-2, got %s", b.ID)
		}
		t.Log("SUCCESS: batch-2 was successfully replayed!")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for replayed batch-2.")
	}

	// Verify no more batches in channel
	select {
	case b := <-s2.ch:
		t.Fatalf("Unexpected extra batch replayed: %s", b.ID)
	default:
		// Good - no more batches
	}

	s2.Close()
}
