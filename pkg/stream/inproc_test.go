package stream

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aula-id/etl-pipeline-go/pkg/pipeline"
)

func TestInProcStream_CrashRecovery(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "etl_test_spillover")
	os.RemoveAll(dir) // Clean up from previous runs

	// --- SCENARIO 1: Producer writes, then CRASHES before Commit ---
	t.Log("Scenario 1: Writing data and simulating crash...")

	s1, err := NewInProcStream(dir, 10)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	ctx := context.Background()
	if err := s1.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	writer := s1.Writer()
	reader := s1.Reader() // Attach reader to receive data

	batch1 := pipeline.RecordBatch{ID: "batch-1", Records: []pipeline.Record{{Key: []byte("key1")}}}
	batch2 := pipeline.RecordBatch{ID: "batch-2", Records: []pipeline.Record{{Key: []byte("key2")}}}

	// Publish writes to file (fsync) and channel
	if err := writer.Publish(ctx, batch1); err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}
	if err := writer.Publish(ctx, batch2); err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}

	// Read batch 1 and COMMIT it
	readBatch, _ := reader.Read(ctx)
	if readBatch.ID != "batch-1" {
		t.Fatalf("Expected batch-1, got %s", readBatch.ID)
	}
	if err := reader.Commit(ctx, nil); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// SIMULATE CRASH: Close stream without committing batch-2
	s1.Drain(ctx)
	s1.Close()
	t.Log("Crash simulated. batch-1 committed, batch-2 uncommitted in spillover file.")

	// --- SCENARIO 2: Restart Stream (Recovery) ---
	t.Log("Scenario 2: Restarting stream and verifying replay...")

	s2, err := NewInProcStream(dir, 10)
	if err != nil {
		t.Fatalf("Failed to create stream 2: %v", err)
	}

	if err := s2.Init(ctx); err != nil {
		t.Fatalf("Init 2 failed: %v", err)
	}

	// FIX LOGIC:
	// In our simplified Phase 1 replay logic, replayUncommitted() pushes
	// directly to the main channel `s2.ch` before any Reader() is attached.
	// Therefore, we verify the replay by reading directly from `s2.ch`.

	select {
	case b := <-s2.ch:
		if b.ID != "batch-2" {
			t.Fatalf("Expected replayed batch-2, got %s", b.ID)
		}
		t.Log("SUCCESS: batch-2 was successfully replayed from spillover file!")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for replayed batch-2. Replay might have failed.")
	}

	s2.Close()
}
