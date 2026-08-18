package stream

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/aula-id/etl-pipeline-go/pkg/checkpoint"
	"github.com/aula-id/etl-pipeline-go/pkg/model"
)

func TestInProcStream_SmartReplay(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Setup BoltDB checkpoint store
	ckptPath := filepath.Join(dir, "checkpoints.db")
	ckptStore, err := checkpoint.NewBoltStore(ckptPath)
	if err != nil {
		t.Fatalf("NewBoltStore failed: %v", err)
	}
	defer ckptStore.Close()

	ctx := context.Background()

	// Scenario 1: No consumer checkpoints → replay all batches
	t.Run("NoCheckpoints_ReplayAll", func(t *testing.T) {
		spillDir := filepath.Join(dir, "spill1")
		str, err := NewInProcStream(spillDir, 10, ckptStore, "pipeline1", logger)
		if err != nil {
			t.Fatalf("NewInProcStream failed: %v", err)
		}

		if err := str.Init(ctx); err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		// Write 3 batches to spillover
		writer := str.Writer()
		for i := 1; i <= 3; i++ {
			batch := model.RecordBatch{
				ID:         "batch-" + string(rune('0'+i)),
				Checkpoint: []byte(fmt.Sprintf("seq:%d", i)),
			}
			if err := writer.Publish(ctx, batch); err != nil {
				t.Fatalf("Publish failed: %v", err)
			}
		}

		// Init should replay all 3 batches
		if err := str.Init(ctx); err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		// Read from channel and verify
		replayed := 0
		for i := 0; i < 3; i++ {
			select {
			case batch := <-str.ch:
				replayed++
				expected := fmt.Sprintf("seq:%d", i+1)
				if string(batch.Checkpoint) != expected {
					t.Errorf("Batch %d: expected checkpoint %q, got %q", i, expected, string(batch.Checkpoint))
				}
			default:
				t.Fatalf("Expected 3 batches in channel, got %d", replayed)
			}
		}

		if replayed != 3 {
			t.Errorf("Expected 3 replayed batches, got %d", replayed)
		}

		str.Close()
	})

	// Scenario 2: Single consumer, partial commit → replay uncommitted
	t.Run("SingleConsumer_PartialCommit", func(t *testing.T) {
		spillDir := filepath.Join(dir, "spill2")

		// Set consumer checkpoint to seq:2
		if err := ckptStore.Save(ctx, "consumer/pipeline2/sink_a", []byte("seq:2")); err != nil {
			t.Fatalf("Save checkpoint failed: %v", err)
		}

		str, err := NewInProcStream(spillDir, 10, ckptStore, "pipeline2", logger)
		if err != nil {
			t.Fatalf("NewInProcStream failed: %v", err)
		}

		if err := str.Init(ctx); err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		// Write 4 batches
		writer := str.Writer()
		for i := 1; i <= 4; i++ {
			batch := model.RecordBatch{
				ID:         fmt.Sprintf("batch-%d", i),
				Checkpoint: []byte(fmt.Sprintf("seq:%d", i)),
			}
			if err := writer.Publish(ctx, batch); err != nil {
				t.Fatalf("Publish failed: %v", err)
			}
		}

		// Init should replay only batches 3 and 4 (seq:2 already committed)
		if err := str.Init(ctx); err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		// Read from channel and verify
		replayed := 0
		for i := 0; i < 2; i++ {
			select {
			case batch := <-str.ch:
				replayed++
				expected := fmt.Sprintf("seq:%d", i+3) // Should be seq:3, seq:4
				if string(batch.Checkpoint) != expected {
					t.Errorf("Batch %d: expected checkpoint %q, got %q", i, expected, string(batch.Checkpoint))
				}
			default:
				t.Fatalf("Expected 2 batches in channel, got %d", replayed)
			}
		}

		if replayed != 2 {
			t.Errorf("Expected 2 replayed batches (seq:3, seq:4), got %d", replayed)
		}

		str.Close()
	})

	// Scenario 3: Multi-consumer, different checkpoints → replay based on slowest
	t.Run("MultiConsumer_DifferentCheckpoints", func(t *testing.T) {
		spillDir := filepath.Join(dir, "spill3")

		// Consumer A at seq:3, Consumer B at seq:1, Consumer C at seq:2
		_ = ckptStore.Save(ctx, "consumer/pipeline3/sink_a", []byte("seq:3"))
		_ = ckptStore.Save(ctx, "consumer/pipeline3/sink_b", []byte("seq:1"))
		_ = ckptStore.Save(ctx, "consumer/pipeline3/sink_c", []byte("seq:2"))

		str, err := NewInProcStream(spillDir, 10, ckptStore, "pipeline3", logger)
		if err != nil {
			t.Fatalf("NewInProcStream failed: %v", err)
		}

		if err := str.Init(ctx); err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		// Write 5 batches
		writer := str.Writer()
		for i := 1; i <= 5; i++ {
			batch := model.RecordBatch{
				ID:         fmt.Sprintf("batch-%d", i),
				Checkpoint: []byte(fmt.Sprintf("seq:%d", i)),
			}
			if err := writer.Publish(ctx, batch); err != nil {
				t.Fatalf("Publish failed: %v", err)
			}
		}

		// Init should replay batches 2, 3, 4, 5 (slowest consumer is at seq:1)
		if err := str.Init(ctx); err != nil {
			t.Fatalf("Init failed: %v", err)
		}

		// Read from channel and verify
		replayed := 0
		for i := 0; i < 4; i++ {
			select {
			case batch := <-str.ch:
				replayed++
				expected := fmt.Sprintf("seq:%d", i+2) // Should be seq:2, seq:3, seq:4, seq:5
				if string(batch.Checkpoint) != expected {
					t.Errorf("Batch %d: expected checkpoint %q, got %q", i, expected, string(batch.Checkpoint))
				}
			default:
				t.Fatalf("Expected 4 batches in channel, got %d", replayed)
			}
		}

		if replayed != 4 {
			t.Errorf("Expected 4 replayed batches, got %d", replayed)
		}

		str.Close()
	})
}

func TestInProcStream_Phase1BackwardCompat(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// No checkpoint store (Phase 1 mode)
	spillDir := filepath.Join(dir, "spill")
	str, err := NewInProcStream(spillDir, 10, nil, "", logger)
	if err != nil {
		t.Fatalf("NewInProcStream failed: %v", err)
	}

	ctx := context.Background()

	if err := str.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Write 2 batches
	writer := str.Writer()
	for i := 1; i <= 2; i++ {
		batch := model.RecordBatch{
			ID:         fmt.Sprintf("batch-%d", i),
			Checkpoint: []byte(fmt.Sprintf("seq:%d", i)),
		}
		if err := writer.Publish(ctx, batch); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Init should replay all (Phase 1 behavior)
	if err := str.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Verify all 2 batches are in channel
	replayed := 0
	for i := 0; i < 2; i++ {
		select {
		case <-str.ch:
			replayed++
		default:
			break
		}
	}

	if replayed != 2 {
		t.Errorf("Expected 2 replayed batches (Phase 1 mode), got %d", replayed)
	}

	str.Close()
}
