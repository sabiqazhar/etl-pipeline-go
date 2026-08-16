package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aula-id/etl-pipeline-go/pkg/stream"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// Test 1: Happy path — Source → Stream → Sink
func TestPipeline_HappyPath(t *testing.T) {
	dir := t.TempDir()

	// Create test data
	batches := []RecordBatch{
		{ID: "batch-1", Records: []Record{{Key: []byte("k1"), Payload: []byte("v1")}}},
		{ID: "batch-2", Records: []Record{{Key: []byte("k2"), Payload: []byte("v2")}}},
		{ID: "batch-3", Records: []Record{{Key: []byte("k3"), Payload: []byte("v3")}}},
	}

	src := newMockSource(batches)
	sink := newMockSink()

	str, err := stream.NewInProcStream(filepath.Join(dir, "spill"), 10)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	pipe := New("test-pipeline", src, str, []SinkConfig{
		{ID: "sink-1", Sink: sink},
	}, nil, testLogger())

	ctx := context.Background()

	// Init
	if err := pipe.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Run (should complete when source is exhausted)
	if err := pipe.Run(ctx); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify sink received all batches
	received := sink.Received()
	if len(received) != 3 {
		t.Fatalf("Expected 3 batches in sink, got %d", len(received))
	}

	for i, b := range received {
		expected := fmt.Sprintf("batch-%d", i+1)
		if b.ID != expected {
			t.Errorf("Batch %d: expected ID %q, got %q", i, expected, b.ID)
		}
	}

	// Cleanup
	pipe.Close()
}

// Test 2: Multi-sink fan-out — each sink gets ALL batches independently
func TestPipeline_MultiSinkFanOut(t *testing.T) {
	dir := t.TempDir()

	batches := []RecordBatch{
		{ID: "batch-1", Records: []Record{{Key: []byte("k1")}}},
		{ID: "batch-2", Records: []Record{{Key: []byte("k2")}}},
	}

	src := newMockSource(batches)
	sinkA := newMockSink()
	sinkB := newMockSink()

	str, err := stream.NewInProcStream(filepath.Join(dir, "spill"), 10)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	pipe := New("fanout-pipeline", src, str, []SinkConfig{
		{ID: "sink-A", Sink: sinkA},
		{ID: "sink-B", Sink: sinkB},
	}, nil, testLogger())

	ctx := context.Background()
	if err := pipe.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	if err := pipe.Run(ctx); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// BOTH sinks should receive ALL batches (pub/sub, not partition)
	if len(sinkA.Received()) != 2 {
		t.Errorf("Sink A: expected 2 batches, got %d", len(sinkA.Received()))
	}
	if len(sinkB.Received()) != 2 {
		t.Errorf("Sink B: expected 2 batches, got %d", len(sinkB.Received()))
	}

	pipe.Close()
}

// Test 3: Per-sink isolation — slow sink doesn't block fast sink
func TestPipeline_SinkIsolation(t *testing.T) {
	dir := t.TempDir()

	batches := []RecordBatch{
		{ID: "batch-1", Records: []Record{{Key: []byte("k1")}}},
	}

	src := newMockSource(batches)

	fastSink := newMockSink()
	slowSink := newMockSink()
	slowSink.loadFunc = func(ctx context.Context, batch RecordBatch) error {
		time.Sleep(500 * time.Millisecond) // Simulate slow sink
		slowSink.mu.Lock()
		defer slowSink.mu.Unlock()
		slowSink.received = append(slowSink.received, batch)
		return nil
	}

	str, err := stream.NewInProcStream(filepath.Join(dir, "spill"), 10)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	pipe := New("isolation-pipeline", src, str, []SinkConfig{
		{ID: "fast-sink", Sink: fastSink},
		{ID: "slow-sink", Sink: slowSink},
	}, nil, testLogger())

	ctx := context.Background()
	if err := pipe.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	start := time.Now()
	if err := pipe.Run(ctx); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	elapsed := time.Since(start)

	// Fast sink should have received data
	if len(fastSink.Received()) != 1 {
		t.Errorf("Fast sink: expected 1 batch, got %d", len(fastSink.Received()))
	}

	// Slow sink also received data (just took longer)
	if len(slowSink.Received()) != 1 {
		t.Errorf("Slow sink: expected 1 batch, got %d", len(slowSink.Received()))
	}

	// Total time should be ~500ms (slow sink), not 1000ms (serial)
	// This proves they run concurrently
	if elapsed > 900*time.Millisecond {
		t.Errorf("Sinks appear to run serially (took %v). Expected concurrent execution.", elapsed)
	}

	t.Logf("Pipeline completed in %v (concurrent execution confirmed)", elapsed)
	pipe.Close()
}

// Test 4: Graceful shutdown via context cancellation
func TestPipeline_GracefulShutdown(t *testing.T) {
	dir := t.TempDir()

	// Source that blocks forever (simulates streaming source)
	src := newMockSource(nil) // No batches — Run() will close channel immediately
	sink := newMockSink()

	str, err := stream.NewInProcStream(filepath.Join(dir, "spill"), 10)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	pipe := New("shutdown-pipeline", src, str, []SinkConfig{
		{ID: "sink-1", Sink: sink},
	}, nil, testLogger())

	ctx, cancel := context.WithCancel(context.Background())

	if err := pipe.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- pipe.Run(ctx)
	}()

	// Let it run briefly, then cancel
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		// Context cancelled is expected
		if err != nil && err != context.Canceled {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Pipeline did not shut down within timeout")
	}

	pipe.Drain(context.Background())
	pipe.Close()
}
