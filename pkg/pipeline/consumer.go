package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
	"github.com/aula-id/etl-pipeline-go/pkg/stream"
)

// Consumer reads batches from a Stream and loads them to a Sink.
// Each Consumer is independent — one slow sink does NOT block others.
type Consumer struct {
	sm     *lifecycle.StateManager
	id     string
	reader stream.StreamReader
	sink   Sink
	logger *slog.Logger
}

// NewConsumer creates a Consumer that reads from reader and loads to sink.
func NewConsumer(id string, reader stream.StreamReader, sink Sink, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{
		sm:     lifecycle.NewStateManager(),
		id:     id,
		reader: reader,
		sink:   sink,
		logger: logger,
	}
}

func (c *Consumer) Init(ctx context.Context) error {
	if err := c.sm.Transition(lifecycle.StateIdle); err != nil {
		return err
	}
	return c.sink.Init(ctx)
}

func (c *Consumer) Run(ctx context.Context) error {
	if err := c.sm.Transition(lifecycle.StateRunning); err != nil {
		return err
	}

	// Start the sink
	if err := c.sink.Run(ctx); err != nil {
		return fmt.Errorf("sink run failed: %w", err)
	}

	// Read loop: read batch → load to sink → commit
	for {
		batch, err := c.reader.Read(ctx)
		if err != nil {
			if err == io.EOF {
				// Stream drained — no more data
				c.logger.Info("stream drained, consumer finishing", "consumer", c.id)
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read failed: %w", err)
		}

		// Load to sink (must be idempotent)
		if err := c.sink.Load(ctx, batch); err != nil {
			// Phase 1: return error → Supervisor restarts
			// Phase 2: retry + DLQ routing
			return fmt.Errorf("sink load failed (consumer=%s, batch=%s): %w", c.id, batch.ID, err)
		}

		// Commit checkpoint (marks this batch as processed)
		if err := c.reader.Commit(ctx, batch.Checkpoint); err != nil {
			c.logger.Error("commit failed", "consumer", c.id, "error", err)
			// Non-fatal: worst case, batch is replayed on restart (at-least-once)
		}

		c.logger.Debug("batch loaded",
			"consumer", c.id,
			"batch_id", batch.ID,
			"records", len(batch.Records),
		)
	}
}

func (c *Consumer) Drain(ctx context.Context) error {
	if err := c.sm.Transition(lifecycle.StateDraining); err != nil {
		return err
	}

	c.logger.Info("draining consumer", "consumer", c.id)
	return c.sink.Drain(ctx)
}

func (c *Consumer) Close() error {
	if err := c.sm.Transition(lifecycle.StateClosed); err != nil {
		return nil
	}
	return c.sink.Close()
}
