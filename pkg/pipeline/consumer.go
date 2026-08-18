package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/aula-id/etl-pipeline-go/pkg/checkpoint"
	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
	"github.com/aula-id/etl-pipeline-go/pkg/stream"
)

// Consumer reads batches from a Stream and loads them to a Sink.
// Each Consumer is independent — one slow sink does NOT block others.
// After successful Load(), Consumer commits its checkpoint to BoltDB.
type Consumer struct {
	sm              *lifecycle.StateManager
	id              string // sink ID
	pipelineID      string // ← NEW: for componentID construction
	reader          stream.StreamReader
	sink            Sink
	checkpointStore checkpoint.Store // ← NEW
	logger          *slog.Logger
}

// NewConsumer creates a Consumer.
// checkpointStore can be nil (checkpointing disabled — Phase 1 behavior).
// pipelineID is used to construct the checkpoint componentID.
func NewConsumer(sinkID string, pipelineID string, reader stream.StreamReader, sink Sink, ckptStore checkpoint.Store, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{
		sm:              lifecycle.NewStateManager(),
		id:              sinkID,
		pipelineID:      pipelineID,
		reader:          reader,
		sink:            sink,
		checkpointStore: ckptStore,
		logger:          logger,
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

	// Read loop: read batch → load to sink → commit checkpoint
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

		// Step 9: Apply transforms (Phase 3 — skip for now)
		// transformedBatch := c.transformChain.Apply(batch)

		// Step 10: Load to sink (must be idempotent)
		if err := c.sink.Load(ctx, batch); err != nil {
			// Load failed → DO NOT commit checkpoint.
			// Phase 2: retry + DLQ routing (TICKET-10)
			// For now: return error → Supervisor restarts consumer
			return fmt.Errorf("sink load failed (consumer=%s, batch=%s): %w", c.id, batch.ID, err)
		}

		// Step 11: Commit checkpoint to BoltDB
		// RFC: "Commit ckpt B (BoltDB)" — only after successful Load
		// This is NON-FATAL: if commit fails, batch will be replayed on restart
		// (at-least-once is preserved, sink must be idempotent)
		if err := c.commitCheckpoint(ctx, batch); err != nil {
			c.logger.Error("checkpoint commit failed (batch will be replayed on restart)",
				"consumer", c.id,
				"batch_id", batch.ID,
				"error", err,
			)
			// Non-fatal: do NOT return error. At-least-once is preserved.
		}

		c.logger.Debug("batch loaded and checkpointed",
			"consumer", c.id,
			"batch_id", batch.ID,
			"records", len(batch.Records),
			"checkpoint", string(batch.Checkpoint),
		)
	}
}

// commitCheckpoint saves this consumer's checkpoint to BoltDB.
// componentID format (RFC Section 2.1): "consumer/<pipeline_id>/<sink_id>"
//
// Each consumer has an INDEPENDENT checkpoint. This means:
// - Sink A can be at checkpoint "seq:5" while Sink B is at "seq:3"
// - On restart, each consumer resumes from its own position
// - This enables per-sink isolation: slow sink doesn't affect others
func (c *Consumer) commitCheckpoint(ctx context.Context, batch RecordBatch) error {
	if c.checkpointStore == nil {
		return nil // Checkpointing disabled (Phase 1 mode)
	}
	if batch.Checkpoint == nil {
		return nil // Source didn't provide a checkpoint token
	}

	componentID := fmt.Sprintf("consumer/%s/%s", c.pipelineID, c.id)
	return c.checkpointStore.Save(ctx, componentID, batch.Checkpoint)
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
