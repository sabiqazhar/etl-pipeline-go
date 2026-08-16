package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aula-id/etl-pipeline-go/pkg/checkpoint"
	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
	"github.com/aula-id/etl-pipeline-go/pkg/stream"
)

// Producer owns a Source and writes batches to a Stream.
// It commits checkpoints to the CheckpointStore after each successful publish.
// It implements lifecycle.Lifecycle so it can be supervised.
type Producer struct {
	sm              *lifecycle.StateManager
	source          Source
	stream          stream.Stream
	checkpointStore checkpoint.Store
	pipelineID      string
	logger          *slog.Logger
}

// NewProducer creates a Producer that reads from source and writes to writer.
// checkpointStore can be nil (checkpointing disabled — Phase 1 behavior).
func NewProducer(source Source, stream stream.Stream, ckptStore checkpoint.Store, pipelineID string, logger *slog.Logger) *Producer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Producer{
		sm:              lifecycle.NewStateManager(),
		source:          source,
		stream:          stream,
		checkpointStore: ckptStore,
		pipelineID:      pipelineID,
		logger:          logger,
	}
}

func (p *Producer) Init(ctx context.Context) error {
	if err := p.sm.Transition(lifecycle.StateIdle); err != nil {
		return err
	}
	return p.source.Init(ctx)
}

func (p *Producer) Run(ctx context.Context) error {
	if err := p.sm.Transition(lifecycle.StateRunning); err != nil {
		return err
	}

	// Start the source (it begins pushing batches to its internal channel)
	if err := p.source.Run(ctx); err != nil {
		return fmt.Errorf("source run failed: %w", err)
	}

	writer := p.stream.Writer()
	records := p.source.Records()
	for {
		select {
		case batch, ok := <-records:
			if !ok {
				p.logger.Info("source exhausted, flushing stream and closing readers")

				if err := writer.Flush(ctx); err != nil {
					return fmt.Errorf("flush failed: %w", err)
				}

				if err := p.stream.Drain(ctx); err != nil {
					p.logger.Error("stream drain failed during source exhaustion", "error", err)
				}

				return nil
			}

			// Publish to stream (blocks if backpressure)
			if err := writer.Publish(ctx, batch); err != nil {
				return fmt.Errorf("publish failed: %w", err)
			}

			// Commit checkpoint. RFC 0001 Section 2.2: crash between publish and
			// commit replays the batch (at-least-once, deduplicated downstream by
			// the idempotent sink). So commit failure is NON-FATAL: log, continue.
			if err := p.commitCheckpoint(ctx, batch); err != nil {
				p.logger.Error("checkpoint commit failed (batch will be replayed on restart)",
					"batch_id", batch.ID,
					"error", err,
				)
			}

			p.logger.Debug("batch published and checkpointed",
				"batch_id", batch.ID,
				"records", len(batch.Records),
				"checkpoint", string(batch.Checkpoint),
			)

		case <-ctx.Done():
			p.logger.Info("producer stopping due to context cancellation")
			return ctx.Err()
		}
	}
}

// commitCheckpoint saves the producer's checkpoint token.
// componentID format (RFC 0001 Section 2.1): "producer/<pipelineID>".
func (p *Producer) commitCheckpoint(ctx context.Context, batch RecordBatch) error {
	if p.checkpointStore == nil {
		return nil // Checkpointing disabled (Phase 1 mode).
	}
	if batch.Checkpoint == nil {
		return nil // Source didn't provide a checkpoint token.
	}
	componentID := fmt.Sprintf("producer/%s", p.pipelineID)
	return p.checkpointStore.Save(ctx, componentID, batch.Checkpoint)
}

func (p *Producer) Drain(ctx context.Context) error {
	if err := p.sm.Transition(lifecycle.StateDraining); err != nil {
		return err
	}

	p.logger.Info("draining producer")

	// Drain the source (stop extraction, flush remaining)
	if err := p.source.Drain(ctx); err != nil {
		p.logger.Error("source drain failed", "error", err)
	}

	// Flush any remaining data in the stream writer
	writer := p.stream.Writer()
	if err := writer.Flush(ctx); err != nil {
		p.logger.Error("stream flush failed", "error", err)
	}

	return nil
}

func (p *Producer) Close() error {
	if err := p.sm.Transition(lifecycle.StateClosed); err != nil {
		// Idempotent: already closed
		return nil
	}
	return p.source.Close()
}
