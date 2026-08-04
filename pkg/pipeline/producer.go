package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
	"github.com/aula-id/etl-pipeline-go/pkg/stream"
)

// Producer owns a Source and writes batches to a Stream.
// It implements lifecycle.Lifecycle so it can be supervised.
type Producer struct {
	sm     *lifecycle.StateManager
	source Source
	stream stream.Stream
	logger *slog.Logger
}

// NewProducer creates a Producer that reads from source and writes to writer.
func NewProducer(source Source, stream stream.Stream, logger *slog.Logger) *Producer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Producer{
		sm:     lifecycle.NewStateManager(),
		source: source,
		stream: stream,
		logger: logger,
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

			p.logger.Debug("batch published",
				"batch_id", batch.ID,
				"records", len(batch.Records),
			)

		case <-ctx.Done():
			p.logger.Info("producer stopping due to context cancellation")
			return ctx.Err()
		}
	}
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
