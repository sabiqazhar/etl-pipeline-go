package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aula-id/etl-pipeline-go/pkg/checkpoint"
	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
	"github.com/aula-id/etl-pipeline-go/pkg/stream"
)

// SinkConfig holds a sink and its identifier.
type SinkConfig struct {
	ID   string
	Sink Sink
}

// Pipeline composes Source → Stream → N Consumers (one per Sink).
// It implements lifecycle.Lifecycle and is managed by a Supervisor.
type Pipeline struct {
	sm              *lifecycle.StateManager
	id              string
	source          Source
	stream          stream.Stream
	sinks           []SinkConfig
	producer        *Producer
	consumers       []*Consumer
	checkpointStore checkpoint.Store
	logger          *slog.Logger
}

// New creates a Pipeline.
// checkpointStore can be nil (checkpointing disabled — backward compatible with Phase 1).
func New(id string, source Source, str stream.Stream, sinks []SinkConfig, ckptStore checkpoint.Store, logger *slog.Logger) *Pipeline {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pipeline{
		sm:              lifecycle.NewStateManager(),
		id:              id,
		source:          source,
		stream:          str,
		sinks:           sinks,
		checkpointStore: ckptStore,
		logger:          logger,
	}
}

// Init initializes all components in order: Stream → Producer → Consumers.
func (p *Pipeline) Init(ctx context.Context) error {
	if err := p.sm.Transition(lifecycle.StateIdle); err != nil {
		return err
	}

	p.logger.Info("initializing pipeline", "pipeline", p.id)

	// 1. Init Stream
	if err := p.stream.Init(ctx); err != nil {
		return fmt.Errorf("stream init failed: %w", err)
	}

	// 2. Init Producer (pass checkpoint store)
	p.producer = NewProducer(p.source, p.stream, p.checkpointStore, p.id, p.logger)
	if err := p.producer.Init(ctx); err != nil {
		return fmt.Errorf("producer init failed: %w", err)
	}

	// 3. Init Consumers (pass checkpoint store + pipeline ID)
	p.consumers = make([]*Consumer, 0, len(p.sinks))
	for _, sc := range p.sinks {
		reader := p.stream.Reader()
		// ← UPDATED: pass pipeline ID and checkpoint store
		consumer := NewConsumer(sc.ID, p.id, reader, sc.Sink, p.checkpointStore, p.logger)
		if err := consumer.Init(ctx); err != nil {
			return fmt.Errorf("consumer %q init failed: %w", sc.ID, err)
		}
		p.consumers = append(p.consumers, consumer)
	}

	p.logger.Info("pipeline initialized",
		"pipeline", p.id,
		"consumers", len(p.consumers),
		"checkpointing", p.checkpointStore != nil,
	)
	return nil
}

// Run starts Producer and all Consumers concurrently.
// Blocks until all components finish or context is cancelled.
func (p *Pipeline) Run(ctx context.Context) error {
	if err := p.sm.Transition(lifecycle.StateRunning); err != nil {
		return err
	}

	p.logger.Info("pipeline running", "pipeline", p.id)

	var wg sync.WaitGroup
	errCh := make(chan error, len(p.consumers)+1)

	// Start Producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.producer.Run(ctx); err != nil {
			errCh <- fmt.Errorf("producer: %w", err)
		}
	}()

	// Start all Consumers
	for _, consumer := range p.consumers {
		wg.Add(1)
		go func(c *Consumer) {
			defer wg.Done()
			if err := c.Run(ctx); err != nil {
				errCh <- fmt.Errorf("consumer %q: %w", c.id, err)
			}
		}(consumer)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(errCh)

	// Return the first error, if any
	for err := range errCh {
		if ctx.Err() != nil {
			// Context cancelled — not a real error
			return ctx.Err()
		}
		return err
	}

	p.logger.Info("pipeline completed", "pipeline", p.id)
	return nil
}

// Drain performs graceful shutdown in REVERSE order (UQ-06):
// 1. Consumers drain first (finish in-flight batches, commit checkpoints)
// 2. Producer drains (stop extraction, flush remaining to stream)
// 3. Stream drains (close reader channels, allow final reads)
func (p *Pipeline) Drain(ctx context.Context) error {
	if err := p.sm.Transition(lifecycle.StateDraining); err != nil {
		return err
	}

	p.logger.Info("draining pipeline (reverse order)", "pipeline", p.id)

	// Step 1: Drain all consumers FIRST
	// They finish their current batch and commit checkpoints.
	for _, consumer := range p.consumers {
		if err := consumer.Drain(ctx); err != nil {
			p.logger.Error("consumer drain failed", "consumer", consumer.id, "error", err)
		}
	}

	// Step 2: Drain producer (stop source, flush stream)
	if err := p.producer.Drain(ctx); err != nil {
		p.logger.Error("producer drain failed", "error", err)
	}

	// Step 3: Drain stream (close reader channels → consumers get io.EOF)
	if err := p.stream.Drain(ctx); err != nil {
		p.logger.Error("stream drain failed", "error", err)
	}

	p.logger.Info("pipeline drained", "pipeline", p.id)
	return nil
}

// Close releases all resources. Idempotent.
func (p *Pipeline) Close() error {
	if err := p.sm.Transition(lifecycle.StateClosed); err != nil {
		return nil // Already closed
	}

	p.logger.Info("closing pipeline", "pipeline", p.id)

	// Close in reverse order
	for _, consumer := range p.consumers {
		if err := consumer.Close(); err != nil {
			p.logger.Error("consumer close failed", "consumer", consumer.id, "error", err)
		}
	}

	if err := p.producer.Close(); err != nil {
		p.logger.Error("producer close failed", "error", err)
	}

	if err := p.stream.Close(); err != nil {
		p.logger.Error("stream close failed", "error", err)
	}

	return nil
}
