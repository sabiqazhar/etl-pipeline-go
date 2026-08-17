package pipeline

import (
	"context"
	"sync"

	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
	"github.com/aula-id/etl-pipeline-go/pkg/model"
)

// --- Mock Source ---

type mockSource struct {
	sm      *lifecycle.StateManager
	batches []RecordBatch
	ch      chan RecordBatch
}

func newMockSource(batches []RecordBatch) *mockSource {
	return &mockSource{
		sm:      lifecycle.NewStateManager(),
		batches: batches,
		ch:      make(chan RecordBatch, len(batches)),
	}
}

func (m *mockSource) Init(ctx context.Context) error {
	return m.sm.Transition(lifecycle.StateIdle)
}

func (m *mockSource) Run(ctx context.Context) error {
	if err := m.sm.Transition(lifecycle.StateRunning); err != nil {
		return err
	}
	// Push all batches to channel, then close it
	for _, b := range m.batches {
		select {
		case m.ch <- b:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	close(m.ch)
	return nil
}

func (m *mockSource) Records() <-chan RecordBatch {
	return m.ch
}

func (m *mockSource) Drain(ctx context.Context) error {
	return m.sm.Transition(lifecycle.StateDraining)
}

func (m *mockSource) Close() error {
	return m.sm.Transition(lifecycle.StateClosed)
}

// --- Mock Sink ---

type mockSink struct {
	sm       *lifecycle.StateManager
	mu       sync.Mutex
	received []RecordBatch
	loadFunc func(ctx context.Context, batch model.RecordBatch) error
}

func newMockSink() *mockSink {
	return &mockSink{
		sm:       lifecycle.NewStateManager(),
		received: make([]RecordBatch, 0),
	}
}

func (m *mockSink) Init(ctx context.Context) error {
	return m.sm.Transition(lifecycle.StateIdle)
}

func (m *mockSink) Run(ctx context.Context) error {
	return m.sm.Transition(lifecycle.StateRunning)
}

func (m *mockSink) Load(ctx context.Context, batch RecordBatch) error {
	if m.loadFunc != nil {
		return m.loadFunc(ctx, batch)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.received = append(m.received, batch)
	return nil
}

func (m *mockSink) Drain(ctx context.Context) error {
	return m.sm.Transition(lifecycle.StateDraining)
}

func (m *mockSink) Close() error {
	return m.sm.Transition(lifecycle.StateClosed)
}

func (m *mockSink) LoadCalls() []RecordBatch {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]RecordBatch, len(m.received))
	copy(result, m.received)
	return result
}

func (m *mockSink) Received() []RecordBatch {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]RecordBatch, len(m.received))
	copy(result, m.received)
	return result
}
