package sink

import (
	"context"
	"fmt"

	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
	"github.com/aula-id/etl-pipeline-go/pkg/model"
)

type StdoutSink struct {
	sm *lifecycle.StateManager
	id string
}

func NewStdoutSink(id string, cfg map[string]any) (model.Sink, error) {
	return &StdoutSink{
		sm: lifecycle.NewStateManager(),
		id: id,
	}, nil
}

func (s *StdoutSink) Init(ctx context.Context) error { return s.sm.Transition(lifecycle.StateIdle) }
func (s *StdoutSink) Drain(ctx context.Context) error {
	return s.sm.Transition(lifecycle.StateDraining)
}
func (s *StdoutSink) Close() error { return s.sm.Transition(lifecycle.StateClosed) }

func (s *StdoutSink) Run(ctx context.Context) error {
	return s.sm.Transition(lifecycle.StateRunning)
}

func (s *StdoutSink) Load(ctx context.Context, batch model.RecordBatch) error {
	fmt.Printf("SINK [%s] received batch %s with %d records:\n", s.id, batch.ID, len(batch.Records))
	for _, r := range batch.Records {
		fmt.Printf("   - Key: %s, Payload: %s\n", string(r.Key), string(r.Payload))
	}
	return nil
}
