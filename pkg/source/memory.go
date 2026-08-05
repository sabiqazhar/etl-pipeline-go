package source

import (
	"context"
	"time"

	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
	"github.com/aula-id/etl-pipeline-go/pkg/model"
)

// MemorySource emits hardcoded records for testing.
type MemorySource struct {
	sm      *lifecycle.StateManager
	ch      chan model.RecordBatch
	records []map[string]any // Raw config data
}

func NewMemorySource(cfg map[string]any) (model.Source, error) {
	// Expect config: records: [{key: "1", val: "a"}, ...]
	rawRecords, _ := cfg["records"].([]any)

	var records []map[string]any
	for _, r := range rawRecords {
		if m, ok := r.(map[string]any); ok {
			records = append(records, m)
		}
	}

	return &MemorySource{
		sm:      lifecycle.NewStateManager(),
		ch:      make(chan model.RecordBatch, 10),
		records: records,
	}, nil
}

func (s *MemorySource) Init(ctx context.Context) error    { return s.sm.Transition(lifecycle.StateIdle) }
func (s *MemorySource) Records() <-chan model.RecordBatch { return s.ch }
func (s *MemorySource) Drain(ctx context.Context) error {
	return s.sm.Transition(lifecycle.StateDraining)
}
func (s *MemorySource) Close() error { return s.sm.Transition(lifecycle.StateClosed) }

func (s *MemorySource) Run(ctx context.Context) error {
	if err := s.sm.Transition(lifecycle.StateRunning); err != nil {
		return err
	}

	defer close(s.ch)

	// Emit batches
	batchSize := 2
	for i := 0; i < len(s.records); i += batchSize {
		end := i + batchSize
		if end > len(s.records) {
			end = len(s.records)
		}

		var recs []model.Record
		for _, r := range s.records[i:end] {
			key, _ := r["key"].(string)
			val, _ := r["val"].(string)
			recs = append(recs, model.Record{
				Key:     []byte(key),
				Payload: []byte(val),
			})
		}

		batch := model.RecordBatch{
			ID:      "mem-batch",
			Records: recs,
		}

		select {
		case s.ch <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}

		time.Sleep(100 * time.Millisecond) // Simulate work
	}

	return nil
}
