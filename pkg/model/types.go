package model

import (
	"context"
	"time"

	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
)

// Record represents a single data record.
type Record struct {
	Key       []byte            `json:"key"`
	Payload   []byte            `json:"payload"`
	Headers   map[string]string `json:"headers"`
	Timestamp time.Time         `json:"timestamp"`
}

// CheckpointToken is an opaque, source-specific position marker.
type CheckpointToken []byte

// RecordBatch is a group of records processed together.
type RecordBatch struct {
	ID         string          `json:"id"`
	Records    []Record        `json:"records"`
	Checkpoint CheckpointToken `json:"checkpoint"`
}

// Source extracts records from an external system.
type Source interface {
	lifecycle.Lifecycle
	Records() <-chan RecordBatch
}

// Sink loads transformed records to an external target.
// Load MUST be idempotent.
type Sink interface {
	lifecycle.Lifecycle
	Load(context.Context, RecordBatch) error
}
