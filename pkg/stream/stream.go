package stream

import (
	"context"

	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
	"github.com/aula-id/etl-pipeline-go/pkg/model"
)

type Stream interface {
	lifecycle.Lifecycle
	Writer() StreamWriter
	Reader() StreamReader
}

type StreamWriter interface {
	Publish(context.Context, model.RecordBatch) error
	Flush(context.Context) error
}

type StreamReader interface {
	Read(context.Context) (model.RecordBatch, error)
	Commit(context.Context, model.CheckpointToken) error
}
