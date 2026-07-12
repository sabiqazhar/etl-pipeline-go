package stream

import (
	"context"

	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
	"github.com/aula-id/etl-pipeline-go/pkg/pipeline"
)

type Stream interface {
	lifecycle.Lifecycle
}

type StreamWriter interface {
	Publish(context.Context, pipeline.RecordBatch) error
	Flush(context.Context) error
}

type StreamReader interface {
	Read(context.Context) (pipeline.RecordBatch, error)
	Commit(context.Context, pipeline.CheckpointToken) error
}
