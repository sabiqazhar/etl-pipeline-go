package config

import (
	"fmt"

	"github.com/aula-id/etl-pipeline-go/pkg/model"
	"github.com/aula-id/etl-pipeline-go/pkg/sink"
	"github.com/aula-id/etl-pipeline-go/pkg/source"
)

// SourceFactory creates a Source instance from config.
type SourceFactory func(cfg map[string]any) (model.Source, error)

// SinkFactory creates a Sink instance from config.
type SinkFactory func(id string, cfg map[string]any) (model.Sink, error)

var (
	sourceRegistry = map[string]SourceFactory{}
	sinkRegistry   = map[string]SinkFactory{}
)

func init() {
	// Register built-in connectors for Phase 1
	RegisterSource("memory", source.NewMemorySource)
	RegisterSink("stdout", sink.NewStdoutSink)
}

func RegisterSource(name string, factory SourceFactory) {
	sourceRegistry[name] = factory
}

func RegisterSink(name string, factory SinkFactory) {
	sinkRegistry[name] = factory
}

func CreateSource(cfg ComponentConfig) (model.Source, error) {
	factory, ok := sourceRegistry[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("unknown source type: %q", cfg.Type)
	}
	return factory(cfg.Config)
}

func CreateSink(id string, cfg ComponentConfig) (model.Sink, error) {
	factory, ok := sinkRegistry[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("unknown sink type: %q", cfg.Type)
	}
	return factory(id, cfg.Config)
}
