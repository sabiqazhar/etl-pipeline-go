package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aula-id/etl-pipeline-go/pkg/config"
	"github.com/aula-id/etl-pipeline-go/pkg/pipeline"
	"github.com/aula-id/etl-pipeline-go/pkg/stream"
	"github.com/aula-id/etl-pipeline-go/pkg/supervisor"
)

// Run loads config, builds pipelines, and starts the supervision tree.
func Run(ctx context.Context, configPath string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 1. Load Config
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger.Info("config loaded", "pipelines", len(cfg.Pipelines))

	// 2. Setup Supervisor
	sup := supervisor.New(logger)

	// 3. Build Pipelines
	for _, pCfg := range cfg.Pipelines {
		pipe, err := buildPipeline(pCfg, logger)
		if err != nil {
			return fmt.Errorf("build pipeline %q: %w", pCfg.ID, err)
		}

		// Add to supervisor
		sup.AddChild(pCfg.ID, pipe, supervisor.WithDrainTimeout(cfg.Worker.DrainTimeout))
	}

	// 4. Serve (Blocks until ctx cancelled or permanent failure)
	logger.Info("worker starting supervision tree")
	return sup.Serve(ctx)
}

func buildPipeline(cfg config.PipelineConfig, logger *slog.Logger) (*pipeline.Pipeline, error) {
	// Create Source
	src, err := config.CreateSource(cfg.Source)
	if err != nil {
		return nil, err
	}

	// Create Stream (InProcStream for Phase 1)
	// Use temp dir for spillover to avoid cluttering project root
	spillDir := fmt.Sprintf("/tmp/etl-spill-%s", cfg.ID)
	str, err := stream.NewInProcStream(spillDir, 100)
	if err != nil {
		return nil, err
	}

	// Create Sinks
	var sinks []pipeline.SinkConfig
	for _, sCfg := range cfg.Sinks {
		sinkInst, err := config.CreateSink(sCfg.ID, sCfg)
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, pipeline.SinkConfig{
			ID:   sCfg.ID,
			Sink: sinkInst,
		})
	}

	return pipeline.New(cfg.ID, src, str, sinks, logger), nil
}
