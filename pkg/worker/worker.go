package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aula-id/etl-pipeline-go/pkg/checkpoint"
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

	// 2. Create Checkpoint Store (BoltDB)
	ckptStore, err := checkpoint.NewBoltStore(cfg.Worker.CheckpointPath)
	if err != nil {
		return fmt.Errorf("open checkpoint store: %w", err)
	}
	defer ckptStore.Close()

	// Non-fatal integrity check on startup (UQ-11): log, let operator investigate.
	if err := ckptStore.Check(); err != nil {
		logger.Error("checkpoint store integrity check failed", "error", err)
	}
	logger.Info("checkpoint store opened", "path", cfg.Worker.CheckpointPath)

	// 3. Setup Supervisor
	sup := supervisor.New(logger)

	// 4. Build Pipelines
	for _, pCfg := range cfg.Pipelines {
		pipe, err := buildPipeline(pCfg, ckptStore, logger)
		if err != nil {
			return fmt.Errorf("build pipeline %q: %w", pCfg.ID, err)
		}

		// Add to supervisor
		sup.AddChild(pCfg.ID, pipe, supervisor.WithDrainTimeout(cfg.Worker.DrainTimeout))
	}

	// 5. Serve (Blocks until ctx cancelled or permanent failure)
	logger.Info("worker starting supervision tree")
	return sup.Serve(ctx)
}

func buildPipeline(cfg config.PipelineConfig, ckptStore checkpoint.Store, logger *slog.Logger) (*pipeline.Pipeline, error) {
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

	return pipeline.New(cfg.ID, src, str, sinks, ckptStore, logger), nil
}
