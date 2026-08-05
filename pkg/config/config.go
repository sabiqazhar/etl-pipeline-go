package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration structure.
type Config struct {
	Worker    WorkerConfig     `yaml:"worker"`
	Pipelines []PipelineConfig `yaml:"pipelines"`
}

type WorkerConfig struct {
	DrainTimeout      time.Duration `yaml:"drain_timeout"`
	MaxRestartBackoff time.Duration `yaml:"max_restart_backoff"`
}

type PipelineConfig struct {
	ID     string            `yaml:"id"`
	Source ComponentConfig   `yaml:"source"`
	Sinks  []ComponentConfig `yaml:"sinks"`
}

type ComponentConfig struct {
	ID     string         `yaml:"id"`
	Type   string         `yaml:"type"`
	Config map[string]any `yaml:"config"`
}

// Load reads and parses a YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config yaml: %w", err)
	}

	// Set defaults
	if cfg.Worker.DrainTimeout == 0 {
		cfg.Worker.DrainTimeout = 30 * time.Second
	}

	return &cfg, nil
}
