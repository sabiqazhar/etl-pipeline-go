package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aula-id/etl-pipeline-go/pkg/worker"
)

func main() {
	configPath := flag.String("config", "examples/simple.yaml", "Path to YAML config file")
	flag.Parse()

	// Setup signal handling for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Println("Pipeline Worker starting...")
	fmt.Printf("Config: %s\n", *configPath)

	if err := worker.Run(ctx, *configPath); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Worker failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Worker shut down gracefully.")
}
