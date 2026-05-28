package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/Longfortv5/longfort-v7-briefings/level2"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg := level2.LoadCfgFromEnv()
	if err := level2.RunOrchestrator(context.Background(), cfg); err != nil {
		slog.Error("orchestrator fatal", "err", err)
		os.Exit(1)
	}
}
