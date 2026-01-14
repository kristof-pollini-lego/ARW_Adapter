package main

import (
	"arw_adapter/adapter"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
)

func main() {
	// Move to config later on
	level := slog.LevelInfo
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		switch v {
		case "DEBUG":
			level = slog.LevelDebug
		case "INFO":
			level = slog.LevelInfo
		case "WARN":
			level = slog.LevelWarn
		case "ERROR":
			level = slog.LevelError
		}
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	logger := slog.New(handler).With(
		"service", "arw-adapter",
		"version", "0.1.0",
		"component", "main",
	)
	slog.SetDefault(logger)

	// Mock Pulsar fail rate
	failRate := int64(10)
	if v := os.Getenv("MOCK_PULSAR_FAIL_RATE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 100 {
			failRate = int64(n)
		}
	}

	cfgPath := os.Getenv("CONFIG_PATH")
	if cfgPath == "" {
		cfgPath = "./connection-config.json"
	}

	cfg, err := adapter.LoadConfig(cfgPath)
	if err != nil {
		slog.Error("failed to load config", "component", "main", "error", err, "configPath", cfgPath)
		os.Exit(1)
	}

	if len(cfg.Sources) == 0 {
		slog.Error("no sources configured", "component", "main")
		os.Exit(1)
	}

	prod := adapter.NewMockPulsarProducer(failRate)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Shutdown handling
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigC
		slog.Info("shutdown signal received", "component", "main")
		cancel()
	}()

	slog.Info("starting multi-source adapter",
		"component", "main",
		"sourceSystem", cfg.SourceSystem,
		"topic", cfg.Topic,
		"sources", len(cfg.Sources),
		"mockPulsarFailRatePercent", failRate,
	)

	errCh := make(chan error, len(cfg.Sources))
	var wg sync.WaitGroup
	wg.Add(len(cfg.Sources))

	for _, sc := range cfg.Sources {
		sc := sc

		// Mock OPC-UA source built from URL + node subscription list
		src := adapter.NewMockOPCUASource(cfg.SourceSystem, sc.SourceId, sc.Url, sc.Subscription)

		aCfg := adapter.ArwAdapterConfig{
			SourceSystem:   cfg.SourceSystem,
			SourceId:       sc.SourceId,
			SourceUrl:      sc.Url,
			Topic:          cfg.Topic,
			QueueSize:      256,
			Workers:        4,
			CheckpointPath: sc.CheckpointPath,
			Subscription:   sc.Subscription,
		}

		a := adapter.NewAdapter(aCfg, src, prod)

		go func() {
			defer wg.Done()
			if err := a.Run(ctx); err != nil {
				errCh <- err
				cancel()
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		slog.Error("adapter exited with error",
			"component", "main",
			"retryable", adapter.IsRetryable(err),
			"error", err,
		)
		os.Exit(1)
	}

	slog.Info("all adapters stopped cleanly", "component", "main")
}
