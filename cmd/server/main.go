// Package main implements the taskqueue server daemon entrypoint.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sumitsaini/taskqueue/internal/api"
	"github.com/sumitsaini/taskqueue/internal/config"
	"github.com/sumitsaini/taskqueue/internal/engine"
	"github.com/sumitsaini/taskqueue/internal/logger"
	"github.com/sumitsaini/taskqueue/internal/queue"
	"github.com/sumitsaini/taskqueue/internal/worker"
)

func main() {
	logger.Init(slog.LevelInfo)

	healthCheck := flag.Bool("healthcheck", false, "Query health endpoint and exit with code 0 if healthy")
	flag.Parse()

	// Load configuration (including environment variable overrides)
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	if *healthCheck {
		addr := cfg.HTTP.Addr
		if strings.HasPrefix(addr, ":") {
			addr = "127.0.0.1" + addr
		}
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get("http://" + addr + "/health")
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Create engine
	eng, err := engine.New(cfg)
	if err != nil {
		logger.Error("engine initialization failed", "error", err)
		os.Exit(1)
	}

	// Recover from WAL + snapshot
	if err := eng.Recover(); err != nil {
		logger.Error("recovery failed", "error", err)
		os.Exit(1)
	}

	// Default task handler — echoes payload
	handler := func(ctx context.Context, task *queue.Task) error {
		// This is the default handler for demonstration.
		// In production, this would dispatch to registered task handlers.
		_ = task
		return nil
	}

	// Start engine (workers, scheduler, checkpoint)
	eng.Start(worker.HandlerFunc(handler))

	// Start HTTP API
	apiServer := api.NewServer(eng, api.ServerConfig{
		Addr:            cfg.HTTP.Addr,
		ReadTimeout:     cfg.HTTP.ReadTimeout,
		WriteTimeout:    cfg.HTTP.WriteTimeout,
		IdleTimeout:     cfg.HTTP.IdleTimeout,
		RateLimitBurst:  cfg.HTTP.RateLimitBurst,
		RateLimitPerSec: cfg.HTTP.RateLimitPerSec,
	})

	go func() {
		if err := apiServer.Start(); err != nil {
			logger.Error("api server error", "error", err)
			os.Exit(1)
		}
	}()

	logger.Info("taskqueue server started", "addr", cfg.HTTP.Addr)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("received signal, starting graceful shutdown", "signal", sig.String())

	// Shutdown sequence: HTTP → Engine (workers → WAL)
	if err := apiServer.Shutdown(cfg.HTTP.ShutdownTimeout); err != nil {
		logger.Error("api shutdown error", "error", err)
	}

	if err := eng.Shutdown(cfg.HTTP.ShutdownTimeout); err != nil {
		logger.Error("engine shutdown error", "error", err)
	}

	logger.Info("server stopped cleanly")
}
