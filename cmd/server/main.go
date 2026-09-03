package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sumitsaini/taskqueue/internal/api"
	"github.com/sumitsaini/taskqueue/internal/config"
	"github.com/sumitsaini/taskqueue/internal/engine"
	"github.com/sumitsaini/taskqueue/internal/queue"
	"github.com/sumitsaini/taskqueue/internal/worker"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	healthCheck := flag.Bool("healthcheck", false, "Query health endpoint and exit with code 0 if healthy")
	flag.Parse()

	// Load configuration (including environment variable overrides)
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
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
		log.Fatalf("engine init: %v", err)
	}

	// Recover from WAL + snapshot
	if err := eng.Recover(); err != nil {
		log.Fatalf("recovery failed: %v", err)
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
			log.Fatalf("api server error: %v", err)
		}
	}()

	log.Printf("taskqueue server started on %s", cfg.HTTP.Addr)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %v, starting graceful shutdown...", sig)

	// Shutdown sequence: HTTP → Engine (workers → WAL)
	if err := apiServer.Shutdown(cfg.HTTP.ShutdownTimeout); err != nil {
		log.Printf("api shutdown error: %v", err)
	}

	if err := eng.Shutdown(cfg.HTTP.ShutdownTimeout); err != nil {
		log.Printf("engine shutdown error: %v", err)
	}

	log.Println("server stopped cleanly")
}
