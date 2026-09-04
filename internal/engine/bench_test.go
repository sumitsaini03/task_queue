package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumitsaini03/task_queue/internal/config"
	"github.com/sumitsaini03/task_queue/internal/queue"
	"github.com/sumitsaini03/task_queue/internal/wal"
)

func BenchmarkEngine_Submit(b *testing.B) {
	cfg := config.Default()
	cfg.WAL.Dir = b.TempDir()
	cfg.WAL.SyncPolicy = "none" // raw throughput
	cfg.Queue.Capacity = b.N + 1000
	cfg.Queue.OverflowPolicy = "block"
	cfg.Worker.Count = 1

	eng, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	if err := eng.Recover(); err != nil {
		b.Fatal(err)
	}
	defer eng.Shutdown(5 * time.Second)

	payload := []byte(`{"data":"benchmark test payload content"}`)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := eng.Submit(&queue.Task{
			Payload: payload,
		})
		if err != nil {
			b.Fatalf("submit: %v", err)
		}
	}
}

func BenchmarkEngine_Throughput(b *testing.B) {
	cfg := config.Default()
	cfg.WAL.Dir = b.TempDir()
	cfg.WAL.SyncPolicy = "none"
	cfg.Queue.Capacity = 4096
	cfg.Queue.OverflowPolicy = "block"
	cfg.Worker.Count = 8

	var count atomic.Int64
	handler := func(ctx context.Context, task *queue.Task) error {
		count.Add(1)
		return nil
	}

	eng, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	if err := eng.Recover(); err != nil {
		b.Fatal(err)
	}
	eng.Start(handler)
	defer eng.Shutdown(5 * time.Second)

	payload := []byte(`{"action":"process"}`)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := eng.Submit(&queue.Task{Payload: payload})
		if err != nil {
			b.Fatalf("submit: %v", err)
		}
	}
}

func BenchmarkEngine_Recovery(b *testing.B) {
	// Prepare a WAL with 5000 tasks
	tempDir := b.TempDir()
	walPath := filepath.Join(tempDir, "wal.bin")

	w, err := wal.Open(walPath, wal.SyncNone)
	if err != nil {
		b.Fatal(err)
	}
	const taskCount = 1000
	for i := 0; i < taskCount; i++ {
		t := &queue.Task{
			ID:      fmt.Sprintf("task_%d", i),
			Payload: []byte("benchmark data"),
		}
		data, _ := json.Marshal(t)
		w.Append(&wal.Entry{
			Type:   wal.EntryEnqueue,
			TaskID: t.ID,
			Data:   data,
		})
	}
	w.Close()

	cfg := config.Default()
	cfg.WAL.Dir = tempDir

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		eng, err := New(cfg)
		if err != nil {
			b.Fatal(err)
		}
		if err := eng.Recover(); err != nil {
			b.Fatal(err)
		}
		eng.walLog.Close()
	}
}
