package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// BenchmarkEnqueueDequeue measures raw throughput of enqueue+dequeue
// in a single-goroutine scenario.
func BenchmarkEnqueueDequeue(b *testing.B) {
	rb := NewRingBuffer(1024, OverflowReject)

	task := &Task{
		ID:        "bench",
		Payload:   []byte("benchmark-payload"),
		State:     TaskStatePending,
		CreatedAt: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Enqueue(task)
		rb.Dequeue()
	}
}

// BenchmarkConcurrentThroughput measures throughput with concurrent
// producers and consumers at various worker counts.
func BenchmarkConcurrentThroughput(b *testing.B) {
	for _, workers := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			rb := NewRingBuffer(1024, OverflowBlock)

			var wg sync.WaitGroup

			// Start consumers
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						_, err := rb.Dequeue()
						if err == ErrQueueClosed {
							return
						}
					}
				}()
			}

			b.ResetTimer()

			// Single producer for consistent measurement
			for i := 0; i < b.N; i++ {
				rb.Enqueue(&Task{
					ID:        fmt.Sprintf("b-%d", i),
					State:     TaskStatePending,
					CreatedAt: time.Now(),
				})
			}

			b.StopTimer()
			rb.Close()
			wg.Wait()
		})
	}
}

// BenchmarkHeapPushPop measures min-heap throughput.
func BenchmarkHeapPushPop(b *testing.B) {
	dh := NewDelayedHeap()
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dh.Push(&Task{
			ID:          fmt.Sprintf("h-%d", i),
			ScheduledAt: now.Add(time.Duration(i) * time.Millisecond),
		})
	}
	for i := 0; i < b.N; i++ {
		dh.Pop()
	}
}
