// Package metrics provides observability for the task queue.
// Uses simple atomic counters and structured logging rather than
// importing Prometheus as a dependency (keeping external deps minimal).
package metrics

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks operational counters and histograms.
type Metrics struct {
	// Counters
	EnqueueTotal  atomic.Int64
	DequeueTotal  atomic.Int64
	CompleteTotal atomic.Int64
	FailTotal     atomic.Int64
	RetryTotal    atomic.Int64
	DeadTotal     atomic.Int64

	// Gauges
	QueueDepth  atomic.Int64
	BusyWorkers atomic.Int64

	// Latency tracking
	mu             sync.Mutex
	enqueueSamples []time.Duration
	executeSamples []time.Duration
}

// New creates a new Metrics instance.
func New() *Metrics {
	return &Metrics{}
}

// RecordEnqueue records an enqueue operation.
func (m *Metrics) RecordEnqueue(latency time.Duration) {
	m.EnqueueTotal.Add(1)
	m.QueueDepth.Add(1)
	m.mu.Lock()
	m.enqueueSamples = append(m.enqueueSamples, latency)
	if len(m.enqueueSamples) > 10000 {
		m.enqueueSamples = m.enqueueSamples[len(m.enqueueSamples)-5000:]
	}
	m.mu.Unlock()
}

// RecordDequeue records a dequeue operation.
func (m *Metrics) RecordDequeue() {
	m.DequeueTotal.Add(1)
	m.QueueDepth.Add(-1)
}

// RecordComplete records a task completion.
func (m *Metrics) RecordComplete(execTime time.Duration) {
	m.CompleteTotal.Add(1)
	m.mu.Lock()
	m.executeSamples = append(m.executeSamples, execTime)
	if len(m.executeSamples) > 10000 {
		m.executeSamples = m.executeSamples[len(m.executeSamples)-5000:]
	}
	m.mu.Unlock()
}

// RecordFail records a task failure.
func (m *Metrics) RecordFail() {
	m.FailTotal.Add(1)
}

// RecordRetry records a retry attempt.
func (m *Metrics) RecordRetry() {
	m.RetryTotal.Add(1)
}

// RecordDead records a task sent to DLQ.
func (m *Metrics) RecordDead() {
	m.DeadTotal.Add(1)
}

// Snapshot returns a JSON-serializable snapshot of all metrics.
type Snapshot struct {
	EnqueueTotal  int64 `json:"enqueue_total"`
	DequeueTotal  int64 `json:"dequeue_total"`
	CompleteTotal int64 `json:"complete_total"`
	FailTotal     int64 `json:"fail_total"`
	RetryTotal    int64 `json:"retry_total"`
	DeadTotal     int64 `json:"dead_total"`
	QueueDepth    int64 `json:"queue_depth"`
	BusyWorkers   int64 `json:"busy_workers"`
	EnqueueP50Ns  int64 `json:"enqueue_p50_ns"`
	EnqueueP99Ns  int64 `json:"enqueue_p99_ns"`
	ExecuteP50Ns  int64 `json:"execute_p50_ns"`
	ExecuteP99Ns  int64 `json:"execute_p99_ns"`
}

// GetSnapshot returns current metric values.
func (m *Metrics) GetSnapshot() Snapshot {
	m.mu.Lock()
	ep50, ep99 := percentiles(m.enqueueSamples)
	xp50, xp99 := percentiles(m.executeSamples)
	m.mu.Unlock()

	return Snapshot{
		EnqueueTotal:  m.EnqueueTotal.Load(),
		DequeueTotal:  m.DequeueTotal.Load(),
		CompleteTotal: m.CompleteTotal.Load(),
		FailTotal:     m.FailTotal.Load(),
		RetryTotal:    m.RetryTotal.Load(),
		DeadTotal:     m.DeadTotal.Load(),
		QueueDepth:    m.QueueDepth.Load(),
		BusyWorkers:   m.BusyWorkers.Load(),
		EnqueueP50Ns:  int64(ep50),
		EnqueueP99Ns:  int64(ep99),
		ExecuteP50Ns:  int64(xp50),
		ExecuteP99Ns:  int64(xp99),
	}
}

// LogSummary writes a structured log line with current metrics.
func (m *Metrics) LogSummary() {
	snap := m.GetSnapshot()
	data, _ := json.Marshal(snap)
	log.Printf("metrics: %s", string(data))
}

// percentiles calculates p50 and p99 from a slice of durations.
// Not perfectly sorted (approximate), but good enough for monitoring.
func percentiles(samples []time.Duration) (p50, p99 time.Duration) {
	n := len(samples)
	if n == 0 {
		return 0, 0
	}

	// Copy and sort
	sorted := make([]time.Duration, n)
	copy(sorted, samples)
	sortDurations(sorted)

	p50 = sorted[n*50/100]
	p99Idx := n * 99 / 100
	if p99Idx >= n {
		p99Idx = n - 1
	}
	p99 = sorted[p99Idx]
	return
}

func sortDurations(d []time.Duration) {
	// Simple insertion sort — good enough for capped 10k samples
	for i := 1; i < len(d); i++ {
		key := d[i]
		j := i - 1
		for j >= 0 && d[j] > key {
			d[j+1] = d[j]
			j--
		}
		d[j+1] = key
	}
}

// PrometheusFormat exports metrics in Prometheus text exposition format.
func (m *Metrics) PrometheusFormat() string {
	snap := m.GetSnapshot()
	var b strings.Builder
	fmt.Fprintf(&b, "# HELP taskqueue_depth Current number of tasks in queue\n# TYPE taskqueue_depth gauge\ntaskqueue_depth %d\n", snap.QueueDepth)
	fmt.Fprintf(&b, "# HELP taskqueue_enqueue_total Total tasks enqueued\n# TYPE taskqueue_enqueue_total counter\ntaskqueue_enqueue_total %d\n", snap.EnqueueTotal)
	fmt.Fprintf(&b, "# HELP taskqueue_dequeue_total Total tasks dequeued\n# TYPE taskqueue_dequeue_total counter\ntaskqueue_dequeue_total %d\n", snap.DequeueTotal)
	fmt.Fprintf(&b, "# HELP taskqueue_completed_total Total tasks completed successfully\n# TYPE taskqueue_completed_total counter\ntaskqueue_completed_total %d\n", snap.CompleteTotal)
	fmt.Fprintf(&b, "# HELP taskqueue_failed_total Total tasks that failed\n# TYPE taskqueue_failed_total counter\ntaskqueue_failed_total %d\n", snap.FailTotal)
	fmt.Fprintf(&b, "# HELP taskqueue_retry_total Total retry attempts\n# TYPE taskqueue_retry_total counter\ntaskqueue_retry_total %d\n", snap.RetryTotal)
	fmt.Fprintf(&b, "# HELP taskqueue_dead_total Tasks sent to dead letter queue\n# TYPE taskqueue_dead_total counter\ntaskqueue_dead_total %d\n", snap.DeadTotal)
	fmt.Fprintf(&b, "# HELP taskqueue_worker_busy Number of workers currently executing\n# TYPE taskqueue_worker_busy gauge\ntaskqueue_worker_busy %d\n", snap.BusyWorkers)
	fmt.Fprintf(&b, "# HELP taskqueue_enqueue_duration_p50_seconds Enqueue latency p50 in seconds\n# TYPE taskqueue_enqueue_duration_p50_seconds gauge\ntaskqueue_enqueue_duration_p50_seconds %f\n", float64(snap.EnqueueP50Ns)/1e9)
	fmt.Fprintf(&b, "# HELP taskqueue_enqueue_duration_p99_seconds Enqueue latency p99 in seconds\n# TYPE taskqueue_enqueue_duration_p99_seconds gauge\ntaskqueue_enqueue_duration_p99_seconds %f\n", float64(snap.EnqueueP99Ns)/1e9)
	fmt.Fprintf(&b, "# HELP taskqueue_execute_duration_p50_seconds Execution latency p50 in seconds\n# TYPE taskqueue_execute_duration_p50_seconds gauge\ntaskqueue_execute_duration_p50_seconds %f\n", float64(snap.ExecuteP50Ns)/1e9)
	fmt.Fprintf(&b, "# HELP taskqueue_execute_duration_p99_seconds Execution latency p99 in seconds\n# TYPE taskqueue_execute_duration_p99_seconds gauge\ntaskqueue_execute_duration_p99_seconds %f\n", float64(snap.ExecuteP99Ns)/1e9)
	return b.String()
}
