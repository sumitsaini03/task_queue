package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestMetrics_Counters(t *testing.T) {
	m := New()

	m.RecordEnqueue(10 * time.Millisecond)
	m.RecordEnqueue(20 * time.Millisecond)
	m.RecordDequeue()
	m.RecordComplete(15 * time.Millisecond)
	m.RecordFail()
	m.RecordRetry()
	m.RecordDead()

	snap := m.GetSnapshot()
	if snap.EnqueueTotal != 2 {
		t.Errorf("expected 2 enqueues, got %d", snap.EnqueueTotal)
	}
	if snap.DequeueTotal != 1 {
		t.Errorf("expected 1 dequeue, got %d", snap.DequeueTotal)
	}
	if snap.CompleteTotal != 1 {
		t.Errorf("expected 1 complete, got %d", snap.CompleteTotal)
	}
	if snap.FailTotal != 1 {
		t.Errorf("expected 1 fail, got %d", snap.FailTotal)
	}
	if snap.RetryTotal != 1 {
		t.Errorf("expected 1 retry, got %d", snap.RetryTotal)
	}
	if snap.DeadTotal != 1 {
		t.Errorf("expected 1 dead, got %d", snap.DeadTotal)
	}
	if snap.QueueDepth != 1 {
		t.Errorf("expected queue depth 1 (2 enq - 1 deq), got %d", snap.QueueDepth)
	}
}

func TestMetrics_PrometheusFormat(t *testing.T) {
	m := New()
	m.RecordEnqueue(5 * time.Millisecond)
	m.RecordDequeue()
	m.RecordComplete(25 * time.Millisecond)

	out := m.PrometheusFormat()
	expectedSubstrings := []string{
		"# HELP taskqueue_depth",
		"# TYPE taskqueue_depth gauge",
		"taskqueue_enqueue_total 1",
		"taskqueue_dequeue_total 1",
		"taskqueue_completed_total 1",
		"taskqueue_enqueue_duration_p50_seconds",
		"taskqueue_execute_duration_p50_seconds",
	}

	for _, sub := range expectedSubstrings {
		if !strings.Contains(out, sub) {
			t.Errorf("PrometheusFormat missing %q in output:\n%s", sub, out)
		}
	}
}

func TestMetrics_PercentilesEmpty(t *testing.T) {
	p50, p99 := percentiles(nil)
	if p50 != 0 || p99 != 0 {
		t.Errorf("expected 0, 0 for empty slice, got %v, %v", p50, p99)
	}
}

func TestMetrics_PercentilesSorted(t *testing.T) {
	samples := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
		60 * time.Millisecond,
		70 * time.Millisecond,
		80 * time.Millisecond,
		90 * time.Millisecond,
		100 * time.Millisecond,
	}

	p50, p99 := percentiles(samples)
	if p50 != 60*time.Millisecond {
		t.Errorf("expected p50 around 60ms, got %v", p50)
	}
	if p99 != 100*time.Millisecond {
		t.Errorf("expected p99 around 100ms, got %v", p99)
	}
}
