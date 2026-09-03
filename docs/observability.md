# Observability & Metrics Guide

The `taskqueue` system provides deep operational visibility into queue dynamics, worker utilization, latency distributions, and failure rates through **native Prometheus metrics** and **structured JSON logging**.

---

## 1. Metrics Catalog (`GET /metrics`)

All metrics are exposed at `http://localhost:8080/metrics` in the standard Prometheus exposition text format. No external agent or daemon is required.

| Metric Name | Type | Description | Healthy Baseline / Guidance |
| :--- | :--- | :--- | :--- |
| `taskqueue_depth` | Gauge | Current number of ready tasks waiting in the ring buffer. | Low (< 20% capacity). Sustained high depth indicates workers are saturated. |
| `taskqueue_delayed_depth` | Gauge | Number of delayed tasks waiting in the priority min-heap. | Proportional to scheduled future tasks. Non-blocking. |
| `taskqueue_worker_busy` | Gauge | Number of worker goroutines currently executing a task. | 0 to `worker.count`. High utilization is normal under active workload. |
| `taskqueue_enqueue_total` | Counter | Cumulative tasks submitted to the queue. | Monotonically increasing with incoming traffic. |
| `taskqueue_dequeue_total` | Counter | Cumulative tasks picked up by workers. | Tracks enqueue rate under healthy processing. |
| `taskqueue_completed_total`| Counter | Cumulative tasks that completed successfully. | Tracks dequeue rate unless errors are occurring. |
| `taskqueue_failed_total` | Counter | Cumulative task handler execution failures. | Should be near zero in steady state. Spikes indicate application bugs or downstream dependency outages. |
| `taskqueue_retry_total` | Counter | Cumulative retry attempts scheduled into the min-heap. | Backoff retries for transient failures. |
| `taskqueue_dead_total` | Counter | Cumulative tasks that exhausted all retries or returned non-retryable errors, moved to the DLQ. | 0 under normal operations. Requires manual DLQ inspection (`GET /dlq`). |
| `taskqueue_enqueue_duration_p50_seconds` | Gauge | Median latency of task submission (WAL append + buffer push). | < 0.001s (sub-millisecond under buffered WAL). |
| `taskqueue_enqueue_duration_p99_seconds` | Gauge | 99th percentile submission latency. | < 0.005s. Spikes correlate with disk fsync latency under `sync_policy: every`. |
| `taskqueue_execute_duration_p50_seconds` | Gauge | Median task handler execution runtime. | Application handler dependent. |
| `taskqueue_execute_duration_p99_seconds` | Gauge | 99th percentile task handler execution runtime. | Identifies slow tail latency or blocking operations. |

---

## 2. PromQL Reference Queries

For alerting and custom dashboards, use these production PromQL expressions:

### Ingestion vs. Consumption Rate (Throughput)
```promql
sum(rate(taskqueue_enqueue_total[1m]))
sum(rate(taskqueue_completed_total[1m]))
```

### Worker Saturation Ratio (0.0 – 1.0)
```promql
taskqueue_worker_busy / 8
```

### Error Rate Ratio
```promql
rate(taskqueue_failed_total[1m]) / (rate(taskqueue_completed_total[1m]) + rate(taskqueue_failed_total[1m]))
```

### Critical Alert: Unhandled Dead Letter Queue Growth
```promql
rate(taskqueue_dead_total[5m]) > 0
```

---

## 3. Structured JSON Logging & Request Tracing

Logs are emitted in structured JSON format via Go standard library `log/slog` directly to stdout.

### Tracing a Single Task Lifecycle
Every HTTP request generates or accepts an `X-Request-ID`. The task's lifecycle events can be traced from submission through worker completion:

```json
{"time":"2026-09-03T14:40:01.102Z","level":"INFO","msg":"http_request","request_id":"req_1788447401_1","method":"POST","path":"/tasks","status":201,"duration_ms":0.48}
{"time":"2026-09-03T14:40:01.102Z","level":"INFO","msg":"task_submitted","request_id":"req_1788447401_1","task_id":"task_1788447401_1","state":"pending","priority":1}
{"time":"2026-09-03T14:40:01.103Z","level":"INFO","msg":"task_started","task_id":"task_1788447401_1","attempt":0}
{"time":"2026-09-03T14:40:01.104Z","level":"INFO","msg":"task_completed","task_id":"task_1788447401_1","attempt":0}
```

### Filter By Task ID
```bash
# Grep all events for a specific task ID
docker logs taskqueue-server | grep "task_1788447401_1"

# Extract timestamps and events using jq
docker logs taskqueue-server | jq 'select(.task_id == "task_1788447401_1") | {time, msg, attempt, error}'
```

---

## 4. Grafana Dashboard

An importable dashboard is provided in [`docs/grafana-dashboard.json`](grafana-dashboard.json).

To import:
1. Open Grafana UI -> Dashboards -> New -> Import.
2. Paste the JSON contents or upload `docs/grafana-dashboard.json`.
3. Select your Prometheus data source.
