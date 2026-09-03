# API Contract: Concurrent Task Queue

Base URL: `http://localhost:8080`

---

## POST /tasks — Submit a Task

Enqueue a new task for execution.

### Request

```json
{
  "payload": "<base64-encoded or JSON string>",
  "priority": 0,
  "delay_seconds": 0,
  "max_retries": 5,
  "timeout_seconds": 30,
  "idempotency_key": "optional-unique-key"
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `payload` | string | Yes | — | The task data passed to the handler |
| `priority` | int | No | 0 | Higher = higher priority (for heap scheduling) |
| `delay_seconds` | int | No | 0 | Delay before the task becomes eligible for execution |
| `max_retries` | int | No | 5 | Maximum retry attempts on failure |
| `timeout_seconds` | int | No | 30 | Per-execution timeout |
| `idempotency_key` | string | No | — | If provided, duplicate submissions with the same key are rejected |

### Response — `201 Created`

```json
{
  "id": "task_01H8MZXK...",
  "state": "pending",
  "created_at": "2026-09-03T13:00:00Z"
}
```

### Error Responses

| Status | Condition |
|--------|-----------|
| `400 Bad Request` | Missing/invalid payload |
| `409 Conflict` | Duplicate idempotency key |
| `429 Too Many Requests` | Rate limit exceeded |
| `503 Service Unavailable` | Queue at capacity (reject policy) |

---

## GET /tasks/{id} — Get Task Status

### Response — `200 OK`

```json
{
  "id": "task_01H8MZXK...",
  "state": "running",
  "payload": "...",
  "priority": 0,
  "created_at": "2026-09-03T13:00:00Z",
  "scheduled_at": "2026-09-03T13:00:05Z",
  "started_at": "2026-09-03T13:00:05Z",
  "completed_at": null,
  "retry_count": 0,
  "max_retries": 5,
  "last_error": null
}
```

### Error Responses

| Status | Condition |
|--------|-----------|
| `404 Not Found` | Task ID does not exist |

---

## DELETE /tasks/{id} — Cancel a Task

Cancel a pending or scheduled task. Running tasks cannot be cancelled (they must complete or fail).

### Response — `200 OK`

```json
{
  "id": "task_01H8MZXK...",
  "state": "cancelled",
  "cancelled_at": "2026-09-03T13:01:00Z"
}
```

### Error Responses

| Status | Condition |
|--------|-----------|
| `404 Not Found` | Task ID does not exist |
| `409 Conflict` | Task is already running, completed, or dead |

---

## GET /dlq — List Dead-Letter Queue

Returns tasks that exhausted all retry attempts.

### Query Parameters

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `limit` | int | 50 | Max items to return |
| `offset` | int | 0 | Pagination offset |

### Response — `200 OK`

```json
{
  "tasks": [
    {
      "id": "task_01H8MZXK...",
      "state": "dead",
      "payload": "...",
      "retry_count": 5,
      "max_retries": 5,
      "last_error": "connection timeout",
      "created_at": "2026-09-03T13:00:00Z",
      "completed_at": "2026-09-03T13:05:30Z"
    }
  ],
  "total": 1
}
```

---

## GET /health — Health Check

### Response — `200 OK`

```json
{
  "status": "ok"
}
```

---

## GET /metrics — Prometheus Metrics

Returns metrics in Prometheus exposition format. See Phase 5 for the full metric list.
