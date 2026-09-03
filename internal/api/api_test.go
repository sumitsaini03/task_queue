package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumitsaini/taskqueue/internal/config"
	"github.com/sumitsaini/taskqueue/internal/engine"
	"github.com/sumitsaini/taskqueue/internal/queue"
)

func setupTestServer(t *testing.T) (*Server, *engine.Engine) {
	t.Helper()

	cfg := config.Default()
	cfg.WAL.Dir = t.TempDir()
	cfg.WAL.SyncPolicy = "none"
	cfg.Queue.Capacity = 64
	cfg.Queue.OverflowPolicy = "reject"
	cfg.Worker.Count = 2
	cfg.HTTP.RateLimitBurst = 50
	cfg.HTTP.RateLimitPerSec = 100

	eng, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("engine init: %v", err)
	}
	if err := eng.Recover(); err != nil {
		t.Fatalf("recovery: %v", err)
	}

	server := NewServer(eng, ServerConfig{
		Addr:            ":0",
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		IdleTimeout:     5 * time.Second,
		RateLimitBurst:  cfg.HTTP.RateLimitBurst,
		RateLimitPerSec: cfg.HTTP.RateLimitPerSec,
	})

	return server, eng
}

func TestAPI_Health(t *testing.T) {
	server, eng := setupTestServer(t)
	defer eng.Shutdown(2 * time.Second)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	server.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status ok, got %s", body["status"])
	}
}

func TestAPI_Stats(t *testing.T) {
	server, eng := setupTestServer(t)
	defer eng.Shutdown(2 * time.Second)

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()
	server.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var stats engine.EngineStats
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func TestAPI_Metrics(t *testing.T) {
	server, eng := setupTestServer(t)
	defer eng.Shutdown(2 * time.Second)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	server.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Errorf("expected text/plain, got %s", contentType)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "taskqueue_depth") {
		t.Errorf("expected taskqueue_depth in metrics output, got: %s", body)
	}
}

func TestAPI_SubmitTask_Success(t *testing.T) {
	server, eng := setupTestServer(t)
	defer eng.Shutdown(2 * time.Second)

	payload := `{"payload":{"action":"resize","width":100},"priority":1}`
	req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
	}

	var res SubmitResponse
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if res.ID == "" {
		t.Error("expected non-empty task ID")
	}
	if res.State != "pending" {
		t.Errorf("expected state pending, got %s", res.State)
	}
}

func TestAPI_SubmitTask_InvalidPayload(t *testing.T) {
	server, eng := setupTestServer(t)
	defer eng.Shutdown(2 * time.Second)

	// Missing payload
	req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{"priority":1}`))
	rec := httptest.NewRecorder()
	server.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", rec.Code)
	}

	// Invalid JSON
	req = httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`not-json`))
	rec = httptest.NewRecorder()
	server.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", rec.Code)
	}
}

func TestAPI_SubmitTask_IdempotencyConflict(t *testing.T) {
	server, eng := setupTestServer(t)
	defer eng.Shutdown(2 * time.Second)

	body := `{"payload":{"data":"first"},"idempotency_key":"key-123"}`
	req1 := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(body))
	rec1 := httptest.NewRecorder()
	server.mux.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusCreated {
		t.Fatalf("expected 201 on first submit, got %d", rec1.Code)
	}

	// Second submit with same idempotency key must return 409 Conflict per api-contract.md
	req2 := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(body))
	rec2 := httptest.NewRecorder()
	server.mux.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusConflict {
		t.Errorf("expected 409 Conflict on duplicate idempotency key, got %d: %s", rec2.Code, rec2.Body.String())
	}
}

func TestAPI_GetTask_SuccessAndNotFound(t *testing.T) {
	server, eng := setupTestServer(t)
	defer eng.Shutdown(2 * time.Second)

	// Submit first
	task, err := eng.Submit(&queue.Task{
		Payload: []byte(`{"action":"process"}`),
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// GET existing
	req := httptest.NewRequest(http.MethodGet, "/tasks/"+task.ID, nil)
	rec := httptest.NewRecorder()
	server.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var res TaskResponse
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if res.ID != task.ID {
		t.Errorf("expected ID %s, got %s", task.ID, res.ID)
	}

	// GET nonexistent
	reqNF := httptest.NewRequest(http.MethodGet, "/tasks/nonexistent_12345", nil)
	recNF := httptest.NewRecorder()
	server.mux.ServeHTTP(recNF, reqNF)

	if recNF.Code != http.StatusNotFound {
		t.Errorf("expected 404 Not Found, got %d", recNF.Code)
	}
}

func TestAPI_CancelTask(t *testing.T) {
	server, eng := setupTestServer(t)
	defer eng.Shutdown(2 * time.Second)

	// Submit a task with future delay so it stays scheduled
	task, err := eng.Submit(&queue.Task{
		Payload:     []byte(`{}`),
		ScheduledAt: time.Now().Add(1 * time.Hour),
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// DELETE /tasks/{id}
	req := httptest.NewRequest(http.MethodDelete, "/tasks/"+task.ID, nil)
	rec := httptest.NewRecorder()
	server.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on cancel, got %d: %s", rec.Code, rec.Body.String())
	}

	// Cancelling again should return 409 Conflict
	reqConflict := httptest.NewRequest(http.MethodDelete, "/tasks/"+task.ID, nil)
	recConflict := httptest.NewRecorder()
	server.mux.ServeHTTP(recConflict, reqConflict)

	if recConflict.Code != http.StatusConflict {
		t.Errorf("expected 409 Conflict cancelling already cancelled, got %d", recConflict.Code)
	}

	// Cancel nonexistent should return 404
	reqNF := httptest.NewRequest(http.MethodDelete, "/tasks/does_not_exist", nil)
	recNF := httptest.NewRecorder()
	server.mux.ServeHTTP(recNF, reqNF)

	if recNF.Code != http.StatusNotFound {
		t.Errorf("expected 404 Not Found, got %d", recNF.Code)
	}
}

func TestAPI_DLQ(t *testing.T) {
	server, eng := setupTestServer(t)
	defer eng.Shutdown(2 * time.Second)

	// GET /dlq
	req := httptest.NewRequest(http.MethodGet, "/dlq?limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	server.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var dlq DLQResponse
	if err := json.NewDecoder(rec.Body).Decode(&dlq); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dlq.Total != 0 {
		t.Errorf("expected 0 total initially, got %d", dlq.Total)
	}
}

func TestAPI_RateLimit(t *testing.T) {
	// Create server with 1 burst token
	cfg := config.Default()
	cfg.WAL.Dir = t.TempDir()
	cfg.WAL.SyncPolicy = "none"
	cfg.HTTP.RateLimitBurst = 1
	cfg.HTTP.RateLimitPerSec = 0.001 // very slow refill

	eng, _ := engine.New(cfg)
	eng.Recover()
	defer eng.Shutdown(2 * time.Second)

	server := NewServer(eng, ServerConfig{
		Addr:            ":0",
		RateLimitBurst:  1,
		RateLimitPerSec: 0.001,
	})

	// First request succeeds
	req1 := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{"payload":{"a":1}}`))
	rec1 := httptest.NewRecorder()
	server.mux.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("expected 201 on first, got %d", rec1.Code)
	}

	// Second immediate request should be rate-limited: 429
	req2 := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{"payload":{"a":2}}`))
	rec2 := httptest.NewRecorder()
	server.mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429 Too Many Requests, got %d", rec2.Code)
	}
}

func TestAPI_MethodNotAllowed(t *testing.T) {
	server, eng := setupTestServer(t)
	defer eng.Shutdown(2 * time.Second)

	methods := []struct {
		method string
		path   string
	}{
		{http.MethodPut, "/tasks"},
		{http.MethodDelete, "/tasks"},
		{http.MethodPost, "/dlq"},
		{http.MethodPost, "/metrics"},
	}

	for _, tc := range methods {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		rec := httptest.NewRecorder()
		server.mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: expected 405 Method Not Allowed, got %d", tc.method, tc.path, rec.Code)
		}
	}
}

func TestAPI_FullQueue_503(t *testing.T) {
	// Tiny queue capacity of 1 with reject policy
	cfg := config.Default()
	cfg.WAL.Dir = t.TempDir()
	cfg.WAL.SyncPolicy = "none"
	cfg.Queue.Capacity = 1
	cfg.Queue.OverflowPolicy = "reject"
	cfg.Worker.Count = 1

	eng, _ := engine.New(cfg)
	eng.Recover()
	defer eng.Shutdown(2 * time.Second)

	server := NewServer(eng, ServerConfig{
		Addr:            ":0",
		RateLimitBurst:  100,
		RateLimitPerSec: 100,
	})

	// Fill queue: submit 1
	req1 := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{"payload":{"a":1}}`))
	rec1 := httptest.NewRecorder()
	server.mux.ServeHTTP(rec1, req1)

	// Submit 2 should get 503 Service Unavailable
	req2 := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(`{"payload":{"a":2}}`))
	rec2 := httptest.NewRecorder()
	server.mux.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable on full queue, got %d", rec2.Code)
	}
}
