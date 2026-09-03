// Package api implements the HTTP handlers and middleware for the
// task queue server, including rate limiting via a token bucket.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sumitsaini/taskqueue/internal/engine"
	"github.com/sumitsaini/taskqueue/internal/logger"
	"github.com/sumitsaini/taskqueue/internal/queue"
)

// Server holds the HTTP server and its dependencies.
type Server struct {
	engine  *engine.Engine
	limiter *TokenBucket
	mux     *http.ServeMux
	handler http.Handler
	server  *http.Server
	reqSeq  atomic.Uint64
}

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	RateLimitBurst  float64
	RateLimitPerSec float64
}

type responseWriterTracker struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriterTracker) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (s *Server) wrapMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = fmt.Sprintf("req_%d_%d", time.Now().UnixNano(), s.reqSeq.Add(1))
		}
		w.Header().Set("X-Request-ID", reqID)
		ctx := context.WithValue(r.Context(), logger.RequestIDKey, reqID)
		start := time.Now()

		rw := &responseWriterTracker{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r.WithContext(ctx))

		if r.URL.Path != "/metrics" {
			logger.WithRequestID(ctx).Info("http_request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rw.statusCode,
				"duration_ms", float64(time.Since(start).Microseconds())/1000.0,
			)
		}
	})
}

// ServeHTTP implements http.Handler, enabling in-memory testing through middleware.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// NewServer creates a new API server.
func NewServer(eng *engine.Engine, cfg ServerConfig) *Server {
	s := &Server{
		engine:  eng,
		limiter: NewTokenBucket(cfg.RateLimitBurst, cfg.RateLimitPerSec),
		mux:     http.NewServeMux(),
	}

	s.routes()
	s.handler = s.wrapMiddleware(s.mux)

	s.server = &http.Server{
		Addr:         cfg.Addr,
		Handler:      s.handler,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	return s
}

// Start begins accepting HTTP requests.
func (s *Server) Start() error {
	log.Printf("api: server starting on %s", s.server.Addr)
	err := s.server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/tasks", s.handleTasks)
	s.mux.HandleFunc("/tasks/", s.handleTaskByID)
	s.mux.HandleFunc("/dlq", s.handleDLQ)
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/stats", s.handleStats)
	s.mux.HandleFunc("/metrics", s.handleMetrics)
}

// --- Handlers ---

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.submitTask(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/tasks/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "task ID required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getTask(w, r, id)
	case http.MethodDelete:
		s.cancelTask(w, r, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) submitTask(w http.ResponseWriter, r *http.Request) {
	// Rate limit
	if !s.limiter.Allow() {
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}

	var req SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	if len(req.Payload) == 0 {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}

	task := &queue.Task{
		Payload:        req.Payload,
		Priority:       req.Priority,
		MaxRetries:     req.MaxRetries,
		IdempotencyKey: req.IdempotencyKey,
	}

	if req.DelaySeconds > 0 {
		task.ScheduledAt = time.Now().Add(time.Duration(req.DelaySeconds) * time.Second)
	}
	if req.TimeoutSeconds > 0 {
		task.Timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}

	result, err := s.engine.Submit(task)
	if err != nil {
		if errors.Is(err, engine.ErrDuplicateIdempotencyKey) {
			writeError(w, http.StatusConflict, "duplicate idempotency key")
			return
		}
		if strings.Contains(err.Error(), "queue: buffer is full") {
			writeError(w, http.StatusServiceUnavailable, "queue at capacity")
			return
		}
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("submit failed: %v", err))
		return
	}

	logger.WithRequestID(r.Context()).Info("task_submitted",
		"task_id", result.ID,
		"state", result.State.String(),
		"priority", result.Priority,
	)

	writeJSON(w, http.StatusCreated, SubmitResponse{
		ID:        result.ID,
		State:     result.State.String(),
		CreatedAt: result.CreatedAt,
	})
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request, id string) {
	task, ok := s.engine.GetTask(id)
	if !ok {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}

	writeJSON(w, http.StatusOK, TaskResponse{
		ID:          task.ID,
		State:       task.State.String(),
		Payload:     task.Payload,
		Priority:    task.Priority,
		CreatedAt:   task.CreatedAt,
		ScheduledAt: nilTime(task.ScheduledAt),
		StartedAt:   nilTime(task.StartedAt),
		CompletedAt: nilTime(task.CompletedAt),
		RetryCount:  task.RetryCount,
		MaxRetries:  task.MaxRetries,
		LastError:   task.LastError,
	})
}

func (s *Server) cancelTask(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.engine.CancelTask(id); err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		if strings.Contains(err.Error(), "cannot be cancelled") {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	logger.WithRequestID(r.Context()).Info("task_cancelled",
		"task_id", id,
	)

	task, _ := s.engine.GetTask(id)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":           id,
		"state":        "cancelled",
		"cancelled_at": task.CompletedAt,
	})
}

func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	limit := queryInt(r, "limit", 50)
	offset := queryInt(r, "offset", 0)

	tasks, total := s.engine.ListDLQ(limit, offset)

	dlqTasks := make([]TaskResponse, 0, len(tasks))
	for _, t := range tasks {
		dlqTasks = append(dlqTasks, TaskResponse{
			ID:          t.ID,
			State:       t.State.String(),
			Payload:     t.Payload,
			RetryCount:  t.RetryCount,
			MaxRetries:  t.MaxRetries,
			LastError:   t.LastError,
			CreatedAt:   t.CreatedAt,
			CompletedAt: nilTime(t.CompletedAt),
		})
	}

	writeJSON(w, http.StatusOK, DLQResponse{
		Tasks: dlqTasks,
		Total: total,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.engine.Stats()
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, s.engine.Metrics().PrometheusFormat())
}

// --- Request/Response types ---

// SubmitRequest is the JSON body for POST /tasks.
type SubmitRequest struct {
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	DelaySeconds   int             `json:"delay_seconds"`
	MaxRetries     int             `json:"max_retries"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	IdempotencyKey string          `json:"idempotency_key"`
}

// SubmitResponse is returned on successful task submission.
type SubmitResponse struct {
	ID        string    `json:"id"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
}

// TaskResponse is the full task representation.
type TaskResponse struct {
	ID          string     `json:"id"`
	State       string     `json:"state"`
	Payload     []byte     `json:"payload,omitempty"`
	Priority    int        `json:"priority"`
	CreatedAt   time.Time  `json:"created_at"`
	ScheduledAt *time.Time `json:"scheduled_at,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	RetryCount  int        `json:"retry_count"`
	MaxRetries  int        `json:"max_retries"`
	LastError   string     `json:"last_error,omitempty"`
}

// DLQResponse is returned by GET /dlq.
type DLQResponse struct {
	Tasks []TaskResponse `json:"tasks"`
	Total int            `json:"total"`
}

// ErrorResponse is a standard error payload.
type ErrorResponse struct {
	Error string `json:"error"`
}

// --- TokenBucket ---

// TokenBucket implements a simple token-bucket rate limiter.
type TokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

// NewTokenBucket creates a token bucket with the given capacity and refill rate.
func NewTokenBucket(maxTokens float64, refillRate float64) *TokenBucket {
	return &TokenBucket{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

// Allow checks if a request is allowed under the rate limit.
func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
	tb.lastRefill = now

	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}

func queryInt(r *http.Request, key string, defaultVal int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return n
}

func nilTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
