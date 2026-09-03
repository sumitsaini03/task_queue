// Package logger provides structured JSON logging for the task queue.
package logger

import (
	"context"
	"log/slog"
	"os"
	"sync"
)

var (
	defaultLogger *slog.Logger
	once          sync.Once
)

// Init initializes the global structured logger with JSON format.
func Init(level slog.Level) {
	opts := &slog.HandlerOptions{
		Level: level,
	}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	defaultLogger = slog.New(handler)
	slog.SetDefault(defaultLogger)
}

// Get returns the default structured logger.
func Get() *slog.Logger {
	once.Do(func() {
		if defaultLogger == nil {
			Init(slog.LevelInfo)
		}
	})
	return defaultLogger
}

// Info logs an informational message with structured key-value attributes.
func Info(msg string, args ...any) {
	Get().Info(msg, args...)
}

// Warn logs a warning message with structured key-value attributes.
func Warn(msg string, args ...any) {
	Get().Warn(msg, args...)
}

// Error logs an error message with structured key-value attributes.
func Error(msg string, args ...any) {
	Get().Error(msg, args...)
}

// Debug logs a debug message with structured key-value attributes.
func Debug(msg string, args ...any) {
	Get().Debug(msg, args...)
}

// ContextKey is a custom type for context values.
type ContextKey string

const (
	// RequestIDKey is the context key for HTTP request IDs.
	RequestIDKey ContextKey = "request_id"
)

// WithRequestID returns a logger enriched with the request ID from context.
func WithRequestID(ctx context.Context) *slog.Logger {
	l := Get()
	if reqID, ok := ctx.Value(RequestIDKey).(string); ok && reqID != "" {
		return l.With(slog.String("request_id", reqID))
	}
	return l
}
