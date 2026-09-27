package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"
)

func TestHTTPMiddlewarePropagatesRequestIDAndWritesStructuredLog(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	router := gin.New()
	router.Use(HTTPMiddleware(logger))
	router.GET("/health", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	request.Header.Set(RequestIDHeader, "req-123")
	traceID := trace.TraceID{1, 2, 3}
	spanID := trace.SpanID{4, 5, 6}
	request = request.WithContext(trace.ContextWithRemoteSpanContext(request.Context(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID})))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if got := response.Header().Get(RequestIDHeader); got != "req-123" {
		t.Fatalf("request ID = %q, want req-123", got)
	}
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("decode structured log: %v", err)
	}
	if entry["request_id"] != "req-123" || entry["trace_id"] != traceID.String() || entry["route"] != "/health" || entry["status"] != float64(http.StatusNoContent) {
		t.Fatalf("unexpected log entry: %#v", entry)
	}
}

func TestHTTPMiddlewareGeneratesRequestID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(HTTPMiddleware(slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))))
	router.GET("/health", func(c *gin.Context) { c.Status(http.StatusOK) })

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health", nil))

	if got := response.Header().Get(RequestIDHeader); len(got) != 32 {
		t.Fatalf("generated request ID length = %d, want 32", len(got))
	}
}
