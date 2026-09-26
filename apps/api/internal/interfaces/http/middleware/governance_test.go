package interfaceshttpmiddleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestRateLimitRejectsBeyondBurstAndRefills(t *testing.T) {
	limiter := newTokenBucket(2, 2)
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	limiter.now = func() time.Time { return now }
	if !limiter.allow("client") || !limiter.allow("client") || limiter.allow("client") {
		t.Fatal("unexpected initial token bucket decisions")
	}
	now = now.Add(500 * time.Millisecond)
	if !limiter.allow("client") {
		t.Fatal("expected one refilled token")
	}
}

func TestRequestTimeoutPropagatesDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(NewRequestTimeout(time.Second))
	router.GET("/deadline", func(c *gin.Context) {
		_, ok := c.Request.Context().Deadline()
		c.JSON(http.StatusOK, gin.H{"deadline": ok})
	})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/deadline", nil))
	if recorder.Code != http.StatusOK || recorder.Body.String() != "{\"deadline\":true}" {
		t.Fatalf("unexpected response: %d %s", recorder.Code, recorder.Body.String())
	}
}
