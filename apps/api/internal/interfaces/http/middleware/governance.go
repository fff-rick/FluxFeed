package interfaceshttpmiddleware

import (
	inframetrics "FluxFeed/internal/infra/metrics"
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const maxRateLimitClients = 10000

type tokenBucket struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	clients map[string]*clientBucket
	now     func() time.Time
}

type clientBucket struct {
	tokens float64
	last   time.Time
}

func newTokenBucket(requestsPerSecond int, burst int) *tokenBucket {
	if requestsPerSecond <= 0 {
		requestsPerSecond = 50
	}
	if burst < requestsPerSecond {
		burst = requestsPerSecond
	}
	return &tokenBucket{
		rate: float64(requestsPerSecond), burst: float64(burst),
		clients: make(map[string]*clientBucket), now: time.Now,
	}
}

func (l *tokenBucket) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	bucket := l.clients[key]
	if bucket == nil {
		if len(l.clients) >= maxRateLimitClients {
			// ponytail: 任意淘汰保持 O(1) 和有界内存；多实例全局配额需要 Redis 时再替换。
			for clientKey := range l.clients {
				delete(l.clients, clientKey)
				break
			}
		}
		l.clients[key] = &clientBucket{tokens: l.burst - 1, last: now}
		return true
	}
	bucket.tokens = min(l.burst, bucket.tokens+now.Sub(bucket.last).Seconds()*l.rate)
	bucket.last = now
	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

// NewRateLimit 使用进程内按 IP Token Bucket 保护 API；健康检查和指标端点不受限。
func NewRateLimit(requestsPerSecond int, burst int) gin.HandlerFunc {
	limiter := newTokenBucket(requestsPerSecond, burst)
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/health" || c.Request.URL.Path == "/metrics" || limiter.allow(c.ClientIP()) {
			c.Next()
			return
		}
		inframetrics.ObserveGovernance("http", "rate_limited")
		c.Header("Retry-After", "1")
		c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
	}
}

// NewRequestTimeout 把截止时间下传到数据库、Redis 和消息依赖。
func NewRequestTimeout(timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		if timeout <= 0 || c.Request.URL.Path == "/api/uploads" || strings.HasPrefix(c.Request.URL.Path, "/uploads/") {
			c.Next()
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
