package infrahttpgin

import (
	infraconfig "FluxFeed/internal/infra/config"
	inframetrics "FluxFeed/internal/infra/metrics"
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Init 创建 Gin 引擎，并使用默认日志和恢复中间件。
func Init() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	g := gin.Default()
	g.Use(inframetrics.HTTPMiddleware())
	return g
}

// Run 根据配置端口启动 HTTP 服务。
func Run(ctx context.Context, cfg *infraconfig.Config, g *gin.Engine) error {
	port := cfg.Port
	addr := ":" + strconv.Itoa(port)
	server := &http.Server{
		Addr:              addr,
		Handler:           g,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownTimeout := parseDuration(cfg.Governance.ShutdownTimeout, 5*time.Second)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}

func parseDuration(raw string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
