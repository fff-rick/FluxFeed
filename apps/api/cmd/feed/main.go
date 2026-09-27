package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	infraconfig "FluxFeed/internal/infra/config"
	infradatabase "FluxFeed/internal/infra/database"
	infrahttpgin "FluxFeed/internal/infra/httpgin"
	inframetrics "FluxFeed/internal/infra/metrics"
	infraobservability "FluxFeed/internal/infra/observability"
	interfaceshttprouter "FluxFeed/internal/interfaces/http/router"
)

const configPath = "./configs/config.yaml"

func main() {
	infraobservability.ConfigureLogging("fluxfeed-api")
	shutdownTracing, err := infraobservability.ConfigureTracing(context.Background(), "fluxfeed-api")
	if err != nil {
		log.Fatalf("init tracing failed: %v", err)
	}
	defer shutdownTracer(shutdownTracing)
	// 启动顺序保持简单：配置 -> 数据库 -> Gin -> 路由 -> 启动服务。
	cfg, err := infraconfig.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}
	log.Printf(
		"config loaded: port=%d database=%s:%d/%s jwt_access_ttl=%s",
		cfg.Port,
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.Name,
		cfg.JWT.AccessTTL,
	)

	// 数据库连接使用 database/sql 连接池，后续会被 GORM 复用。
	db, err := infradatabase.New(cfg.Database)
	if err != nil {
		log.Fatalf("init database failed: %v", err)
	}
	defer db.Close()
	if err := inframetrics.RegisterDatabase(db); err != nil {
		log.Fatalf("register database metrics failed: %v", err)
	}
	log.Println("database connection initialized")

	// Gin 引擎只负责 HTTP 入口，业务依赖在 router.Register 中装配。
	g := infrahttpgin.Init()
	log.Println("gin engine initialized")

	// router.Register 会完成仓储、Service、Handler 和中间件的组装。
	if err := interfaceshttprouter.Register(g, cfg, db); err != nil {
		log.Fatalf("init router failed: %v", err)
	}
	log.Println("router registered")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Run 会阻塞当前进程，收到终止信号后等待在途请求完成。
	log.Println("server is running")
	if err := infrahttpgin.Run(ctx, cfg, g); err != nil {
		log.Fatalf("run server failed: %v", err)
	}
	log.Println("server stopped")
}

func shutdownTracer(shutdown func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		log.Printf("shutdown tracing failed: %v", err)
	}
}
