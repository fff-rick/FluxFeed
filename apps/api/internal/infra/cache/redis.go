package infracache

import (
	infraconfig "FluxFeed/internal/infra/config"
	"crypto/tls"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient 创建 Redis 客户端，连接在首次命令执行时建立。
func NewRedisClient(cfg infraconfig.RedisConfig) *redis.Client {
	options := &redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		PoolTimeout:  200 * time.Millisecond,
	}
	if cfg.TLS {
		options.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.TLSServerName}
	}
	return redis.NewClient(options)
}
