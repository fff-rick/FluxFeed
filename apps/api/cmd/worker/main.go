package main

import (
	applicationeventbus "FluxFeed/internal/application/eventbus"
	"context"
	"database/sql"
	"log"
	"os/signal"
	"syscall"
	"time"

	applicationembedding "FluxFeed/internal/application/embedding"
	applicationexposure "FluxFeed/internal/application/exposure"
	applicationinteraction "FluxFeed/internal/application/interaction"
	applicationrecommendation "FluxFeed/internal/application/recommendation"
	applicationvideo "FluxFeed/internal/application/video"
	infracache "FluxFeed/internal/infra/cache"
	infraconfig "FluxFeed/internal/infra/config"
	infradatabase "FluxFeed/internal/infra/database"
	inframetrics "FluxFeed/internal/infra/metrics"
	inframq "FluxFeed/internal/infra/mq"
	infraembedding "FluxFeed/internal/infra/persistence/embedding"
	infraeventbus "FluxFeed/internal/infra/persistence/eventbus"
	infrafeed "FluxFeed/internal/infra/persistence/feed"
	infrainteraction "FluxFeed/internal/infra/persistence/interaction"
	migration "FluxFeed/internal/infra/persistence/migration"
	infraoutbox "FluxFeed/internal/infra/persistence/outbox"
	infrarecommendation "FluxFeed/internal/infra/persistence/recommendation"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const configPath = "./configs/config.yaml"

func main() {
	cfg, err := infraconfig.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}
	if cfg.Redis.Addr == "" {
		log.Fatal("redis addr is required for worker")
	}

	sqlDB, err := infradatabase.New(cfg.Database)
	if err != nil {
		log.Fatalf("init database failed: %v", err)
	}
	defer closeSQL(sqlDB)

	gormDB, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		log.Fatalf("init gorm failed: %v", err)
	}
	if err := migration.AutoMigrate(gormDB); err != nil {
		log.Fatalf("auto migrate failed: %v", err)
	}

	eventBus, err := newWorkerEventBus(cfg, gormDB)
	if err != nil {
		log.Fatalf("init event bus failed: %v", err)
	}
	defer eventBus.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := inframetrics.RunServer(ctx, ":9091"); err != nil {
			log.Printf("metrics server failed: %v", err)
		}
	}()

	if err := startWorkers(ctx, cfg, gormDB, eventBus); err != nil {
		log.Fatalf("start workers failed: %v", err)
	}
	log.Println("gcfeed worker is running")
	<-ctx.Done()
	log.Println("gcfeed worker stopped")
}

type workerEventBus interface {
	applicationinteraction.ActionEventConsumer
	applicationvideo.PublishedEventConsumer
	ConsumeVideoPublishedForEmbedding(ctx context.Context, handler func(context.Context, *applicationvideo.PublishedEvent) error) error
	ConsumeViewEventRecorded(ctx context.Context, handler func(context.Context, *applicationexposure.ViewEventRecordedEvent) error) error
	Close() error
}

type recommendationEventConsumer interface {
	ConsumeViewEventRecordedForRecommendation(ctx context.Context, handler func(context.Context, *applicationexposure.ViewEventRecordedEvent) error) error
}

func newWorkerEventBus(cfg *infraconfig.Config, db *gorm.DB) (workerEventBus, error) {
	if len(cfg.Kafka.Brokers) > 0 {
		kafka, err := inframq.NewKafka(cfg.Kafka, inframq.WithConsumerDeduplicator(infraeventbus.NewDeduplicator(db)))
		if err == nil {
			log.Printf("event bus enabled: kafka")
			return inframq.NewFeedEventBus(kafka, cfg.Kafka), nil
		}
		log.Printf("kafka unavailable, falling back to rabbitmq: %v", err)
	}
	return inframq.NewRabbitMQ(cfg.RabbitMQ)
}

func startWorkers(ctx context.Context, cfg *infraconfig.Config, gormDB *gorm.DB, eventBus workerEventBus) error {
	if bus, ok := eventBus.(applicationeventbus.Bus); ok {
		outboxRepo := infraoutbox.New(gormDB)
		go applicationeventbus.NewOutboxWorker(outboxRepo, bus).Run(ctx)
		go monitorOutbox(ctx, outboxRepo)
	}
	redisClient := infracache.NewRedisClient(cfg.Redis)
	feedCache := infracache.NewFeedCache(redisClient)

	interactionRepo := infrainteraction.New(gormDB)
	actionWorker := applicationinteraction.NewActionWorker(interactionRepo, eventBus)
	if err := actionWorker.Start(ctx); err != nil {
		return err
	}

	feedRepo := infrafeed.New(gormDB)
	feedPreheater := applicationvideo.NewFeedPreheater(feedRepo, feedCache)
	fanoutWorker := applicationvideo.NewFanoutWorker(feedRepo, eventBus, feedCache, feedPreheater)
	if err := fanoutWorker.Start(ctx); err != nil {
		return err
	}

	embeddingRepo := infraembedding.New(gormDB)
	embeddingService := applicationembedding.New(embeddingRepo, nil)
	embeddingWorker := applicationembedding.NewVideoEmbeddingWorker(embeddingService, eventBus)
	if err := embeddingWorker.Start(ctx); err != nil {
		return err
	}

	recommendationRepo := infrarecommendation.New(gormDB)
	featureWorker := applicationembedding.NewUserFeatureWorker(recommendationRepo, eventBus)
	if err := featureWorker.Start(ctx); err != nil {
		return err
	}

	if consumer, ok := eventBus.(recommendationEventConsumer); ok {
		return applicationrecommendation.NewEventWorker(feedCache, consumer).Start(ctx)
	}
	return nil
}

func monitorOutbox(ctx context.Context, repo *infraoutbox.Repository) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		status, err := repo.Status(ctx)
		if err != nil {
			log.Printf("observe outbox status failed: %v", err)
		} else {
			inframetrics.SetOutboxStatus(status.Pending, status.Failed, status.OldestPendingAt)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func closeSQL(db *sql.DB) {
	if db != nil {
		_ = db.Close()
	}
}
