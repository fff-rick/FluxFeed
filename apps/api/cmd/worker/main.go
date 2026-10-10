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
	infraobservability "FluxFeed/internal/infra/observability"
	infraembedding "FluxFeed/internal/infra/persistence/embedding"
	infraeventbus "FluxFeed/internal/infra/persistence/eventbus"
	infrafeed "FluxFeed/internal/infra/persistence/feed"
	infragovernance "FluxFeed/internal/infra/persistence/governance"
	infrainteraction "FluxFeed/internal/infra/persistence/interaction"
	migration "FluxFeed/internal/infra/persistence/migration"
	infraoutbox "FluxFeed/internal/infra/persistence/outbox"
	infrarecommendation "FluxFeed/internal/infra/persistence/recommendation"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const configPath = "./configs/config.yaml"

func main() {
	infraobservability.ConfigureLogging("fluxfeed-worker")
	shutdownTracing, err := infraobservability.ConfigureTracing(context.Background(), "fluxfeed-worker")
	if err != nil {
		log.Fatalf("init tracing failed: %v", err)
	}
	defer shutdownTracer(shutdownTracing)
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
	if err := inframetrics.RegisterDatabase(sqlDB); err != nil {
		log.Fatalf("register database metrics failed: %v", err)
	}

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

func shutdownTracer(shutdown func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		log.Printf("shutdown tracing failed: %v", err)
	}
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
	outboxRepo := infraoutbox.New(gormDB)
	if bus, ok := eventBus.(applicationeventbus.Bus); ok {
		go applicationeventbus.NewOutboxWorker(outboxRepo, bus).Run(ctx)
		go monitorOutbox(ctx, outboxRepo)
	}
	videoTopic := cfg.Kafka.VideoTopic
	if videoTopic == "" {
		videoTopic = "fluxfeed.video"
	}
	interactionTopic := cfg.Kafka.InteractionTopic
	if interactionTopic == "" {
		interactionTopic = "fluxfeed.interaction"
	}
	moderationService := infragovernance.New(gormDB, outboxRepo, videoTopic, interactionTopic)
	if _, ok := eventBus.(applicationeventbus.Bus); !ok {
		videoPublisher, _ := eventBus.(applicationvideo.PublishedEventPublisher)
		var comments applicationinteraction.CommentEventPublisher
		if value, yes := eventBus.(applicationinteraction.CommentEventPublisher); yes {
			comments = value
		}
		moderationService.SetPublishers(videoPublisher, comments)
	}
	go runModerationWorker(ctx, moderationService)
	redisClient := infracache.NewRedisClient(cfg.Redis)
	feedCache := infracache.NewFeedCache(redisClient)
	if bus, ok := eventBus.(applicationeventbus.Bus); ok {
		if err := bus.Subscribe(ctx, videoTopic, "fluxfeed-content-visibility", func(ctx context.Context, event *applicationeventbus.Event) error {
			if event.Type != applicationeventbus.TypeContentVisibilityChanged {
				return nil
			}
			var payload struct {
				VideoID int64  `json:"video_id"`
				Status  string `json:"status"`
			}
			if err := event.Decode(&payload); err != nil {
				return err
			}
			return feedCache.InvalidateVideo(ctx, payload.VideoID, payload.Status != "published")
		}); err != nil {
			return err
		}
	}

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

func runModerationWorker(ctx context.Context, service *infragovernance.Service) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := service.ProcessPending(ctx, 50); err != nil && ctx.Err() == nil {
			log.Printf("process moderation queue failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
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
