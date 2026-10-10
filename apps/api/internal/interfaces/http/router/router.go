package interfaceshttprouter

import (
	applicationaccount "FluxFeed/internal/application/account"
	applicationeventbus "FluxFeed/internal/application/eventbus"
	applicationexposure "FluxFeed/internal/application/exposure"
	applicationfeed "FluxFeed/internal/application/feed"
	applicationgovernance "FluxFeed/internal/application/governance"
	applicationinteraction "FluxFeed/internal/application/interaction"
	applicationmessage "FluxFeed/internal/application/message"
	applicationplayback "FluxFeed/internal/application/playback"
	applicationrecommendation "FluxFeed/internal/application/recommendation"
	applicationrelation "FluxFeed/internal/application/relation"
	applicationvideo "FluxFeed/internal/application/video"
	domainfeed "FluxFeed/internal/domain/feed"
	infraauth "FluxFeed/internal/infra/auth"
	infracache "FluxFeed/internal/infra/cache"
	infraconfig "FluxFeed/internal/infra/config"
	infrajwt "FluxFeed/internal/infra/jwt"
	inframq "FluxFeed/internal/infra/mq"
	infraaccount "FluxFeed/internal/infra/persistence/account"
	infraexposure "FluxFeed/internal/infra/persistence/exposure"
	infrafeed "FluxFeed/internal/infra/persistence/feed"
	infragovernance "FluxFeed/internal/infra/persistence/governance"
	infrainteraction "FluxFeed/internal/infra/persistence/interaction"
	inframessage "FluxFeed/internal/infra/persistence/message"
	migration "FluxFeed/internal/infra/persistence/migration"
	infraoutbox "FluxFeed/internal/infra/persistence/outbox"
	infraplayback "FluxFeed/internal/infra/persistence/playback"
	infrarecommendation "FluxFeed/internal/infra/persistence/recommendation"
	infrarelation "FluxFeed/internal/infra/persistence/relation"
	infravideo "FluxFeed/internal/infra/persistence/video"
	interfaceshttpaccount "FluxFeed/internal/interfaces/http/account"
	interfaceshttpexposure "FluxFeed/internal/interfaces/http/exposure"
	interfaceshttpfeed "FluxFeed/internal/interfaces/http/feed"
	interfaceshttpgovernance "FluxFeed/internal/interfaces/http/governance"
	interfaceshttpinteraction "FluxFeed/internal/interfaces/http/interaction"
	interfaceshttpmessage "FluxFeed/internal/interfaces/http/message"
	interfaceshttpmiddleware "FluxFeed/internal/interfaces/http/middleware"
	interfaceshttpoutbox "FluxFeed/internal/interfaces/http/outbox"
	interfaceshttpplayback "FluxFeed/internal/interfaces/http/playback"
	interfaceshttprecommendation "FluxFeed/internal/interfaces/http/recommendation"
	interfaceshttprelation "FluxFeed/internal/interfaces/http/relation"
	interfaceshttpupload "FluxFeed/internal/interfaces/http/upload"
	interfaceshttpvideo "FluxFeed/internal/interfaces/http/video"
	"context"
	"database/sql"
	"log"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Register 负责后端依赖装配：数据库模型、仓储、Service、Handler、中间件和路由。
func Register(g *gin.Engine, cfg *infraconfig.Config, db *sql.DB) error {
	g.Use(
		interfaceshttpmiddleware.NewRateLimit(cfg.Governance.RateLimitRPS, cfg.Governance.RateLimitBurst),
		interfaceshttpmiddleware.NewRequestTimeout(governanceDuration(cfg.Governance.RequestTimeout, 3*time.Second)),
	)
	// database/sql 连接池交给 GORM 复用，避免维护两套数据库连接。
	gormDB, err := gorm.Open(gormmysql.New(gormmysql.Config{
		Conn: db,
	}), &gorm.Config{})
	if err != nil {
		return err
	}

	// AutoMigrate 根据模型创建或补齐表结构，适合教学项目快速启动。
	if err := migration.AutoMigrate(gormDB); err != nil {
		return err
	}
	if err := infragovernance.EnsureAdmin(gormDB, cfg.Admin.Account, cfg.Admin.Password); err != nil {
		return err
	}

	// JWT Manager 同时被账号服务用于签发 token，也被鉴权中间件用于校验 token。
	jwtManager, err := infrajwt.NewManager(cfg.JWT.Secret, cfg.JWT.AccessTTL)
	if err != nil {
		return err
	}

	// 下面按领域模块组装依赖：Repository -> Service -> Handler。
	accountRepo := infraaccount.New(gormDB)
	accountOptions := []applicationaccount.Option{applicationaccount.WithPublicURL(cfg.Auth.PublicURL)}
	if cfg.Auth.SMTP.Host != "" {
		accountOptions = append(accountOptions, applicationaccount.WithMailer(infraauth.NewSMTPMailer(cfg.Auth.SMTP), cfg.Auth.PublicURL))
	}
	if infraauth.OAuthProviderEnabled(cfg.Auth.Google.ClientID, cfg.Auth.Google.ClientSecret) {
		accountOptions = append(accountOptions, applicationaccount.WithOAuthProvider(infraauth.NewGoogleProvider(cfg.Auth.Google.ClientID, cfg.Auth.Google.ClientSecret, cfg.Auth.PublicURL)))
	}
	if infraauth.OAuthProviderEnabled(cfg.Auth.GitHub.ClientID, cfg.Auth.GitHub.ClientSecret) {
		accountOptions = append(accountOptions, applicationaccount.WithOAuthProvider(infraauth.NewGitHubProvider(cfg.Auth.GitHub.ClientID, cfg.Auth.GitHub.ClientSecret, cfg.Auth.PublicURL)))
	}
	accountService := applicationaccount.New(accountRepo, jwtManager, accountOptions...)
	accountHandler := interfaceshttpaccount.New(accountService)
	videoRepo := infravideo.New(gormDB)
	feedRepo := infrafeed.New(gormDB)
	recommendationRepo := infrarecommendation.New(gormDB)
	recommendationService := applicationrecommendation.New(recommendationRepo)
	recommendationHandler := interfaceshttprecommendation.New(recommendationService)
	governanceSwitches := applicationgovernance.NewSwitches()
	governanceHandler := interfaceshttpgovernance.New(governanceSwitches)
	feedOptions := []applicationfeed.Option{
		applicationfeed.WithRecommender(recommendationService),
		applicationfeed.WithRecommendationTimeout(governanceDuration(cfg.Governance.RecommendationTimeout, 500*time.Millisecond)),
		applicationfeed.WithRecommendationDegradeSwitch(func() bool {
			enabled, _ := governanceSwitches.Enabled(applicationgovernance.SwitchRecommendationDegraded)
			return enabled
		}),
	}
	videoOptions := []applicationvideo.Option{}
	interactionOptions := []applicationinteraction.Option{}
	exposureOptions := []applicationexposure.Option{}
	var feedCache *infracache.FeedCache
	if cfg.Redis.Addr != "" {
		redisClient := infracache.NewRedisClient(cfg.Redis)
		feedCache = infracache.NewFeedCache(redisClient)
		videoOptions = append(videoOptions, applicationvideo.WithVisibilityInvalidator(feedCache))
		feedOptions = append(feedOptions, applicationfeed.WithFeedCache(feedCache))
		interactionOptions = append(interactionOptions, applicationinteraction.WithHotScoreRecorder(feedCache))
		interactionOptions = append(interactionOptions, applicationinteraction.WithStatCache(feedCache))
		exposureOptions = append(exposureOptions, applicationexposure.WithSeenRecorder(feedCache))
	}
	interactionRepo := infrainteraction.New(gormDB)
	exposureRepo := infraexposure.New(gormDB)
	relationRepo := infrarelation.New(gormDB)
	messageRepo := inframessage.New(gormDB)
	messageService := applicationmessage.New(messageRepo)
	messageHandler := interfaceshttpmessage.New(messageService)
	playbackRepo := infraplayback.New(gormDB)
	playbackService := applicationplayback.New(playbackRepo)
	playbackHandler := interfaceshttpplayback.New(playbackService)
	outboxRepo := infraoutbox.New(gormDB)
	outboxHandler := interfaceshttpoutbox.New(outboxRepo)
	moderationService := infragovernance.New(gormDB, outboxRepo, normalizedTopic(cfg.Kafka.VideoTopic, "fluxfeed.video"), normalizedTopic(cfg.Kafka.InteractionTopic, "fluxfeed.interaction"))
	moderationHandler := interfaceshttpgovernance.NewModeration(moderationService)
	if feedCache != nil {
		moderationService.SetVisibilityInvalidator(feedCache)
	}
	feedOptions = append(feedOptions, applicationfeed.WithVisibilityFilter(moderationService))
	feedService := applicationfeed.New(feedRepo, feedOptions...)
	feedHandler := interfaceshttpfeed.New(feedService)
	videoOptions = append(videoOptions, applicationvideo.WithReviewSubmitter(moderationService))
	interactionOptions = append(interactionOptions, applicationinteraction.WithCommentReviewSubmitter(moderationService))
	interactionOptions = append(interactionOptions, applicationinteraction.WithCommentVisibility(moderationService))
	eventPublisher := newApplicationEventPublisher(cfg)
	if eventPublisher != nil {
		if _, ok := eventPublisher.(applicationeventbus.Bus); !ok {
			var comments applicationinteraction.CommentEventPublisher
			if value, yes := eventPublisher.(applicationinteraction.CommentEventPublisher); yes {
				comments = value
			}
			moderationService.SetPublishers(eventPublisher, comments)
		}
	}
	if eventPublisher != nil {
		if _, kafkaEnabled := eventPublisher.(*inframq.FeedEventBus); kafkaEnabled {
			videoRepo.EnablePublishedOutbox(outboxRepo, normalizedTopic(cfg.Kafka.VideoTopic, "fluxfeed.video"))
			interactionRepo.EnableCommentOutbox(outboxRepo, normalizedTopic(cfg.Kafka.InteractionTopic, "fluxfeed.interaction"))
			exposureRepo.EnableViewEventOutbox(outboxRepo, normalizedTopic(cfg.Kafka.ExposureTopic, "fluxfeed.exposure"))
			relationRepo.EnableFollowOutbox(outboxRepo, normalizedTopic(cfg.Kafka.RelationTopic, "fluxfeed.relation"))
		} else {
			videoOptions = append(videoOptions, applicationvideo.WithPublishedEventPublisher(eventPublisher))
			exposureOptions = append(exposureOptions, applicationexposure.WithViewEventPublisher(eventPublisher))
			if publisher, ok := eventPublisher.(applicationinteraction.CommentEventPublisher); ok {
				interactionOptions = append(interactionOptions, applicationinteraction.WithCommentEventPublisher(publisher))
			}
		}
		if feedCache != nil {
			interactionOptions = append(interactionOptions, applicationinteraction.WithAsyncActionPipeline(feedCache, eventPublisher))
		}
	}
	messageWriter := NewMessageWriter(messageService)
	interactionOptions = append(interactionOptions, applicationinteraction.WithMessageWriter(messageWriter))
	relationOptions := []applicationrelation.Option{applicationrelation.WithMessageWriter(messageWriter), applicationrelation.WithBlockChecker(moderationService)}
	if _, kafkaEnabled := eventPublisher.(*inframq.FeedEventBus); !kafkaEnabled {
		publisher, ok := eventPublisher.(applicationrelation.FollowEventPublisher)
		if ok {
			relationOptions = append(relationOptions, applicationrelation.WithFollowEventPublisher(publisher))
		}
	}
	videoService := applicationvideo.New(videoRepo, videoOptions...)
	videoHandler := interfaceshttpvideo.New(videoService)
	videoHandler.SetVisibility(moderationService)
	interactionService := applicationinteraction.New(interactionRepo, interactionOptions...)
	interactionHandler := interfaceshttpinteraction.New(interactionService)
	exposureService := applicationexposure.New(exposureRepo, exposureOptions...)
	exposureHandler := interfaceshttpexposure.New(exposureService)
	if feedCache != nil {
		relationOptions = append(relationOptions, applicationrelation.WithFollowFeedBackfiller(NewFollowFeedBackfiller(feedRepo, feedCache)))
	}
	relationService := applicationrelation.New(relationRepo, relationOptions...)
	relationHandler := interfaceshttprelation.New(relationService)
	uploadHandler := interfaceshttpupload.New("./uploads")
	uploadHandler.SetAssetStore(moderationService)

	g.GET("/health", HealthCheck)
	g.GET("/metrics", gin.WrapH(promhttp.Handler()))

	authMiddleware := interfaceshttpmiddleware.NewJWTAuthWithAccount(jwtManager, moderationService.AccountActive)
	optionalAuthMiddleware := interfaceshttpmiddleware.NewOptionalJWTAuthWithAccount(jwtManager, moderationService.AccountActive)
	g.GET("/uploads/*filepath", optionalAuthMiddleware, uploadHandler.Serve)
	api := g.Group("/api")

	// RESTful 路由约定：路径表达资源，HTTP 方法表达动作。
	// 会话资源用于登录态：创建会话表示登录，删除当前会话表示登出。
	sessions := api.Group("/sessions")
	sessions.POST("", accountHandler.Login)
	sessions.DELETE("/current", authMiddleware, accountHandler.Logout)
	api.GET("/auth/capabilities", accountHandler.Capabilities)
	api.POST("/email-verifications", accountHandler.VerifyEmail)
	api.POST("/password-reset-requests", accountHandler.RequestPasswordReset)
	api.POST("/password-resets", accountHandler.ResetPassword)
	api.GET("/oauth/:provider", accountHandler.BeginOAuth)
	api.GET("/oauth/:provider/callback", accountHandler.FinishOAuth)
	api.POST("/oauth/sessions", accountHandler.ExchangeOAuthSession)

	// 用户资源承载注册、当前用户资料和用户作品列表。
	users := api.Group("/users")
	users.POST("", accountHandler.Register)
	users.GET("/me", authMiddleware, accountHandler.Me)
	users.PATCH("/me", authMiddleware, accountHandler.UpdateMe)
	users.POST("/me/email-verification-requests", authMiddleware, accountHandler.RequestEmailVerification)
	users.GET("/me/videos", authMiddleware, videoHandler.ListMine)
	users.PUT("/me/following/:targetUserId", authMiddleware, relationHandler.Follow)
	users.DELETE("/me/following/:targetUserId", authMiddleware, relationHandler.Unfollow)
	users.GET("/me/following", authMiddleware, relationHandler.ListFollowing)
	users.GET("/me/followers", authMiddleware, relationHandler.ListFollowers)
	users.GET("/:userId", optionalAuthMiddleware, interfaceshttpgovernance.RequireUserVisible(moderationService, "userId"), accountHandler.Get)
	users.GET("/:userId/videos", optionalAuthMiddleware, videoHandler.ListByAuthor)

	// 视频是互动资源的父资源，点赞、收藏和评论都挂在具体视频下。
	videos := api.Group("/videos")
	videos.POST("", authMiddleware, videoHandler.Create)
	videos.PATCH("/:videoId", authMiddleware, videoHandler.Update)
	api.POST("/video-drafts", authMiddleware, videoHandler.CreateDraft)
	api.PATCH("/video-drafts/:videoId", authMiddleware, videoHandler.UpdateDraft)
	api.POST("/video-drafts/:videoId/submissions", authMiddleware, videoHandler.SubmitDraft)
	videos.GET("/:videoId", optionalAuthMiddleware, videoHandler.Get)
	videos.DELETE("/:videoId", authMiddleware, videoHandler.Delete)
	videos.PUT("/:videoId/like", authMiddleware, interactionHandler.Like)
	videos.DELETE("/:videoId/like", authMiddleware, interactionHandler.Unlike)
	videos.PUT("/:videoId/favorite", authMiddleware, interactionHandler.Favorite)
	videos.DELETE("/:videoId/favorite", authMiddleware, interactionHandler.Unfavorite)
	videos.POST("/:videoId/comments", authMiddleware, interactionHandler.CreateComment)
	videos.GET("/:videoId/comments", optionalAuthMiddleware, interactionHandler.ListComments)

	uploads := api.Group("/uploads", authMiddleware)
	uploads.POST("", uploadHandler.Create)

	// Feed 暴露为条目集合，客户端通过游标和 limit 控制分页。
	api.GET("/feed-items", optionalAuthMiddleware, feedHandler.ListFeedItems)
	api.POST("/feed-queries", optionalAuthMiddleware, feedHandler.Query)
	api.POST("/video-view-events", authMiddleware, exposureHandler.CreateViewEvent)
	// 删除评论只需要评论自身 ID，所以放在顶层 comments 资源下。
	api.DELETE("/comments/:commentId", authMiddleware, interactionHandler.DeleteComment)
	api.GET("/messages", authMiddleware, messageHandler.List)
	api.PATCH("/messages", authMiddleware, messageHandler.MarkRead)
	api.GET("/message-stats/unread", authMiddleware, messageHandler.CountUnread)
	api.GET("/playback-config", authMiddleware, playbackHandler.GetConfig)
	api.GET("/preload-videos", authMiddleware, playbackHandler.ListPreloadVideos)
	api.POST("/playback-qos-reports", authMiddleware, playbackHandler.CreateQoSReport)
	api.POST("/reports", authMiddleware, moderationHandler.CreateReport)
	api.GET("/reports/me", authMiddleware, moderationHandler.ListMyReports)
	api.PUT("/users/me/blocks/:userId", authMiddleware, moderationHandler.Block)
	api.DELETE("/users/me/blocks/:userId", authMiddleware, moderationHandler.Unblock)
	api.POST("/appeals/:targetType/:targetId", authMiddleware, moderationHandler.CreateAppeal)
	api.GET("/appeals/me", authMiddleware, moderationHandler.ListMyAppeals)

	admin := api.Group("/admin", authMiddleware)
	admin.GET("/reviews", interfaceshttpgovernance.RequirePermission(moderationService, "review.read"), moderationHandler.ListReviews)
	admin.POST("/reviews/:reviewId/decisions", interfaceshttpgovernance.RequirePermission(moderationService, "review.decide"), moderationHandler.DecideReview)
	admin.GET("/appeals", interfaceshttpgovernance.RequirePermission(moderationService, "appeal.read"), moderationHandler.ListAppeals)
	admin.POST("/appeals/:appealId/decisions", interfaceshttpgovernance.RequirePermission(moderationService, "appeal.decide"), moderationHandler.DecideAppeal)
	admin.GET("/reports", interfaceshttpgovernance.RequirePermission(moderationService, "report.read"), moderationHandler.ListReports)
	admin.POST("/reports/:reportId/resolution", interfaceshttpgovernance.RequirePermission(moderationService, "report.resolve"), moderationHandler.ResolveReport)
	admin.POST("/content/:targetType/:targetId/offline", interfaceshttpgovernance.RequirePermission(moderationService, "content.moderate"), moderationHandler.Offline)
	admin.POST("/content/:targetType/:targetId/restore", interfaceshttpgovernance.RequirePermission(moderationService, "content.moderate"), moderationHandler.Restore)
	admin.POST("/users/:userId/ban", interfaceshttpgovernance.RequirePermission(moderationService, "user.ban"), moderationHandler.Ban)
	admin.POST("/users/:userId/unban", interfaceshttpgovernance.RequirePermission(moderationService, "user.ban"), moderationHandler.Unban)
	admin.PUT("/users/:userId/role", interfaceshttpgovernance.RequirePermission(moderationService, "role.assign"), moderationHandler.AssignRole)
	admin.GET("/audit-logs", interfaceshttpgovernance.RequirePermission(moderationService, "audit.read"), moderationHandler.ListAudit)

	internal := g.Group("/internal")
	internal.GET("/governance/degrade-switches", interfaceshttpmiddleware.NewInternalTokenAuth(cfg.Internal.Token), governanceHandler.List)
	internal.PATCH("/governance/degrade-switches/:key", interfaceshttpmiddleware.NewInternalTokenAuth(cfg.Internal.Token), governanceHandler.Update)
	internal.POST("/recommendation-candidates", recommendationHandler.ListCandidates)
	internal.POST("/exposure-decisions", recommendationHandler.DecideExposures)
	internal.POST("/exposures", recommendationHandler.SaveExposures)
	internal.POST("/messages", interfaceshttpmiddleware.NewInternalTokenAuth(cfg.Internal.Token), messageHandler.Create)
	internal.POST("/playback-qos-reports", interfaceshttpmiddleware.NewInternalTokenAuth(cfg.Internal.Token), playbackHandler.CreateInternalQoSReport)
	internal.GET("/outbox-events", interfaceshttpmiddleware.NewInternalTokenAuth(cfg.Internal.Token), outboxHandler.List)
	internal.POST("/moderation/results", interfaceshttpmiddleware.NewInternalTokenAuth(cfg.Internal.Token), moderationHandler.MachineResult)

	return nil
}

func normalizedTopic(topic string, fallback string) string {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return fallback
	}
	return topic
}

func governanceDuration(raw string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

type applicationEventPublisher interface {
	applicationvideo.PublishedEventPublisher
	applicationinteraction.ActionEventPublisher
	applicationexposure.ViewEventPublisher
}

func newApplicationEventPublisher(cfg *infraconfig.Config) applicationEventPublisher {
	if len(cfg.Kafka.Brokers) > 0 {
		kafka, err := inframq.NewKafka(cfg.Kafka)
		if err == nil {
			log.Printf("event bus enabled: kafka")
			return inframq.NewFeedEventBus(kafka, cfg.Kafka)
		}
		log.Printf("kafka unavailable, falling back to rabbitmq: %v", err)
	}
	if cfg.RabbitMQ.URL == "" {
		return nil
	}
	rabbitMQ, err := inframq.NewRabbitMQ(cfg.RabbitMQ)
	if err != nil {
		log.Printf("rabbitmq disabled: %v", err)
		return nil
	}
	log.Printf("event bus enabled: rabbitmq")
	return rabbitMQ
}

type MessageWriter struct {
	service *applicationmessage.Service
}

func NewMessageWriter(service *applicationmessage.Service) *MessageWriter {
	return &MessageWriter{service: service}
}

func (w *MessageWriter) CreateFromEvent(ctx context.Context, userID int64, messageType string, title string, content string, eventID string, idempotencyKey string) (any, error) {
	return w.service.CreateFromEvent(ctx, userID, messageType, title, content, eventID, idempotencyKey)
}

func (w *MessageWriter) CreateFromActorEvent(ctx context.Context, userID int64, messageType string, title string, content string, eventID string, idempotencyKey string, actorID int64, actorNickname string, actorAvatarURL string) (any, error) {
	return w.service.CreateFromActorEvent(ctx, userID, messageType, title, content, eventID, idempotencyKey, actorID, actorNickname, actorAvatarURL)
}

type FollowFeedBackfiller struct {
	feedRepo interface {
		CountFollowers(ctx context.Context, authorID int64) (int, error)
		ListAuthorRecentVideos(ctx context.Context, authorID int64, limit int) ([]*domainfeed.FeedPageItem, error)
	}
	feedCache interface {
		AddInboxItems(ctx context.Context, authorID int64, userIDs []int64, item *domainfeed.FeedPageItem, maxLen int64) error
		InvalidateFollowingIndex(ctx context.Context, viewerID int64) error
	}
}

func NewFollowFeedBackfiller(feedRepo interface {
	CountFollowers(ctx context.Context, authorID int64) (int, error)
	ListAuthorRecentVideos(ctx context.Context, authorID int64, limit int) ([]*domainfeed.FeedPageItem, error)
}, feedCache interface {
	AddInboxItems(ctx context.Context, authorID int64, userIDs []int64, item *domainfeed.FeedPageItem, maxLen int64) error
	InvalidateFollowingIndex(ctx context.Context, viewerID int64) error
}) *FollowFeedBackfiller {
	return &FollowFeedBackfiller{feedRepo: feedRepo, feedCache: feedCache}
}

func (b *FollowFeedBackfiller) CountFollowers(ctx context.Context, authorID int64) (int, error) {
	return b.feedRepo.CountFollowers(ctx, authorID)
}

func (b *FollowFeedBackfiller) ListAuthorRecentVideos(ctx context.Context, authorID int64, limit int) ([]*domainfeed.FeedPageItem, error) {
	return b.feedRepo.ListAuthorRecentVideos(ctx, authorID, limit)
}

func (b *FollowFeedBackfiller) AddInboxItems(ctx context.Context, authorID int64, userIDs []int64, item *domainfeed.FeedPageItem, maxLen int64) error {
	return b.feedCache.AddInboxItems(ctx, authorID, userIDs, item, maxLen)
}

func (b *FollowFeedBackfiller) InvalidateFollowingIndex(ctx context.Context, viewerID int64) error {
	return b.feedCache.InvalidateFollowingIndex(ctx, viewerID)
}

// HealthCheck 提供基础健康检查接口，方便本地调试和容器探活。
func HealthCheck(c *gin.Context) {
	c.JSON(200, gin.H{
		"message": "All is well",
	})
}
