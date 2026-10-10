package migration

import (
	infraaccount "FluxFeed/internal/infra/persistence/account"
	infraembedding "FluxFeed/internal/infra/persistence/embedding"
	infraeventbus "FluxFeed/internal/infra/persistence/eventbus"
	infraexposure "FluxFeed/internal/infra/persistence/exposure"
	infrafeed "FluxFeed/internal/infra/persistence/feed"
	infragovernance "FluxFeed/internal/infra/persistence/governance"
	infrainteraction "FluxFeed/internal/infra/persistence/interaction"
	inframessage "FluxFeed/internal/infra/persistence/message"
	infraoutbox "FluxFeed/internal/infra/persistence/outbox"
	infraplayback "FluxFeed/internal/infra/persistence/playback"
	infrarecommendation "FluxFeed/internal/infra/persistence/recommendation"
	infrarelation "FluxFeed/internal/infra/persistence/relation"
	infravideo "FluxFeed/internal/infra/persistence/video"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

func AutoMigrate(db *gorm.DB) error {
	if err := removeDuplicateLegacyViewEvents(db); err != nil {
		return err
	}
	if err := autoMigrateModels(db); err != nil {
		return err
	}
	if err := infravideo.EnsureStats(db); err != nil {
		return err
	}
	if err := infragovernance.SeedRBAC(db); err != nil {
		return err
	}
	if err := infragovernance.BackfillMediaAssets(db); err != nil {
		return err
	}
	return infrafeed.EnsureTimelineIndex(db)
}

// removeDuplicateLegacyViewEvents keeps the earliest copy so the idempotency index can be added to upgraded databases.
func removeDuplicateLegacyViewEvents(db *gorm.DB) error {
	if !db.Migrator().HasTable(&infraexposure.ViewEventModel{}) {
		return nil
	}
	if db.Migrator().HasIndex(&infraexposure.ViewEventModel{}, "uk_view_event_request") {
		return nil
	}
	return db.Exec(`
		DELETE duplicate_event
		FROM video_view_events AS duplicate_event
		JOIN video_view_events AS original_event
		  ON original_event.user_id = duplicate_event.user_id
		 AND original_event.video_id = duplicate_event.video_id
		 AND original_event.event_type = duplicate_event.event_type
		 AND original_event.request_id = duplicate_event.request_id
		 AND original_event.id < duplicate_event.id
		WHERE duplicate_event.request_id IS NOT NULL
		  AND duplicate_event.request_id <> ''
	`).Error
}

func autoMigrateModels(db *gorm.DB) error {
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		err = db.AutoMigrate(
			&infraaccount.UserModel{},
			&infraaccount.ExternalIdentityModel{},
			&infraaccount.ActionTokenModel{},
			&infraembedding.VideoEmbeddingModel{},
			&infraeventbus.ConsumerEventModel{},
			&infrarecommendation.UserInterestModel{},
			&infravideo.VideoModel{},
			&infravideo.VideoStatModel{},
			&infrafeed.InboxModel{},
			&infraexposure.ViewEventModel{},
			&infraexposure.ExposureModel{},
			&infrainteraction.ActionModel{},
			&infrainteraction.CommentModel{},
			&inframessage.MessageModel{},
			&infraoutbox.EventModel{},
			&infraplayback.ConfigModel{},
			&infraplayback.QoSLogModel{},
			&infrarelation.FollowModel{},
			&infrarelation.RelationStatModel{},
			&infragovernance.ReviewModel{},
			&infragovernance.ReviewItemModel{},
			&infragovernance.StateHistoryModel{},
			&infragovernance.AppealModel{},
			&infragovernance.ReportModel{},
			&infragovernance.UserBlockModel{},
			&infragovernance.RoleModel{},
			&infragovernance.PermissionModel{},
			&infragovernance.RolePermissionModel{},
			&infragovernance.UserRoleModel{},
			&infragovernance.AuditLogModel{},
			&infragovernance.MediaAssetModel{},
		)
		if err == nil {
			return nil
		}
		if !isConcurrentMigrationError(err) {
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 150 * time.Millisecond)
	}
	return err
}

func isConcurrentMigrationError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	case 1050, 1060, 1061:
		return true
	default:
		return false
	}
}
