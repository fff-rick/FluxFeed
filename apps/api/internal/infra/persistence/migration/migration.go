package migration

import (
	infraaccount "FluxFeed/internal/infra/persistence/account"
	infraembedding "FluxFeed/internal/infra/persistence/embedding"
	infraexposure "FluxFeed/internal/infra/persistence/exposure"
	infrafeed "FluxFeed/internal/infra/persistence/feed"
	infrainteraction "FluxFeed/internal/infra/persistence/interaction"
	inframessage "FluxFeed/internal/infra/persistence/message"
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
	if err := autoMigrateModels(db); err != nil {
		return err
	}
	if err := infravideo.EnsureStats(db); err != nil {
		return err
	}
	return infrafeed.EnsureTimelineIndex(db)
}

func autoMigrateModels(db *gorm.DB) error {
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		err = db.AutoMigrate(
			&infraaccount.UserModel{},
			&infraembedding.VideoEmbeddingModel{},
			&infrarecommendation.UserInterestModel{},
			&infravideo.VideoModel{},
			&infravideo.VideoStatModel{},
			&infrafeed.InboxModel{},
			&infraexposure.ViewEventModel{},
			&infraexposure.ExposureModel{},
			&infrainteraction.ActionModel{},
			&infrainteraction.CommentModel{},
			&inframessage.MessageModel{},
			&infraplayback.ConfigModel{},
			&infraplayback.QoSLogModel{},
			&infrarelation.FollowModel{},
			&infrarelation.RelationStatModel{},
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
	case 1060, 1061:
		return true
	default:
		return false
	}
}
