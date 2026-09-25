package infraexposure

import (
	applicationeventbus "FluxFeed/internal/application/eventbus"
	applicationexposure "FluxFeed/internal/application/exposure"
	domainexposure "FluxFeed/internal/domain/exposure"
	domainvideo "FluxFeed/internal/domain/video"
	infraoutbox "FluxFeed/internal/infra/persistence/outbox"
	infravideo "FluxFeed/internal/infra/persistence/video"
	"context"
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Repository struct {
	db            *gorm.DB
	outbox        *infraoutbox.Repository
	exposureTopic string
}

func New(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) EnableViewEventOutbox(outbox *infraoutbox.Repository, topic string) {
	r.outbox = outbox
	r.exposureTopic = strings.TrimSpace(topic)
}

// SaveViewEvent 写入观看行为，并在 exposed 事件时 upsert 用户视频曝光聚合。
func (r *Repository) SaveViewEvent(ctx context.Context, event *domainexposure.ViewEvent) (*domainexposure.ViewEvent, *domainexposure.Exposure, error) {
	var eventModel ViewEventModel
	var exposureModel ExposureModel
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := ensurePublishedVideo(tx, event.VideoID); err != nil {
			return err
		}

		eventModel = ViewEventModel{
			UserID:    event.UserID,
			VideoID:   event.VideoID,
			Scene:     event.Scene,
			RequestID: stringPtr(event.RequestID),
			EventType: event.EventType,
			WatchMs:   event.WatchMs,
			Completed: event.Completed,
		}
		if err := tx.Create(&eventModel).Error; err != nil {
			return err
		}

		if event.CountsAsExposure() {
			exposureModel = ExposureModel{
				UserID:         event.UserID,
				VideoID:        event.VideoID,
				FirstExposedAt: eventModel.CreatedAt,
				LastExposedAt:  eventModel.CreatedAt,
				ExposureCount:  1,
				LastScene:      event.Scene,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "user_id"},
					{Name: "video_id"},
				},
				DoUpdates: clause.Assignments(map[string]any{
					"last_exposed_at": gorm.Expr("VALUES(last_exposed_at)"),
					"exposure_count":  gorm.Expr("exposure_count + 1"),
					"last_scene":      gorm.Expr("VALUES(last_scene)"),
					"updated_at":      gorm.Expr("VALUES(updated_at)"),
				}),
			}).Create(&exposureModel).Error; err != nil {
				return err
			}

			if err := tx.Where("user_id = ? AND video_id = ?", event.UserID, event.VideoID).Take(&exposureModel).Error; err != nil {
				return err
			}
		}
		if r.outbox != nil {
			saved := restoreViewEvent(eventModel)
			var exposure *domainexposure.Exposure
			if event.CountsAsExposure() {
				exposure = restoreExposure(exposureModel)
			}
			source := applicationexposure.NewViewEventRecordedEvent(saved, exposure)
			eventType := applicationeventbus.TypeVideoViewed
			if source.EventType == domainexposure.EventTypeExposed {
				eventType = applicationeventbus.TypeVideoExposed
			}
			envelope, err := applicationeventbus.New(source.EventID, eventType, source.UserID, source.VideoID, source.OccurredAt, source)
			if err != nil {
				return err
			}
			if err := r.outbox.Add(tx, r.exposureTopic, envelope); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, mapExposureError(err)
	}

	savedEvent := restoreViewEvent(eventModel)
	var exposure *domainexposure.Exposure
	if event.CountsAsExposure() {
		exposure = restoreExposure(exposureModel)
	}
	return savedEvent, exposure, nil
}

func ensurePublishedVideo(tx *gorm.DB, videoID int64) error {
	var video infravideo.VideoModel
	err := tx.Select("id").
		Where("id = ? AND status = ?", videoID, domainvideo.StatusPublished).
		Take(&video).
		Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domainexposure.ErrVideoNotFound
		}
		return err
	}
	return nil
}

func restoreViewEvent(model ViewEventModel) *domainexposure.ViewEvent {
	return domainexposure.RestoreViewEvent(
		model.ID,
		model.UserID,
		model.VideoID,
		model.Scene,
		stringValue(model.RequestID),
		model.EventType,
		model.WatchMs,
		model.Completed,
		model.CreatedAt,
	)
}

func restoreExposure(model ExposureModel) *domainexposure.Exposure {
	return domainexposure.RestoreExposure(
		model.ID,
		model.UserID,
		model.VideoID,
		model.FirstExposedAt,
		model.LastExposedAt,
		model.ExposureCount,
		model.LastScene,
		model.CreatedAt,
		model.UpdatedAt,
	)
}

func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func mapExposureError(err error) error {
	if errors.Is(err, domainexposure.ErrVideoNotFound) {
		return err
	}
	if isDuplicateKeyError(err) {
		return err
	}
	return err
}

func isDuplicateKeyError(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}
