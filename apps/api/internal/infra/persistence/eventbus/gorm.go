package infraeventbus

import (
	"context"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

type Deduplicator struct{ db *gorm.DB }

func NewDeduplicator(db *gorm.DB) *Deduplicator { return &Deduplicator{db: db} }

func (d *Deduplicator) IsProcessed(ctx context.Context, group string, eventID string) (bool, error) {
	var count int64
	err := d.db.WithContext(ctx).Model(&ConsumerEventModel{}).
		Where("group_id = ? AND event_id = ?", group, eventID).Count(&count).Error
	return count > 0, err
}

func (d *Deduplicator) MarkProcessed(ctx context.Context, group string, eventID string) error {
	err := d.db.WithContext(ctx).Create(&ConsumerEventModel{
		GroupID: group, EventID: eventID, ProcessedAt: time.Now().UTC(),
	}).Error
	if isDuplicate(err) {
		return nil
	}
	return err
}

func isDuplicate(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}
