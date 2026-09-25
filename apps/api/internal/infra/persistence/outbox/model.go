package infraoutbox

import "time"

type EventModel struct {
	EventID       string     `gorm:"column:event_id;size:128;primaryKey"`
	Topic         string     `gorm:"column:topic;size:255;not null"`
	Payload       []byte     `gorm:"column:payload;type:longblob;not null"`
	Status        string     `gorm:"column:status;size:16;not null;index:idx_outbox_pending,priority:1"`
	Attempts      int        `gorm:"column:attempts;not null"`
	NextAttemptAt time.Time  `gorm:"column:next_attempt_at;not null;index:idx_outbox_pending,priority:2"`
	PublishedAt   *time.Time `gorm:"column:published_at"`
	LastError     string     `gorm:"column:last_error;type:text"`
	CreatedAt     time.Time  `gorm:"column:created_at;not null"`
	UpdatedAt     time.Time  `gorm:"column:updated_at;not null"`
}

func (EventModel) TableName() string { return "outbox_event" }
