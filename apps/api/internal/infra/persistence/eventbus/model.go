package infraeventbus

import "time"

// ConsumerEventModel 用消费组和事件 ID 唯一标识一次已完成的消费。
type ConsumerEventModel struct {
	GroupID     string    `gorm:"column:group_id;size:128;primaryKey"`
	EventID     string    `gorm:"column:event_id;size:128;primaryKey"`
	ProcessedAt time.Time `gorm:"column:processed_at;not null;index"`
}

func (ConsumerEventModel) TableName() string { return "consumer_event" }
