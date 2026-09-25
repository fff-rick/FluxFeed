package infrarecommendation

import "time"

// UserInterestModel 保存 Feature Consumer 物化的用户兴趣向量。
type UserInterestModel struct {
	UserID        int64     `gorm:"column:user_id;primaryKey;autoIncrement:false"`
	Model         string    `gorm:"column:model;size:64;primaryKey"`
	Dimension     int       `gorm:"column:dimension;not null"`
	EmbeddingJSON string    `gorm:"column:embedding_json;type:json;not null"`
	UpdatedAt     time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (UserInterestModel) TableName() string { return "user_interest_embedding" }
