package infraaccount

import "time"

// UserModel 映射 account 表，保存用户登录凭证和展示资料。
type UserModel struct {
	ID      int64  `gorm:"column:id;primaryKey;autoIncrement"`
	Account string `gorm:"column:account;size:255;not null;uniqueIndex"`
	// Password 保存 bcrypt 哈希值，登录时用于校验用户输入密码。
	Password        string     `gorm:"column:password;size:255;not null"`
	Nickname        string     `gorm:"column:nickname;size:128;not null"`
	AvatarURL       string     `gorm:"column:avatar_url;size:512"`
	Bio             string     `gorm:"column:bio;size:255"`
	Status          int        `gorm:"column:status;not null;default:1"`
	Role            string     `gorm:"column:role;size:32;not null"`
	EmailVerifiedAt *time.Time `gorm:"column:email_verified_at"`
	CreatedAt       time.Time  `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt       time.Time  `gorm:"column:updated_at;autoUpdateTime"`
}

type ExternalIdentityModel struct {
	ID              int64     `gorm:"column:id;primaryKey;autoIncrement"`
	UserID          int64     `gorm:"column:user_id;not null;index"`
	Provider        string    `gorm:"column:provider;size:32;not null;uniqueIndex:uk_external_identity"`
	ProviderSubject string    `gorm:"column:provider_subject;size:255;not null;uniqueIndex:uk_external_identity"`
	Email           string    `gorm:"column:email;size:255;not null"`
	CreatedAt       time.Time `gorm:"column:created_at;autoCreateTime"`
}

func (ExternalIdentityModel) TableName() string { return "account_external_identity" }

type ActionTokenModel struct {
	ID        int64      `gorm:"column:id;primaryKey;autoIncrement"`
	UserID    int64      `gorm:"column:user_id;not null;index"`
	Purpose   string     `gorm:"column:purpose;size:32;not null;index"`
	TokenHash string     `gorm:"column:token_hash;size:64;not null;uniqueIndex"`
	ExpiresAt time.Time  `gorm:"column:expires_at;not null;index"`
	UsedAt    *time.Time `gorm:"column:used_at"`
	CreatedAt time.Time  `gorm:"column:created_at;autoCreateTime"`
}

func (ActionTokenModel) TableName() string { return "account_action_token" }

// TableName 指定数据库表名，避免 GORM 使用默认复数表名。
func (UserModel) TableName() string {
	return "account"
}
