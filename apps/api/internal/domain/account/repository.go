package domainaccount

import (
	"context"
	"time"
)

type ExternalIdentity struct {
	UserID          int64
	Provider        string
	ProviderSubject string
	Email           string
}

type ActionToken struct {
	UserID    int64
	Purpose   string
	TokenHash string
	ExpiresAt time.Time
}

// Repository 定义账号领域需要的持久化能力，应用层只依赖这个接口。
type Repository interface {
	// Save 保存新用户，账号重复时返回 ErrAccountAlreadyExists。
	Save(ctx context.Context, user *User) error
	// FindByAccount 用于登录流程通过账号查找用户。
	FindByAccount(ctx context.Context, account string) (*User, error)
	// FindByID 用于根据登录态读取当前用户。
	FindByID(ctx context.Context, id int64) (*User, error)
	// UpdateProfile 只更新用户展示资料字段。
	UpdateProfile(ctx context.Context, user *User) error
	UpdatePassword(ctx context.Context, user *User) error
	MarkEmailVerified(ctx context.Context, userID int64, verifiedAt time.Time) error
	FindByExternalIdentity(ctx context.Context, provider, subject string) (*User, error)
	LinkExternalIdentity(ctx context.Context, identity ExternalIdentity) error
	SaveActionToken(ctx context.Context, token ActionToken) error
	ConsumeActionToken(ctx context.Context, purpose, tokenHash string, now time.Time) (int64, error)
}
