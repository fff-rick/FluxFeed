package infragovernance

import (
	domainaccount "FluxFeed/internal/domain/account"
	infraaccount "FluxFeed/internal/infra/persistence/account"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// EnsureAdmin idempotently creates or refreshes the administrator configured through the environment.
func EnsureAdmin(db *gorm.DB, account, password string) error {
	account, password = strings.TrimSpace(account), strings.TrimSpace(password)
	if account == "" && password == "" {
		return nil
	}
	if account == "" || password == "" {
		return fmt.Errorf("ADMIN and ADMIN_PW must be configured together")
	}
	admin, err := domainaccount.New(account, password, "FluxFeed Admin")
	if err != nil {
		return fmt.Errorf("invalid bootstrap admin: %w", err)
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var row infraaccount.UserModel
		err := tx.Where("account = ?", account).Take(&row).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			row = infraaccount.UserModel{Account: admin.Account, Password: admin.Password, Nickname: admin.Nickname, Status: domainaccount.StatusNormal, Role: domainaccount.RoleAdmin}
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
		case err != nil:
			return err
		default:
			if err := tx.Model(&row).Updates(map[string]any{"password": admin.Password, "role": domainaccount.RoleAdmin, "status": domainaccount.StatusNormal}).Error; err != nil {
				return err
			}
		}
		var super RoleModel
		if err := tx.Where("name = ?", "super_admin").Take(&super).Error; err != nil {
			return err
		}
		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&UserRoleModel{UserID: row.ID, RoleID: super.ID}).Error
	})
}

var rolePermissions = map[string][]string{
	"reviewer":    {"review.read", "review.decide", "appeal.read", "appeal.decide"},
	"moderator":   {"review.read", "review.decide", "appeal.read", "appeal.decide", "report.read", "report.resolve", "content.moderate", "user.ban"},
	"super_admin": {"review.read", "review.decide", "appeal.read", "appeal.decide", "report.read", "report.resolve", "content.moderate", "user.ban", "role.assign", "audit.read"},
}

func BackfillMediaAssets(db *gorm.DB) error {
	type videoRow struct {
		ID, AuthorID       int64
		MediaURL, CoverURL string
		Status             int
	}
	var videos []videoRow
	if err := db.Table("video").Select("id,author_id,media_url,cover_url,status").Find(&videos).Error; err != nil {
		return err
	}
	for _, video := range videos {
		status := "offline"
		if video.Status == 2 {
			status = "published"
		}
		for _, asset := range []struct{ kind, url string }{{"video", video.MediaURL}, {"cover", video.CoverURL}} {
			if !strings.HasPrefix(asset.url, "/uploads/") {
				continue
			}
			row := MediaAssetModel{OwnerID: video.AuthorID, Kind: asset.kind, URL: asset.url, Path: filepath.Join("uploads", strings.TrimPrefix(asset.url, "/uploads/")), Status: status, VideoID: video.ID}
			if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
				return err
			}
		}
	}
	type avatarRow struct {
		ID        int64
		AvatarURL string
	}
	var users []avatarRow
	if err := db.Table("account").Select("id,avatar_url").Where("avatar_url LIKE ?", "/uploads/%").Find(&users).Error; err != nil {
		return err
	}
	for _, user := range users {
		row := MediaAssetModel{OwnerID: user.ID, Kind: "avatar", URL: user.AvatarURL, Path: filepath.Join("uploads", strings.TrimPrefix(user.AvatarURL, "/uploads/")), Status: "published"}
		if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
			return err
		}
	}
	return nil
}

func SeedRBAC(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		for roleName, permissions := range rolePermissions {
			role := RoleModel{Name: roleName}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&role).Error; err != nil {
				return err
			}
			if err := tx.Where("name = ?", roleName).Take(&role).Error; err != nil {
				return err
			}
			for _, permissionName := range permissions {
				permission := PermissionModel{Name: permissionName}
				if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&permission).Error; err != nil {
					return err
				}
				if err := tx.Where("name = ?", permissionName).Take(&permission).Error; err != nil {
					return err
				}
				if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&RolePermissionModel{RoleID: role.ID, PermissionID: permission.ID}).Error; err != nil {
					return err
				}
			}
		}
		var super RoleModel
		if err := tx.Where("name = ?", "super_admin").Take(&super).Error; err != nil {
			return err
		}
		return tx.Exec("INSERT IGNORE INTO admin_user_role (user_id, role_id, created_at) SELECT id, ?, NOW() FROM account WHERE role = 'admin'", super.ID).Error
	})
}
