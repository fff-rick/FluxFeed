package infragovernance

import "time"

type ReviewModel struct {
	ID         int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	RequestID  string    `gorm:"column:request_id;size:64;not null;uniqueIndex" json:"request_id"`
	TargetType string    `gorm:"column:target_type;size:16;not null;index:idx_review_queue,priority:2;uniqueIndex:uk_review_version,priority:1" json:"target_type"`
	TargetID   int64     `gorm:"column:target_id;not null;uniqueIndex:uk_review_version,priority:2" json:"target_id"`
	Version    int       `gorm:"column:version;not null;default:1;uniqueIndex:uk_review_version,priority:3" json:"version"`
	Status     string    `gorm:"column:status;size:24;not null;index:idx_review_queue,priority:1" json:"status"`
	ReasonCode string    `gorm:"column:reason_code;size:64" json:"reason_code,omitempty"`
	Reason     string    `gorm:"column:reason;size:512" json:"reason,omitempty"`
	CreatedAt  time.Time `gorm:"column:created_at;autoCreateTime;index:idx_review_queue,priority:3"`
	UpdatedAt  time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (ReviewModel) TableName() string { return "content_review" }

type ReviewItemModel struct {
	ID         int64      `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	ReviewID   int64      `gorm:"column:review_id;not null;uniqueIndex:uk_review_component,priority:1;index" json:"review_id"`
	Component  string     `gorm:"column:component;size:16;not null;uniqueIndex:uk_review_component,priority:2" json:"component"`
	Content    string     `gorm:"column:content;size:1024" json:"content"`
	Verdict    string     `gorm:"column:verdict;size:16;not null;default:pending" json:"verdict"`
	ReasonCode string     `gorm:"column:reason_code;size:64" json:"reason_code,omitempty"`
	Reason     string     `gorm:"column:reason;size:512" json:"reason,omitempty"`
	Provider   string     `gorm:"column:provider;size:64" json:"provider,omitempty"`
	ReviewedAt *time.Time `gorm:"column:reviewed_at"`
	CreatedAt  time.Time  `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt  time.Time  `gorm:"column:updated_at;autoUpdateTime"`
}

func (ReviewItemModel) TableName() string { return "content_review_item" }

type StateHistoryModel struct {
	ID         int64     `gorm:"column:id;primaryKey;autoIncrement"`
	TargetType string    `gorm:"column:target_type;size:16;not null;index:idx_state_target,priority:1"`
	TargetID   int64     `gorm:"column:target_id;not null;index:idx_state_target,priority:2"`
	FromStatus int       `gorm:"column:from_status;not null"`
	ToStatus   int       `gorm:"column:to_status;not null"`
	ActorType  string    `gorm:"column:actor_type;size:16;not null"`
	ActorID    int64     `gorm:"column:actor_id;not null;default:0"`
	Reason     string    `gorm:"column:reason;size:512"`
	CreatedAt  time.Time `gorm:"column:created_at;autoCreateTime;index:idx_state_target,priority:3"`
}

func (StateHistoryModel) TableName() string { return "content_state_history" }

type AppealModel struct {
	ID             int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	TargetType     string    `gorm:"column:target_type;size:16;not null;index:idx_appeal_owner,priority:2;uniqueIndex:uk_active_appeal,priority:1" json:"target_type"`
	TargetID       int64     `gorm:"column:target_id;not null;uniqueIndex:uk_active_appeal,priority:2" json:"target_id"`
	ReviewID       int64     `gorm:"column:review_id;not null;uniqueIndex:uk_active_appeal,priority:3" json:"review_id"`
	UserID         int64     `gorm:"column:user_id;not null;index:idx_appeal_owner,priority:1;uniqueIndex:uk_appeal_idempotency,priority:1" json:"user_id"`
	Reason         string    `gorm:"column:reason;size:512;not null" json:"reason"`
	Status         string    `gorm:"column:status;size:16;not null;index" json:"status"`
	DecisionReason string    `gorm:"column:decision_reason;size:512" json:"decision_reason"`
	ReviewerID     int64     `gorm:"column:reviewer_id" json:"reviewer_id"`
	CreatedAt      time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt      time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	IdempotencyKey *string   `gorm:"column:idempotency_key;size:128;uniqueIndex:uk_appeal_idempotency,priority:2" json:"-"`
}

func (AppealModel) TableName() string { return "content_appeal" }

type ReportModel struct {
	ID             int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	ReporterID     int64     `gorm:"column:reporter_id;not null;index:idx_reporter_created,priority:1;uniqueIndex:uk_report_idempotency,priority:1" json:"reporter_id"`
	TargetType     string    `gorm:"column:target_type;size:16;not null;index:idx_report_queue,priority:2" json:"target_type"`
	TargetID       int64     `gorm:"column:target_id;not null" json:"target_id"`
	Category       string    `gorm:"column:category;size:64;not null" json:"category"`
	Description    string    `gorm:"column:description;size:512;not null" json:"description"`
	Status         string    `gorm:"column:status;size:16;not null;index:idx_report_queue,priority:1" json:"status"`
	Resolution     string    `gorm:"column:resolution;size:512" json:"resolution"`
	ResolverID     int64     `gorm:"column:resolver_id" json:"resolver_id"`
	CreatedAt      time.Time `gorm:"column:created_at;autoCreateTime;index:idx_reporter_created,priority:2;index:idx_report_queue,priority:3" json:"created_at"`
	UpdatedAt      time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	IdempotencyKey *string   `gorm:"column:idempotency_key;size:128;uniqueIndex:uk_report_idempotency,priority:2" json:"-"`
}

func (ReportModel) TableName() string { return "content_report" }

type UserBlockModel struct {
	BlockerID int64     `gorm:"column:blocker_id;primaryKey"`
	BlockedID int64     `gorm:"column:blocked_id;primaryKey"`
	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime"`
}

func (UserBlockModel) TableName() string { return "user_block" }

type RoleModel struct {
	ID   int64  `gorm:"column:id;primaryKey;autoIncrement"`
	Name string `gorm:"column:name;size:32;not null;uniqueIndex"`
}

func (RoleModel) TableName() string { return "admin_role" }

type PermissionModel struct {
	ID   int64  `gorm:"column:id;primaryKey;autoIncrement"`
	Name string `gorm:"column:name;size:64;not null;uniqueIndex"`
}

func (PermissionModel) TableName() string { return "admin_permission" }

type RolePermissionModel struct {
	RoleID       int64 `gorm:"column:role_id;primaryKey"`
	PermissionID int64 `gorm:"column:permission_id;primaryKey"`
}

func (RolePermissionModel) TableName() string { return "admin_role_permission" }

type UserRoleModel struct {
	UserID    int64     `gorm:"column:user_id;primaryKey"`
	RoleID    int64     `gorm:"column:role_id;primaryKey"`
	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime"`
}

func (UserRoleModel) TableName() string { return "admin_user_role" }

type AuditLogModel struct {
	ID         int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	ActorID    int64     `gorm:"column:actor_id;not null;index:idx_audit_actor_time,priority:1" json:"actor_id"`
	Permission string    `gorm:"column:permission;size:64;not null" json:"permission"`
	Action     string    `gorm:"column:action;size:64;not null" json:"action"`
	TargetType string    `gorm:"column:target_type;size:16;not null" json:"target_type"`
	TargetID   int64     `gorm:"column:target_id;not null" json:"target_id"`
	Reason     string    `gorm:"column:reason;size:512;not null" json:"reason"`
	BeforeJSON string    `gorm:"column:before_json;type:text" json:"before_json"`
	AfterJSON  string    `gorm:"column:after_json;type:text" json:"after_json"`
	RequestID  string    `gorm:"column:request_id;size:128" json:"request_id"`
	IP         string    `gorm:"column:ip;size:64" json:"ip"`
	CreatedAt  time.Time `gorm:"column:created_at;autoCreateTime;index:idx_audit_actor_time,priority:2" json:"created_at"`
}

func (AuditLogModel) TableName() string { return "management_audit_log" }

type MediaAssetModel struct {
	ID        int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	OwnerID   int64     `gorm:"column:owner_id;not null;index" json:"owner_id"`
	Kind      string    `gorm:"column:kind;size:16;not null" json:"kind"`
	URL       string    `gorm:"column:url;size:512;not null;uniqueIndex" json:"url"`
	Path      string    `gorm:"column:path;size:1024;not null" json:"-"`
	Status    string    `gorm:"column:status;size:16;not null;index" json:"status"`
	VideoID   int64     `gorm:"column:video_id;index" json:"video_id"`
	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (MediaAssetModel) TableName() string { return "media_asset" }
