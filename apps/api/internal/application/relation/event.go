package applicationrelation

import (
	domainrelation "FluxFeed/internal/domain/relation"
	"fmt"
	"time"
)

type FollowChangedEvent struct {
	EventID        string    `json:"event_id"`
	UserID         int64     `json:"user_id"`
	TargetUserID   int64     `json:"target_user_id"`
	Active         bool      `json:"active"`
	IdempotencyKey string    `json:"idempotency_key"`
	OccurredAt     time.Time `json:"occurred_at"`
}

func NewFollowChangedEvent(follow *domainrelation.Follow) *FollowChangedEvent {
	if follow == nil {
		return nil
	}
	return &FollowChangedEvent{
		EventID:        fmt.Sprintf("relation:%d:%d:%d:%d", follow.UserID, follow.TargetUserID, follow.Status, follow.UpdatedAt.UnixNano()),
		UserID:         follow.UserID,
		TargetUserID:   follow.TargetUserID,
		Active:         follow.Active(),
		IdempotencyKey: follow.IdempotencyKey,
		OccurredAt:     follow.UpdatedAt.UTC(),
	}
}
