package applicationinteraction

import (
	domaininteraction "FluxFeed/internal/domain/interaction"
	"time"
)

type CommentedEvent struct {
	EventID        string    `json:"event_id"`
	CommentID      int64     `json:"comment_id"`
	UserID         int64     `json:"user_id"`
	VideoID        int64     `json:"video_id"`
	Content        string    `json:"content"`
	CommentCount   int       `json:"comment_count"`
	IdempotencyKey string    `json:"idempotency_key"`
	OccurredAt     time.Time `json:"occurred_at"`
}

func NewCommentedEvent(comment *domaininteraction.Comment, commentCount int) *CommentedEvent {
	if comment == nil {
		return nil
	}
	return &CommentedEvent{
		EventID:        newEventID(),
		CommentID:      comment.ID,
		UserID:         comment.UserID,
		VideoID:        comment.VideoID,
		Content:        comment.Content,
		CommentCount:   commentCount,
		IdempotencyKey: comment.IdempotencyKey,
		OccurredAt:     comment.CreatedAt.UTC(),
	}
}
