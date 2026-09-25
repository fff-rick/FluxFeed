package applicationeventbus

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const (
	TypeVideoPublished   = "VideoPublished"
	TypeVideoExposed     = "VideoExposed"
	TypeVideoViewed      = "VideoViewed"
	TypeVideoLiked       = "VideoLiked"
	TypeVideoUnliked     = "VideoUnliked"
	TypeVideoCollected   = "VideoCollected"
	TypeVideoUncollected = "VideoUncollected"
	TypeVideoCommented   = "VideoCommented"
	TypeUserFollowed     = "UserFollowed"
	TypeUserUnfollowed   = "UserUnfollowed"
)

var ErrInvalidEvent = errors.New("invalid event")

// Event 是跨消息中间件的统一信封，Payload 保留各业务事件的完整字段。
type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	UserID    int64           `json:"user_id,omitempty"`
	VideoID   int64           `json:"video_id,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

type Bus interface {
	Publish(ctx context.Context, topic string, event *Event) error
	Subscribe(ctx context.Context, topic string, group string, handler func(context.Context, *Event) error) error
	Close() error
}

// ConsumerDeduplicator 记录消费组已经成功处理的事件，避免 Kafka 重投时重复执行业务逻辑。
type ConsumerDeduplicator interface {
	IsProcessed(ctx context.Context, group string, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, group string, eventID string) error
}

func New(id string, eventType string, userID int64, videoID int64, occurredAt time.Time, payload any) (*Event, error) {
	id = strings.TrimSpace(id)
	eventType = strings.TrimSpace(eventType)
	if id == "" || eventType == "" {
		return nil, ErrInvalidEvent
	}
	content, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if occurredAt.IsZero() {
		occurredAt = time.Now()
	}
	return &Event{
		ID:        id,
		Type:      eventType,
		UserID:    userID,
		VideoID:   videoID,
		Timestamp: occurredAt.UTC().UnixMilli(),
		Payload:   content,
	}, nil
}

func (e *Event) Decode(target any) error {
	if e == nil || len(e.Payload) == 0 || target == nil {
		return ErrInvalidEvent
	}
	return json.Unmarshal(e.Payload, target)
}
