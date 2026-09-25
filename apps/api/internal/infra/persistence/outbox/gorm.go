package infraoutbox

import (
	applicationeventbus "FluxFeed/internal/application/eventbus"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

var ErrInvalidStatus = errors.New("invalid outbox status")

type Status struct {
	Pending         int64
	Failed          int64
	OldestPendingAt *time.Time
}

type EventRecord struct {
	EventID       string     `json:"event_id"`
	Topic         string     `json:"topic"`
	Status        string     `json:"status"`
	Attempts      int        `json:"attempts"`
	NextAttemptAt time.Time  `json:"next_attempt_at"`
	PublishedAt   *time.Time `json:"published_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

const (
	statusPending   = "pending"
	statusPublished = "published"
	statusFailed    = "failed"
)

type Repository struct{ db *gorm.DB }

func New(db *gorm.DB) *Repository { return &Repository{db: db} }

func (r *Repository) Add(tx *gorm.DB, topic string, event *applicationeventbus.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return tx.Create(&EventModel{
		EventID: event.ID, Topic: topic, Payload: payload, Status: statusPending, NextAttemptAt: time.Now().UTC(),
	}).Error
}

func (r *Repository) FetchPending(ctx context.Context, limit int) ([]*applicationeventbus.OutboxMessage, error) {
	var models []EventModel
	err := r.db.WithContext(ctx).Where("status = ? AND next_attempt_at <= ?", statusPending, time.Now().UTC()).
		Order("created_at ASC").Limit(limit).Find(&models).Error
	if err != nil {
		return nil, err
	}
	messages := make([]*applicationeventbus.OutboxMessage, 0, len(models))
	for _, model := range models {
		var event applicationeventbus.Event
		if err := json.Unmarshal(model.Payload, &event); err != nil {
			return nil, err
		}
		messages = append(messages, &applicationeventbus.OutboxMessage{Event: &event, Topic: model.Topic, Attempts: model.Attempts})
	}
	return messages, nil
}

func (r *Repository) MarkPublished(ctx context.Context, eventID string) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Model(&EventModel{}).Where("event_id = ?", eventID).Updates(map[string]any{
		"status": statusPublished, "published_at": now, "last_error": "",
	}).Error
}

func (r *Repository) MarkFailed(ctx context.Context, eventID string, cause string, nextAttemptAt time.Time, terminal bool) error {
	status := statusPending
	if terminal {
		status = statusFailed
	}
	return r.db.WithContext(ctx).Model(&EventModel{}).Where("event_id = ?", eventID).Updates(map[string]any{
		"status": status, "attempts": gorm.Expr("attempts + 1"), "next_attempt_at": nextAttemptAt.UTC(), "last_error": cause,
	}).Error
}

func (r *Repository) Status(ctx context.Context) (*Status, error) {
	status := &Status{}
	if err := r.db.WithContext(ctx).Model(&EventModel{}).Where("status = ?", statusPending).Count(&status.Pending).Error; err != nil {
		return nil, err
	}
	if err := r.db.WithContext(ctx).Model(&EventModel{}).Where("status = ?", statusFailed).Count(&status.Failed).Error; err != nil {
		return nil, err
	}
	if status.Pending > 0 {
		var model EventModel
		if err := r.db.WithContext(ctx).Select("created_at").Where("status = ?", statusPending).Order("created_at ASC").Take(&model).Error; err != nil {
			return nil, err
		}
		status.OldestPendingAt = &model.CreatedAt
	}
	return status, nil
}

func (r *Repository) ListEvents(ctx context.Context, status string, limit int) ([]*EventRecord, error) {
	status = strings.TrimSpace(strings.ToLower(status))
	if status != statusPending && status != statusFailed {
		return nil, ErrInvalidStatus
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	var models []EventModel
	if err := r.db.WithContext(ctx).Where("status = ?", status).Order("created_at ASC").Limit(limit).Find(&models).Error; err != nil {
		return nil, err
	}
	records := make([]*EventRecord, 0, len(models))
	for _, model := range models {
		records = append(records, &EventRecord{
			EventID: model.EventID, Topic: model.Topic, Status: model.Status, Attempts: model.Attempts,
			NextAttemptAt: model.NextAttemptAt, PublishedAt: model.PublishedAt, LastError: model.LastError,
			CreatedAt: model.CreatedAt, UpdatedAt: model.UpdatedAt,
		})
	}
	return records, nil
}
