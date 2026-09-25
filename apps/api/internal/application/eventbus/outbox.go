package applicationeventbus

import (
	"context"
	"time"
)

type OutboxMessage struct {
	Event    *Event
	Topic    string
	Attempts int
}

type OutboxStore interface {
	FetchPending(ctx context.Context, limit int) ([]*OutboxMessage, error)
	MarkPublished(ctx context.Context, eventID string) error
	MarkFailed(ctx context.Context, eventID string, cause string, nextAttemptAt time.Time, terminal bool) error
}

type OutboxWorker struct {
	store       OutboxStore
	publisher   Bus
	interval    time.Duration
	maxAttempts int
}

func NewOutboxWorker(store OutboxStore, publisher Bus) *OutboxWorker {
	return &OutboxWorker{store: store, publisher: publisher, interval: 500 * time.Millisecond, maxAttempts: 10}
}

func (w *OutboxWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		_ = w.DispatchOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *OutboxWorker) DispatchOnce(ctx context.Context) error {
	messages, err := w.store.FetchPending(ctx, 100)
	if err != nil {
		return err
	}
	for _, message := range messages {
		if err := w.publisher.Publish(ctx, message.Topic, message.Event); err != nil {
			attempts := message.Attempts + 1
			delay := time.Second << min(attempts-1, 8)
			if markErr := w.store.MarkFailed(ctx, message.Event.ID, err.Error(), time.Now().Add(delay), attempts >= w.maxAttempts); markErr != nil {
				return markErr
			}
			continue
		}
		if err := w.store.MarkPublished(ctx, message.Event.ID); err != nil {
			return err
		}
	}
	return nil
}
