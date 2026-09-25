package applicationeventbus

import (
	"context"
	"errors"
	"testing"
	"time"
)

type memoryOutboxStore struct {
	messages  []*OutboxMessage
	published string
	failed    string
}

func (s *memoryOutboxStore) FetchPending(context.Context, int) ([]*OutboxMessage, error) {
	return s.messages, nil
}
func (s *memoryOutboxStore) MarkPublished(_ context.Context, id string) error {
	s.published = id
	return nil
}
func (s *memoryOutboxStore) MarkFailed(_ context.Context, id, _ string, _ time.Time, _ bool) error {
	s.failed = id
	return nil
}

type memoryOutboxBus struct{ err error }

func (b *memoryOutboxBus) Publish(context.Context, string, *Event) error { return b.err }
func (*memoryOutboxBus) Subscribe(context.Context, string, string, func(context.Context, *Event) error) error {
	return nil
}
func (*memoryOutboxBus) Close() error { return nil }

func TestOutboxWorkerMarksPublishResult(t *testing.T) {
	message := &OutboxMessage{Event: &Event{ID: "evt-1"}, Topic: "video"}
	store := &memoryOutboxStore{messages: []*OutboxMessage{message}}
	worker := NewOutboxWorker(store, &memoryOutboxBus{})
	if err := worker.DispatchOnce(context.Background()); err != nil || store.published != "evt-1" {
		t.Fatalf("published=%q err=%v", store.published, err)
	}

	store.published = ""
	worker.publisher = &memoryOutboxBus{err: errors.New("kafka unavailable")}
	if err := worker.DispatchOnce(context.Background()); err != nil || store.failed != "evt-1" {
		t.Fatalf("failed=%q err=%v", store.failed, err)
	}
}
