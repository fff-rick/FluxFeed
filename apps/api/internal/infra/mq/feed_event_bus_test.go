package inframq

import (
	applicationeventbus "FluxFeed/internal/application/eventbus"
	applicationexposure "FluxFeed/internal/application/exposure"
	applicationinteraction "FluxFeed/internal/application/interaction"
	applicationrelation "FluxFeed/internal/application/relation"
	applicationvideo "FluxFeed/internal/application/video"
	domaininteraction "FluxFeed/internal/domain/interaction"
	infraconfig "FluxFeed/internal/infra/config"
	"context"
	"testing"
	"time"
)

type memoryBus struct {
	topic string
	group string
	event *applicationeventbus.Event
}

func (b *memoryBus) Publish(_ context.Context, topic string, event *applicationeventbus.Event) error {
	b.topic, b.event = topic, event
	return nil
}

func (b *memoryBus) Subscribe(ctx context.Context, topic string, group string, handler func(context.Context, *applicationeventbus.Event) error) error {
	b.topic, b.group = topic, group
	return handler(ctx, b.event)
}

func (*memoryBus) Close() error { return nil }

func TestFeedEventBusPublishesUnifiedVideoEvent(t *testing.T) {
	backend := &memoryBus{}
	bus := NewFeedEventBus(backend, infraconfig.KafkaConfig{})
	source := &applicationvideo.PublishedEvent{
		EventID: "evt-video", VideoID: 1001, AuthorID: 42,
		OccurredAt: time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC),
	}
	if err := bus.PublishVideoPublished(context.Background(), source); err != nil {
		t.Fatalf("PublishVideoPublished: %v", err)
	}
	if backend.topic != "fluxfeed.video" || backend.event.Type != applicationeventbus.TypeVideoPublished || backend.event.UserID != 42 {
		t.Fatalf("unexpected published event: %s %+v", backend.topic, backend.event)
	}

	var consumed *applicationvideo.PublishedEvent
	if err := bus.ConsumeVideoPublished(context.Background(), func(_ context.Context, event *applicationvideo.PublishedEvent) error {
		consumed = event
		return nil
	}); err != nil {
		t.Fatalf("ConsumeVideoPublished: %v", err)
	}
	if backend.group != "fluxfeed-fanout" || consumed == nil || consumed.EventID != source.EventID {
		t.Fatalf("unexpected consumed event: %s %+v", backend.group, consumed)
	}
}

func TestFeedEventBusMapsActionTypes(t *testing.T) {
	backend := &memoryBus{}
	bus := NewFeedEventBus(backend, infraconfig.KafkaConfig{})
	tests := []struct {
		actionType string
		active     bool
		want       string
	}{
		{domaininteraction.ActionTypeLike, true, applicationeventbus.TypeVideoLiked},
		{domaininteraction.ActionTypeLike, false, applicationeventbus.TypeVideoUnliked},
		{domaininteraction.ActionTypeFavorite, true, applicationeventbus.TypeVideoCollected},
		{domaininteraction.ActionTypeFavorite, false, applicationeventbus.TypeVideoUncollected},
	}
	for _, test := range tests {
		event := &applicationinteraction.ActionChangedEvent{
			EventID: "evt-action", UserID: 42, VideoID: 1001,
			ActionType: test.actionType, Active: test.active, OccurredAt: time.Now(),
		}
		if err := bus.PublishActionChanged(context.Background(), event); err != nil {
			t.Fatalf("PublishActionChanged: %v", err)
		}
		if backend.event.Type != test.want {
			t.Fatalf("action %s/%v: got %s, want %s", test.actionType, test.active, backend.event.Type, test.want)
		}
	}
}

func TestKafkaPartitionKeyPrefersVideo(t *testing.T) {
	event := &applicationeventbus.Event{ID: "evt", UserID: 42, VideoID: 1001}
	if got := eventPartitionKey(event); got != "1001" {
		t.Fatalf("unexpected partition key: %s", got)
	}
}

func TestFeedEventBusPublishesCommentAndFollowEvents(t *testing.T) {
	backend := &memoryBus{}
	bus := NewFeedEventBus(backend, infraconfig.KafkaConfig{})
	if err := bus.PublishVideoCommented(context.Background(), &applicationinteraction.CommentedEvent{
		EventID: "evt-comment", UserID: 42, VideoID: 1001, OccurredAt: time.Now(),
	}); err != nil {
		t.Fatalf("PublishVideoCommented: %v", err)
	}
	if backend.topic != "fluxfeed.interaction" || backend.event.Type != applicationeventbus.TypeVideoCommented {
		t.Fatalf("unexpected comment event: %s %+v", backend.topic, backend.event)
	}
	if err := bus.PublishFollowChanged(context.Background(), &applicationrelation.FollowChangedEvent{
		EventID: "evt-follow", UserID: 42, TargetUserID: 77, Active: true, OccurredAt: time.Now(),
	}); err != nil {
		t.Fatalf("PublishFollowChanged: %v", err)
	}
	if backend.topic != "fluxfeed.relation" || backend.event.Type != applicationeventbus.TypeUserFollowed {
		t.Fatalf("unexpected follow event: %s %+v", backend.topic, backend.event)
	}
}

func TestFeedEventBusUsesIndependentViewConsumerGroups(t *testing.T) {
	backend := &memoryBus{event: &applicationeventbus.Event{Type: applicationeventbus.TypeVideoExposed, Payload: []byte(`{"event_id":"evt-view"}`)}}
	bus := NewFeedEventBus(backend, infraconfig.KafkaConfig{})
	handler := func(context.Context, *applicationexposure.ViewEventRecordedEvent) error { return nil }
	if err := bus.ConsumeViewEventRecorded(context.Background(), handler); err != nil {
		t.Fatal(err)
	}
	if backend.group != "fluxfeed-feature" {
		t.Fatalf("unexpected feature group: %s", backend.group)
	}
	if err := bus.ConsumeViewEventRecordedForRecommendation(context.Background(), handler); err != nil {
		t.Fatal(err)
	}
	if backend.group != "fluxfeed-recommendation" {
		t.Fatalf("unexpected recommendation group: %s", backend.group)
	}
}
