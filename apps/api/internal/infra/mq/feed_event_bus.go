package inframq

import (
	applicationeventbus "FluxFeed/internal/application/eventbus"
	applicationexposure "FluxFeed/internal/application/exposure"
	applicationinteraction "FluxFeed/internal/application/interaction"
	applicationrelation "FluxFeed/internal/application/relation"
	applicationvideo "FluxFeed/internal/application/video"
	domainexposure "FluxFeed/internal/domain/exposure"
	domaininteraction "FluxFeed/internal/domain/interaction"
	infraconfig "FluxFeed/internal/infra/config"
	"context"
)

// FeedEventBus 把现有强类型应用事件适配到统一 Event 信封。
type FeedEventBus struct {
	bus    applicationeventbus.Bus
	config infraconfig.KafkaConfig
}

func NewFeedEventBus(bus applicationeventbus.Bus, cfg infraconfig.KafkaConfig) *FeedEventBus {
	return &FeedEventBus{bus: bus, config: normalizeKafkaConfig(cfg)}
}

func (b *FeedEventBus) Close() error { return b.bus.Close() }

func (b *FeedEventBus) Publish(ctx context.Context, topic string, event *applicationeventbus.Event) error {
	return b.bus.Publish(ctx, topic, event)
}

func (b *FeedEventBus) Subscribe(ctx context.Context, topic string, group string, handler func(context.Context, *applicationeventbus.Event) error) error {
	return b.bus.Subscribe(ctx, topic, group, handler)
}

func (b *FeedEventBus) PublishVideoPublished(ctx context.Context, source *applicationvideo.PublishedEvent) error {
	if source == nil {
		return nil
	}
	event, err := applicationeventbus.New(source.EventID, applicationeventbus.TypeVideoPublished, source.AuthorID, source.VideoID, source.OccurredAt, source)
	if err != nil {
		return err
	}
	return b.bus.Publish(ctx, b.config.VideoTopic, event)
}

func (b *FeedEventBus) PublishActionChanged(ctx context.Context, source *applicationinteraction.ActionChangedEvent) error {
	if source == nil {
		return nil
	}
	event, err := applicationeventbus.New(source.EventID, actionEventType(source), source.UserID, source.VideoID, source.OccurredAt, source)
	if err != nil {
		return err
	}
	return b.bus.Publish(ctx, b.config.InteractionTopic, event)
}

func (b *FeedEventBus) PublishViewEventRecorded(ctx context.Context, source *applicationexposure.ViewEventRecordedEvent) error {
	if source == nil {
		return nil
	}
	eventType := applicationeventbus.TypeVideoViewed
	if source.EventType == domainexposure.EventTypeExposed {
		eventType = applicationeventbus.TypeVideoExposed
	}
	event, err := applicationeventbus.New(source.EventID, eventType, source.UserID, source.VideoID, source.OccurredAt, source)
	if err != nil {
		return err
	}
	return b.bus.Publish(ctx, b.config.ExposureTopic, event)
}

func (b *FeedEventBus) PublishVideoCommented(ctx context.Context, source *applicationinteraction.CommentedEvent) error {
	if source == nil {
		return nil
	}
	event, err := applicationeventbus.New(source.EventID, applicationeventbus.TypeVideoCommented, source.UserID, source.VideoID, source.OccurredAt, source)
	if err != nil {
		return err
	}
	return b.bus.Publish(ctx, b.config.InteractionTopic, event)
}

func (b *FeedEventBus) PublishFollowChanged(ctx context.Context, source *applicationrelation.FollowChangedEvent) error {
	if source == nil {
		return nil
	}
	eventType := applicationeventbus.TypeUserUnfollowed
	if source.Active {
		eventType = applicationeventbus.TypeUserFollowed
	}
	event, err := applicationeventbus.New(source.EventID, eventType, source.UserID, 0, source.OccurredAt, source)
	if err != nil {
		return err
	}
	return b.bus.Publish(ctx, b.config.RelationTopic, event)
}

func (b *FeedEventBus) ConsumeVideoPublished(ctx context.Context, handler func(context.Context, *applicationvideo.PublishedEvent) error) error {
	return b.consumeVideoPublished(ctx, b.config.FanoutGroup, handler)
}

func (b *FeedEventBus) ConsumeVideoPublishedForEmbedding(ctx context.Context, handler func(context.Context, *applicationvideo.PublishedEvent) error) error {
	return b.consumeVideoPublished(ctx, b.config.EmbeddingGroup, handler)
}

func (b *FeedEventBus) consumeVideoPublished(ctx context.Context, group string, handler func(context.Context, *applicationvideo.PublishedEvent) error) error {
	return b.bus.Subscribe(ctx, b.config.VideoTopic, group, func(ctx context.Context, event *applicationeventbus.Event) error {
		if event.Type != applicationeventbus.TypeVideoPublished {
			return nil
		}
		var payload applicationvideo.PublishedEvent
		if err := event.Decode(&payload); err != nil {
			return err
		}
		return handler(ctx, &payload)
	})
}

func (b *FeedEventBus) ConsumeActionChanged(ctx context.Context, handler func(context.Context, *applicationinteraction.ActionChangedEvent) error) error {
	return b.bus.Subscribe(ctx, b.config.InteractionTopic, b.config.CounterGroup, func(ctx context.Context, event *applicationeventbus.Event) error {
		if !isActionEventType(event.Type) {
			return nil
		}
		var payload applicationinteraction.ActionChangedEvent
		if err := event.Decode(&payload); err != nil {
			return err
		}
		return handler(ctx, &payload)
	})
}

func (b *FeedEventBus) ConsumeViewEventRecorded(ctx context.Context, handler func(context.Context, *applicationexposure.ViewEventRecordedEvent) error) error {
	return b.consumeViewEventRecorded(ctx, b.config.FeatureGroup, handler)
}

func (b *FeedEventBus) ConsumeViewEventRecordedForRecommendation(ctx context.Context, handler func(context.Context, *applicationexposure.ViewEventRecordedEvent) error) error {
	return b.consumeViewEventRecorded(ctx, b.config.RecommendationGroup, handler)
}

func (b *FeedEventBus) consumeViewEventRecorded(ctx context.Context, group string, handler func(context.Context, *applicationexposure.ViewEventRecordedEvent) error) error {
	return b.bus.Subscribe(ctx, b.config.ExposureTopic, group, func(ctx context.Context, event *applicationeventbus.Event) error {
		if event.Type != applicationeventbus.TypeVideoExposed && event.Type != applicationeventbus.TypeVideoViewed {
			return nil
		}
		var payload applicationexposure.ViewEventRecordedEvent
		if err := event.Decode(&payload); err != nil {
			return err
		}
		return handler(ctx, &payload)
	})
}

func isActionEventType(eventType string) bool {
	return eventType == applicationeventbus.TypeVideoLiked ||
		eventType == applicationeventbus.TypeVideoUnliked ||
		eventType == applicationeventbus.TypeVideoCollected ||
		eventType == applicationeventbus.TypeVideoUncollected
}

func actionEventType(event *applicationinteraction.ActionChangedEvent) string {
	if event.ActionType == domaininteraction.ActionTypeLike {
		if event.Active {
			return applicationeventbus.TypeVideoLiked
		}
		return applicationeventbus.TypeVideoUnliked
	}
	if event.Active {
		return applicationeventbus.TypeVideoCollected
	}
	return applicationeventbus.TypeVideoUncollected
}
