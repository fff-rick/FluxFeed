package applicationrecommendation

import (
	applicationexposure "FluxFeed/internal/application/exposure"
	domainexposure "FluxFeed/internal/domain/exposure"
	inframetrics "FluxFeed/internal/infra/metrics"
	"context"
	"time"
)

type SeenRecorder interface {
	MarkSeen(ctx context.Context, userID int64, videoID int64) error
}

type ViewEventConsumer interface {
	ConsumeViewEventRecordedForRecommendation(ctx context.Context, handler func(context.Context, *applicationexposure.ViewEventRecordedEvent) error) error
}

type EventWorker struct {
	seen     SeenRecorder
	consumer ViewEventConsumer
}

func NewEventWorker(seen SeenRecorder, consumer ViewEventConsumer) *EventWorker {
	return &EventWorker{seen: seen, consumer: consumer}
}

func (w *EventWorker) Start(ctx context.Context) error {
	if w == nil || w.consumer == nil {
		return nil
	}
	return w.consumer.ConsumeViewEventRecordedForRecommendation(ctx, w.HandleViewEventRecorded)
}

func (w *EventWorker) HandleViewEventRecorded(ctx context.Context, event *applicationexposure.ViewEventRecordedEvent) (err error) {
	start := time.Now()
	defer func() { inframetrics.ObserveWorkerJob("recommendation_feedback", time.Since(start), err) }()
	if w == nil || w.seen == nil || event == nil || event.EventType != domainexposure.EventTypeExposed {
		return nil
	}
	return w.seen.MarkSeen(ctx, event.UserID, event.VideoID)
}
