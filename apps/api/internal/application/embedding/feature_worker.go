package applicationembedding

import (
	applicationexposure "FluxFeed/internal/application/exposure"
	domainexposure "FluxFeed/internal/domain/exposure"
	inframetrics "FluxFeed/internal/infra/metrics"
	"context"
	"time"
)

type UserFeatureRefresher interface {
	RefreshUserInterestVector(ctx context.Context, userID int64) error
}

type ViewEventConsumer interface {
	ConsumeViewEventRecorded(ctx context.Context, handler func(context.Context, *applicationexposure.ViewEventRecordedEvent) error) error
}

type UserFeatureWorker struct {
	refresher UserFeatureRefresher
	consumer  ViewEventConsumer
}

func NewUserFeatureWorker(refresher UserFeatureRefresher, consumer ViewEventConsumer) *UserFeatureWorker {
	return &UserFeatureWorker{refresher: refresher, consumer: consumer}
}

func (w *UserFeatureWorker) Start(ctx context.Context) error {
	if w == nil || w.consumer == nil {
		return nil
	}
	return w.consumer.ConsumeViewEventRecorded(ctx, w.HandleViewEventRecorded)
}

func (w *UserFeatureWorker) HandleViewEventRecorded(ctx context.Context, event *applicationexposure.ViewEventRecordedEvent) (err error) {
	start := time.Now()
	defer func() { inframetrics.ObserveWorkerJob("user_feature", time.Since(start), err) }()
	if w == nil || w.refresher == nil || event == nil || event.UserID <= 0 {
		return nil
	}
	if event.EventType != domainexposure.EventTypePlay && event.EventType != domainexposure.EventTypeComplete {
		return nil
	}
	return w.refresher.RefreshUserInterestVector(ctx, event.UserID)
}
