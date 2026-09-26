package applicationembedding

import (
	applicationexposure "FluxFeed/internal/application/exposure"
	domainexposure "FluxFeed/internal/domain/exposure"
	"context"
	"testing"
)

type featureRefresherStub struct{ userIDs []int64 }

func (s *featureRefresherStub) RefreshUserInterestVector(_ context.Context, userID int64) error {
	s.userIDs = append(s.userIDs, userID)
	return nil
}

func TestUserFeatureWorkerRefreshesInterestSignals(t *testing.T) {
	refresher := &featureRefresherStub{}
	worker := NewUserFeatureWorker(refresher, nil)
	for _, eventType := range []string{
		domainexposure.EventTypeExposed,
		domainexposure.EventTypeClick,
		domainexposure.EventTypePlay,
		domainexposure.EventTypeValidPlay,
		domainexposure.EventTypeFinish,
		domainexposure.EventTypeSkip,
		domainexposure.EventTypeNotInterested,
		domainexposure.EventTypeHideAuthor,
	} {
		if err := worker.HandleViewEventRecorded(context.Background(), &applicationexposure.ViewEventRecordedEvent{UserID: 42, EventType: eventType}); err != nil {
			t.Fatal(err)
		}
	}
	if len(refresher.userIDs) != 7 || refresher.userIDs[0] != 42 {
		t.Fatalf("unexpected refreshed users: %v", refresher.userIDs)
	}
}
