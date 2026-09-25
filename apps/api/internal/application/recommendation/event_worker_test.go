package applicationrecommendation

import (
	applicationexposure "FluxFeed/internal/application/exposure"
	domainexposure "FluxFeed/internal/domain/exposure"
	"context"
	"testing"
)

type seenRecorderStub struct{ calls int }

func (s *seenRecorderStub) MarkSeen(context.Context, int64, int64) error {
	s.calls++
	return nil
}

func TestEventWorkerMarksOnlyExposureAsSeen(t *testing.T) {
	seen := &seenRecorderStub{}
	worker := NewEventWorker(seen, nil)
	for _, eventType := range []string{domainexposure.EventTypePlay, domainexposure.EventTypeExposed} {
		if err := worker.HandleViewEventRecorded(context.Background(), &applicationexposure.ViewEventRecordedEvent{UserID: 42, VideoID: 1001, EventType: eventType}); err != nil {
			t.Fatal(err)
		}
	}
	if seen.calls != 1 {
		t.Fatalf("unexpected MarkSeen calls: %d", seen.calls)
	}
}
