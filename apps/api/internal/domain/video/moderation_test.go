package domainvideo

import (
	"testing"
	"time"
)

func TestPendingVideoStateTransitions(t *testing.T) {
	video, err := NewPendingReview(1, "title", "", "/video.mp4", "/cover.jpg", "key")
	if err != nil || video.Status != StatusPendingReview || video.PublishedAt != nil {
		t.Fatalf("unexpected pending video: %+v err=%v", video, err)
	}
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if !video.Publish(at) || video.Status != StatusPublished || video.PublishedAt == nil || !video.PublishedAt.Equal(at) {
		t.Fatalf("publish failed: %+v", video)
	}
	if video.Reject() {
		t.Fatal("published video must not be rejected directly")
	}
	if !video.Offline() || video.Status != StatusOffline {
		t.Fatalf("offline failed: %+v", video)
	}
	if !video.Publish(time.Now()) || !video.PublishedAt.Equal(at) {
		t.Fatal("restore must preserve original published_at")
	}
}
