package applicationeventbus

import (
	"errors"
	"testing"
	"time"
)

func TestEventEnvelopeRoundTrip(t *testing.T) {
	type payload struct {
		Title string `json:"title"`
	}
	at := time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC)
	event, err := New("evt-1", TypeVideoPublished, 42, 1001, at, payload{Title: "hello"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if event.Timestamp != at.UnixMilli() || event.UserID != 42 || event.VideoID != 1001 {
		t.Fatalf("unexpected event: %+v", event)
	}
	var decoded payload
	if err := event.Decode(&decoded); err != nil || decoded.Title != "hello" {
		t.Fatalf("unexpected payload: %+v, %v", decoded, err)
	}
}

func TestEventEnvelopeRejectsMissingIdentity(t *testing.T) {
	_, err := New("", TypeVideoPublished, 0, 0, time.Time{}, struct{}{})
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("unexpected error: %v", err)
	}
}
