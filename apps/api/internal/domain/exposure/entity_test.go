package domainexposure

import "testing"

func TestViewEventBehaviorSemantics(t *testing.T) {
	tests := []struct {
		eventType       string
		completed       bool
		marksSeen       bool
		affectsInterest bool
	}{
		{EventTypeExposed, false, true, false},
		{EventTypeClick, false, false, true},
		{EventTypeValidPlay, false, false, true},
		{EventTypeFinish, true, false, true},
		{EventTypeComplete, true, false, true},
		{EventTypeSkip, false, true, true},
		{EventTypeNotInterested, false, true, true},
		{EventTypeHideAuthor, false, true, true},
	}
	for _, test := range tests {
		event, err := NewViewEvent(1, 2, "recommend", "request-1", test.eventType, 0, false)
		if err != nil {
			t.Fatalf("new %s event: %v", test.eventType, err)
		}
		if event.Completed != test.completed || event.MarksSeen() != test.marksSeen || AffectsInterestProfile(test.eventType) != test.affectsInterest {
			t.Fatalf("unexpected %s semantics: %+v", test.eventType, event)
		}
		if IsNegativeFeedback(test.eventType) != (test.eventType == EventTypeSkip || test.eventType == EventTypeNotInterested || test.eventType == EventTypeHideAuthor) {
			t.Fatalf("unexpected %s negative feedback semantics", test.eventType)
		}
	}
}
