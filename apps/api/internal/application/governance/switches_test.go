package applicationgovernance

import "testing"

func TestSwitchesRejectUnknownKeys(t *testing.T) {
	switches := NewSwitches()
	if !switches.Set(SwitchRecommendationDegraded, true) {
		t.Fatal("expected known switch to update")
	}
	if enabled, ok := switches.Enabled(SwitchRecommendationDegraded); !ok || !enabled {
		t.Fatal("expected recommendation degradation to be enabled")
	}
	if switches.Set("unknown", true) {
		t.Fatal("unknown switch should be rejected")
	}
}
