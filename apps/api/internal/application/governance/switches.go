package applicationgovernance

import "sync/atomic"

const SwitchRecommendationDegraded = "recommendation_degraded"

type Switches struct {
	recommendationDegraded atomic.Bool
}

func NewSwitches() *Switches { return &Switches{} }

func (s *Switches) Enabled(key string) (bool, bool) {
	if s == nil || key != SwitchRecommendationDegraded {
		return false, false
	}
	return s.recommendationDegraded.Load(), true
}

func (s *Switches) Set(key string, enabled bool) bool {
	if s == nil || key != SwitchRecommendationDegraded {
		return false
	}
	s.recommendationDegraded.Store(enabled)
	return true
}

func (s *Switches) Snapshot() map[string]bool {
	enabled, _ := s.Enabled(SwitchRecommendationDegraded)
	return map[string]bool{SwitchRecommendationDegraded: enabled}
}
