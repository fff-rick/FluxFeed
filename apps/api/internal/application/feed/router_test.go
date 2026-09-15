package applicationfeed

import (
	domainfeed "FluxFeed/internal/domain/feed"
	"context"
	"testing"
)

type stubStrategy struct{ scene domainfeed.Scene }

func (s stubStrategy) Scene() domainfeed.Scene { return s.scene }
func (s stubStrategy) List(context.Context, FeedRequest) (*FeedResult, error) {
	return &FeedResult{Scene: s.scene}, nil
}

func TestFeedRouterRegisterAndRoute(t *testing.T) {
	r := NewFeedRouter()
	r.Register(stubStrategy{scene: domainfeed.SceneHot})
	strategy, ok := r.Route(domainfeed.SceneHot)
	if !ok || strategy.Scene() != domainfeed.SceneHot {
		t.Fatal("hot strategy should be routable")
	}
}

func TestDeduplicateFilter(t *testing.T) {
	items := []*domainfeed.FeedPageItem{{VideoID: 1}, {VideoID: 1}, {VideoID: 2}, nil}
	got := (DeduplicateFilter{}).Filter(context.Background(), FeedRequest{}, items)
	if len(got) != 2 || got[0].VideoID != 1 || got[1].VideoID != 2 {
		t.Fatalf("unexpected candidates: %#v", got)
	}
}
