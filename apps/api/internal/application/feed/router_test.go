package applicationfeed

import (
	domainfeed "FluxFeed/internal/domain/feed"
	"context"
	"testing"
	"time"
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

func TestCandidatePipelineMergeDedupRankMixTopN(t *testing.T) {
	base := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	pages := []*FeedPage{
		{Items: []*domainfeed.FeedPageItem{
			{VideoID: 1, AuthorID: 10, PublishedAt: base},
			{VideoID: 2, AuthorID: 10, PublishedAt: base.Add(2 * time.Hour)},
		}},
		{Items: []*domainfeed.FeedPageItem{
			{VideoID: 1, AuthorID: 10, PublishedAt: base},
			{VideoID: 3, AuthorID: 20, PublishedAt: base.Add(time.Hour)},
		}},
	}

	got := NewCandidatePipeline(LatestRanker{}, AuthorDiversityMixer{}).Run(context.Background(), FeedRequest{}, pages, 2)
	if len(got) != 2 || got[0].VideoID != 2 || got[1].VideoID != 3 {
		t.Fatalf("unexpected pipeline result: %#v", got)
	}
}

func TestHotRanker(t *testing.T) {
	items := []*domainfeed.FeedPageItem{{VideoID: 1, HotScore: 3}, {VideoID: 2, HotScore: 9}}
	got := (HotRanker{}).Rank(context.Background(), FeedRequest{}, items)
	if got[0].VideoID != 2 || got[1].VideoID != 1 {
		t.Fatalf("unexpected hot rank: %#v", got)
	}
}
