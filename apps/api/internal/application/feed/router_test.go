package applicationfeed

import (
	domainfeed "FluxFeed/internal/domain/feed"
	"context"
	"testing"
	"time"

	"golang.org/x/sync/singleflight"
)

type stubStrategy struct{ scene domainfeed.Scene }

type stubSeenCache map[int64]struct{}

type staleFeedCache struct {
	page      *FeedPage
	refreshed chan struct{}
}

func (c stubSeenCache) SeenVideoIDs(context.Context, int64, []int64) (map[int64]struct{}, error) {
	return c, nil
}

func (c *staleFeedCache) GetPage(context.Context, string) (*FeedPage, bool, error) {
	return c.page, true, nil
}
func (c *staleFeedCache) GetPageState(context.Context, string) (*FeedPage, bool, bool, error) {
	return c.page, true, true, nil
}
func (c *staleFeedCache) SetPage(_ context.Context, _ string, page *FeedPage, _ time.Duration) error {
	c.page = page
	close(c.refreshed)
	return nil
}
func (c *staleFeedCache) GetCards(context.Context, []int64) (map[int64]*domainfeed.FeedCard, error) {
	return nil, nil
}
func (c *staleFeedCache) SetCards(context.Context, map[int64]*domainfeed.FeedCard, time.Duration) error {
	return nil
}
func (c *staleFeedCache) GetStats(context.Context, []int64) (map[int64]*domainfeed.FeedStat, error) {
	return nil, nil
}
func (c *staleFeedCache) SetStats(context.Context, map[int64]*domainfeed.FeedStat, time.Duration) error {
	return nil
}
func (c *staleFeedCache) ListHotWindowPage(context.Context, time.Time, int, int) ([]*domainfeed.FeedPageItem, error) {
	return nil, nil
}

func (s stubStrategy) Scene() domainfeed.Scene { return s.scene }
func (s stubStrategy) List(context.Context, FeedRequest) (*FeedResult, error) {
	return &FeedResult{Scene: s.scene}, nil
}

func TestMissingCardMarkerPreventsReload(t *testing.T) {
	cards := map[int64]*domainfeed.FeedCard{1001: nil}
	missing := missingCardIDs([]int64{1001, 1002}, cards)
	if len(missing) != 1 || missing[0] != 1002 {
		t.Fatalf("unexpected missing card ids: %v", missing)
	}
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

func TestSeenFilter(t *testing.T) {
	items := []*domainfeed.FeedPageItem{{VideoID: 1}, {VideoID: 2}, {VideoID: 3}}
	got := (SeenFilter{Cache: stubSeenCache{2: {}}}).Filter(context.Background(), FeedRequest{ViewerID: 42}, items)
	if len(got) != 2 || got[0].VideoID != 1 || got[1].VideoID != 3 {
		t.Fatalf("unexpected unseen candidates: %#v", got)
	}
}

func TestStalePageReturnsImmediatelyAndRefreshes(t *testing.T) {
	oldPage := &FeedPage{Scene: domainfeed.SceneTimeline, Items: []*domainfeed.FeedPageItem{{VideoID: 1}}}
	newPage := &FeedPage{Scene: domainfeed.SceneTimeline, Items: []*domainfeed.FeedPageItem{{VideoID: 2}}}
	cache := &staleFeedCache{page: oldPage, refreshed: make(chan struct{})}
	started := make(chan struct{})
	release := make(chan struct{})
	page, err := loadFeedPage(context.Background(), cache, domainfeed.SceneTimeline, "", 10, time.Second, time.Minute, &singleflight.Group{}, func(context.Context) (*FeedPage, error) {
		close(started)
		<-release
		return newPage, nil
	})
	if err != nil || page.Items[0].VideoID != 1 {
		t.Fatalf("expected stale page, got %+v, %v", page, err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not start")
	}
	close(release)
	select {
	case <-cache.refreshed:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not fill cache")
	}
	if cache.page.Items[0].VideoID != 2 {
		t.Fatalf("unexpected refreshed page: %+v", cache.page)
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
