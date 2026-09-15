package applicationfeed

import (
	domainfeed "FluxFeed/internal/domain/feed"
	"context"
)

// CandidateFilter 是候选过滤扩展点。后续可挂接已曝光、黑名单、内容状态等过滤器。
type CandidateFilter interface {
	Filter(ctx context.Context, req FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem
}

// CandidateRanker 是候选排序扩展点。不同 scene 可以注入不同排序器。
type CandidateRanker interface {
	Rank(ctx context.Context, req FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem
}

// DeduplicateFilter 在进入卡片组装前按 video_id 去重，避免多路候选合并后的重复曝光。
type DeduplicateFilter struct{}

func (DeduplicateFilter) Filter(_ context.Context, _ FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem {
	result := make([]*domainfeed.FeedPageItem, 0, len(items))
	seen := make(map[int64]struct{}, len(items))
	for _, item := range items {
		if item == nil || item.VideoID <= 0 {
			continue
		}
		if _, ok := seen[item.VideoID]; ok {
			continue
		}
		seen[item.VideoID] = struct{}{}
		result = append(result, item)
	}
	return result
}

// PreserveOrderRanker 保留上游排序。Timeline/Hot/Recommend 已在数据源阶段确定顺序。
type PreserveOrderRanker struct{}

func (PreserveOrderRanker) Rank(_ context.Context, _ FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem {
	return items
}

func prepareCandidates(ctx context.Context, req FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem {
	items = (DeduplicateFilter{}).Filter(ctx, req, items)
	return (PreserveOrderRanker{}).Rank(ctx, req, items)
}

// FeedAssembler 将轻量候选批量水合成客户端 FeedItem。
// 卡片、计数和 viewer action 的读取集中在这里，Strategy 不再关心数据拼装细节。
type FeedAssembler struct {
	repo  domainfeed.Repository
	cache FeedCache
}

func NewFeedAssembler(repo domainfeed.Repository, cache FeedCache) *FeedAssembler {
	return &FeedAssembler{repo: repo, cache: cache}
}
func (a *FeedAssembler) Assemble(ctx context.Context, req FeedRequest, items []*domainfeed.FeedPageItem) ([]*domainfeed.FeedItem, error) {
	return assembleFeedItems(ctx, a.repo, a.cache, prepareCandidates(ctx, req, items), req.ViewerID)
}
