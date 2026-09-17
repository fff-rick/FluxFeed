package applicationfeed

import (
	domainfeed "FluxFeed/internal/domain/feed"
	"context"
	"sort"
)

type CandidateMerger interface {
	Merge(ctx context.Context, req FeedRequest, pages []*FeedPage) []*domainfeed.FeedPageItem
}

type CandidateFilter interface {
	Filter(ctx context.Context, req FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem
}

type CandidateRanker interface {
	Rank(ctx context.Context, req FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem
}

type CandidateMixer interface {
	Mix(ctx context.Context, req FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem
}

type AppendCandidateMerger struct{}

func (AppendCandidateMerger) Merge(_ context.Context, _ FeedRequest, pages []*FeedPage) []*domainfeed.FeedPageItem {
	var result []*domainfeed.FeedPageItem
	for _, page := range pages {
		if page != nil {
			result = append(result, page.Items...)
		}
	}
	return result
}

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

type PreserveOrderRanker struct{}

func (PreserveOrderRanker) Rank(_ context.Context, _ FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem {
	return items
}

type LatestRanker struct{}

func (LatestRanker) Rank(_ context.Context, _ FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem {
	result := append([]*domainfeed.FeedPageItem(nil), items...)
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].PublishedAt.Equal(result[j].PublishedAt) {
			return result[i].VideoID > result[j].VideoID
		}
		return result[i].PublishedAt.After(result[j].PublishedAt)
	})
	return result
}

type HotRanker struct{}

func (HotRanker) Rank(_ context.Context, _ FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem {
	result := append([]*domainfeed.FeedPageItem(nil), items...)
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].HotScore != result[j].HotScore {
			return result[i].HotScore > result[j].HotScore
		}
		if !result[i].PublishedAt.Equal(result[j].PublishedAt) {
			return result[i].PublishedAt.After(result[j].PublishedAt)
		}
		return result[i].VideoID > result[j].VideoID
	})
	return result
}

type AuthorDiversityMixer struct{}

func (AuthorDiversityMixer) Mix(_ context.Context, _ FeedRequest, items []*domainfeed.FeedPageItem) []*domainfeed.FeedPageItem {
	buckets := make(map[int64][]*domainfeed.FeedPageItem)
	authors := make([]int64, 0)
	for _, item := range items {
		if _, ok := buckets[item.AuthorID]; !ok {
			authors = append(authors, item.AuthorID)
		}
		buckets[item.AuthorID] = append(buckets[item.AuthorID], item)
	}
	result := make([]*domainfeed.FeedPageItem, 0, len(items))
	for len(result) < len(items) {
		for _, authorID := range authors {
			bucket := buckets[authorID]
			if len(bucket) == 0 {
				continue
			}
			result = append(result, bucket[0])
			buckets[authorID] = bucket[1:]
		}
	}
	return result
}

// CandidatePipeline 执行 Merge -> Filter -> Rank -> Mix -> Top-N。
type CandidatePipeline struct {
	Merger  CandidateMerger
	Filters []CandidateFilter
	Ranker  CandidateRanker
	Mixers  []CandidateMixer
}

func NewCandidatePipeline(ranker CandidateRanker, mixers ...CandidateMixer) *CandidatePipeline {
	return &CandidatePipeline{Merger: AppendCandidateMerger{}, Filters: []CandidateFilter{DeduplicateFilter{}}, Ranker: ranker, Mixers: mixers}
}

func (p *CandidatePipeline) Run(ctx context.Context, req FeedRequest, pages []*FeedPage, limit int) []*domainfeed.FeedPageItem {
	merger := p.Merger
	if merger == nil {
		merger = AppendCandidateMerger{}
	}
	items := merger.Merge(ctx, req, pages)
	for _, filter := range p.Filters {
		if filter != nil {
			items = filter.Filter(ctx, req, items)
		}
	}
	if p.Ranker != nil {
		items = p.Ranker.Rank(ctx, req, items)
	}
	for _, mixer := range p.Mixers {
		if mixer != nil {
			items = mixer.Mix(ctx, req, items)
		}
	}
	if limit >= 0 && len(items) > limit {
		items = items[:limit]
	}
	return items
}

func assemblePipelineResult(ctx context.Context, req FeedRequest, scene domainfeed.Scene, pages []*FeedPage, pipeline *CandidatePipeline, limit int, assembler *FeedAssembler) (*FeedResult, error) {
	candidates := pipeline.Run(ctx, req, pages, limit+1)
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	nextCursor := ""
	hasMore := false
	if len(pages) > 0 && pages[0] != nil {
		nextCursor = pages[0].NextCursor
		hasMore = pages[0].HasMore
	}
	items, err := assembler.Assemble(ctx, req, candidates)
	if err != nil {
		return nil, ErrLoadFeedFailed
	}
	return &FeedResult{Scene: scene, Items: items, NextCursor: nextCursor, HasMore: hasMore}, nil
}

type FeedAssembler struct {
	repo  domainfeed.Repository
	cache FeedCache
}

func NewFeedAssembler(repo domainfeed.Repository, cache FeedCache) *FeedAssembler {
	return &FeedAssembler{repo: repo, cache: cache}
}

func (a *FeedAssembler) Assemble(ctx context.Context, req FeedRequest, items []*domainfeed.FeedPageItem) ([]*domainfeed.FeedItem, error) {
	return assembleFeedItems(ctx, a.repo, a.cache, items, req.ViewerID)
}
