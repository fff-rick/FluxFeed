package applicationfeed

import (
	applicationrecommendation "FluxFeed/internal/application/recommendation"
	domainfeed "FluxFeed/internal/domain/feed"
	"context"
	"errors"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// CandidateSource 只负责“从哪里取候选”，不负责卡片组装和 HTTP 输出。
// Strategy 负责场景策略，CandidateSource 负责数据来源，两者解耦后可以独立扩展召回源。
type CandidateSource interface {
	Load(ctx context.Context, req FeedRequest, limit int) (*FeedPage, error)
}

// loadCandidatePages 并行执行多路召回，并按 sources 的注册顺序返回结果。
func loadCandidatePages(ctx context.Context, req FeedRequest, limit int, sources ...CandidateSource) ([]*FeedPage, error) {
	pages := make([]*FeedPage, len(sources))
	group, groupCtx := errgroup.WithContext(ctx)
	for index, source := range sources {
		if source == nil {
			continue
		}
		index, source := index, source
		group.Go(func() error {
			page, err := source.Load(groupCtx, req, limit)
			if err == nil {
				pages[index] = page
			}
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return pages, nil
}

// firstPageOnlySource 让辅助召回源只参与推荐首屏，避免复用主召回游标时语义冲突。
type firstPageOnlySource struct{ CandidateSource }

func (s firstPageOnlySource) Load(ctx context.Context, req FeedRequest, limit int) (*FeedPage, error) {
	if strings.TrimSpace(req.Cursor) != "" {
		return &FeedPage{Scene: req.Scene, Items: []*domainfeed.FeedPageItem{}}, nil
	}
	return s.CandidateSource.Load(ctx, req, limit)
}

// optionalCandidateSource 将辅助召回故障降级为空结果，主召回仍负责请求成败。
type optionalCandidateSource struct{ CandidateSource }

func (s optionalCandidateSource) Load(ctx context.Context, req FeedRequest, limit int) (*FeedPage, error) {
	page, err := s.CandidateSource.Load(ctx, req, limit)
	if err != nil && ctx.Err() == nil {
		return &FeedPage{Scene: req.Scene, Items: []*domainfeed.FeedPageItem{}}, nil
	}
	return page, err
}

type TimelineCandidateSource struct {
	scene domainfeed.Scene
	repo  domainfeed.Repository
}

func NewTimelineCandidateSource(scene domainfeed.Scene, repo domainfeed.Repository) *TimelineCandidateSource {
	return &TimelineCandidateSource{scene: domainfeed.NormalizeScene(scene), repo: repo}
}

func (s *TimelineCandidateSource) Load(ctx context.Context, req FeedRequest, limit int) (*FeedPage, error) {
	cursor, err := parseTimelineCursor(req.Cursor)
	if err != nil {
		return nil, err
	}
	items, err := s.repo.ListTimelinePage(ctx, cursor, limit+1)
	if err != nil {
		return nil, ErrLoadFeedFailed
	}
	return timelinePage(s.scene, items, limit), nil
}

type HotCandidateSource struct {
	repo  domainfeed.Repository
	cache FeedCache
}

func NewHotCandidateSource(repo domainfeed.Repository, cache FeedCache) *HotCandidateSource {
	return &HotCandidateSource{repo: repo, cache: cache}
}

func (s *HotCandidateSource) Load(ctx context.Context, req FeedRequest, limit int) (*FeedPage, error) {
	cursor, err := parseHotCursor(req.Cursor)
	if err != nil {
		return nil, err
	}
	if s.cache != nil {
		if strings.TrimSpace(req.Cursor) != "" && (cursor == nil || cursor.WindowEnd.IsZero()) {
			return nil, domainfeed.ErrInvalidCursor
		}
		windowEnd, offset := time.Now().UTC().Truncate(time.Minute), 0
		if cursor != nil && !cursor.WindowEnd.IsZero() {
			windowEnd, offset = cursor.WindowEnd.UTC().Truncate(time.Minute), cursor.Offset
		}
		items, err := s.cache.ListHotWindowPage(ctx, windowEnd, offset, limit+1)
		if err != nil {
			return nil, ErrLoadFeedFailed
		}
		hasMore := len(items) > limit
		if hasMore {
			items = items[:limit]
		}
		next := ""
		if len(items) > 0 {
			next = encodeHotWindowCursor(&domainfeed.HotCursor{WindowEnd: windowEnd, Offset: offset + len(items)})
		}
		return &FeedPage{Scene: domainfeed.SceneHot, Items: items, NextCursor: next, HasMore: hasMore}, nil
	}
	items, err := s.repo.ListHotPage(ctx, cursor, limit+1)
	if err != nil {
		return nil, ErrLoadFeedFailed
	}
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	next := ""
	if len(items) > 0 {
		last := items[len(items)-1]
		next = encodeHotCursor(&domainfeed.HotCursor{HotScore: last.HotScore, PublishedAt: last.PublishedAt, VideoID: last.VideoID})
	}
	return &FeedPage{Scene: domainfeed.SceneHot, Items: items, NextCursor: next, HasMore: hasMore}, nil
}

type FollowingCandidateSource struct {
	repo  domainfeed.Repository
	index FollowingIndexCache
}

func NewFollowingCandidateSource(repo domainfeed.Repository, index FollowingIndexCache) *FollowingCandidateSource {
	return &FollowingCandidateSource{repo: repo, index: index}
}

func (s *FollowingCandidateSource) Load(ctx context.Context, req FeedRequest, limit int) (*FeedPage, error) {
	if req.ViewerID <= 0 {
		return nil, domainfeed.ErrViewerRequired
	}
	cursor, err := parseTimelineCursor(req.Cursor)
	if err != nil {
		return nil, err
	}
	var items []*domainfeed.FeedPageItem
	if s.index != nil {
		oldest, complete, ready, cacheErr := s.index.GetFollowingSnapshot(ctx, req.ViewerID)
		if cacheErr == nil && !ready {
			// Cold start loads the bounded index from the SQL source of truth.
			snapshot, err := s.repo.ListFollowingPage(ctx, req.ViewerID, nil, 1001)
			if err != nil {
				return nil, ErrLoadFeedFailed
			}
			complete = len(snapshot) <= 1000
			if len(snapshot) > 1000 {
				snapshot = snapshot[:1000]
			}
			if s.index.BootstrapFollowingIndex(ctx, req.ViewerID, snapshot, complete) == nil {
				ready = true
				if len(snapshot) > 0 {
					last := snapshot[len(snapshot)-1]
					oldest = &domainfeed.TimelineCursor{PublishedAt: last.PublishedAt, VideoID: last.VideoID}
				}
			}
		}
		if cacheErr == nil && ready && (complete || !timelineCursorAtOrBefore(cursor, oldest)) {
			authorIDs, pullAuthorIDs, err := s.followingAuthorIDs(ctx, req.ViewerID)
			if err != nil {
				return nil, ErrLoadFeedFailed
			}
			loaded, ok, err := s.index.ListFollowingIndexPage(ctx, req.ViewerID, authorIDs, pullAuthorIDs, oldest != nil, cursor, limit+1)
			if err == nil && ok {
				items = loaded
			}
		}
	}
	if items == nil {
		items, err = s.repo.ListFollowingPage(ctx, req.ViewerID, cursor, limit+1)
		if err != nil {
			return nil, ErrLoadFeedFailed
		}
	}
	return timelinePage(domainfeed.SceneFollowing, items, limit), nil
}

func (s *FollowingCandidateSource) followingAuthorIDs(ctx context.Context, viewerID int64) ([]int64, []int64, error) {
	relationCache, _ := s.index.(FollowingRelationCache)
	if relationCache != nil {
		if authorIDs, pullAuthorIDs, ok, err := relationCache.GetFollowingAuthorIDs(ctx, viewerID); err == nil && ok {
			return authorIDs, pullAuthorIDs, nil
		}
	}
	authorIDs, err := s.repo.ListFollowingAuthorIDs(ctx, viewerID)
	if err != nil {
		return nil, nil, err
	}
	pullAuthorIDs, err := s.repo.ListFollowingPullAuthorIDs(ctx, viewerID)
	if err != nil {
		return nil, nil, err
	}
	if relationCache != nil {
		_ = relationCache.SetFollowingAuthorIDs(ctx, viewerID, authorIDs, pullAuthorIDs)
	}
	return authorIDs, pullAuthorIDs, nil
}

func timelineCursorAtOrBefore(cursor *domainfeed.TimelineCursor, oldest *domainfeed.TimelineCursor) bool {
	if cursor == nil || oldest == nil {
		return false
	}
	return cursor.PublishedAt.Before(oldest.PublishedAt) ||
		(cursor.PublishedAt.Equal(oldest.PublishedAt) && cursor.VideoID <= oldest.VideoID)
}

type RecommendCandidateSource struct{ recommender Recommender }

func NewRecommendCandidateSource(recommender Recommender) *RecommendCandidateSource {
	return &RecommendCandidateSource{recommender: recommender}
}
func (s *RecommendCandidateSource) Load(ctx context.Context, req FeedRequest, limit int) (*FeedPage, error) {
	if req.ViewerID <= 0 {
		return nil, domainfeed.ErrViewerRequired
	}
	result, err := s.recommender.Recommend(ctx, applicationrecommendation.CandidateRequest{UserID: req.ViewerID, Scene: string(domainfeed.SceneRecommend), RequestID: clientContextValue(req.ClientContext, "request_id"), Cursor: req.Cursor, Limit: limit})
	if err != nil {
		if errors.Is(err, applicationrecommendation.ErrLoadRecommendationFailed) {
			return nil, ErrLoadFeedFailed
		}
		return nil, err
	}
	items := make([]*domainfeed.FeedPageItem, 0, len(result.Candidates))
	for _, c := range result.Candidates {
		items = append(items, &domainfeed.FeedPageItem{VideoID: c.VideoID, AuthorID: c.AuthorID, PublishedAt: c.PublishedAt, HotScore: c.HotScore})
	}
	return &FeedPage{Scene: domainfeed.SceneRecommend, Items: items, NextCursor: result.NextCursor, HasMore: result.HasMore}, nil
}

func timelinePage(scene domainfeed.Scene, items []*domainfeed.FeedPageItem, limit int) *FeedPage {
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	next := ""
	if len(items) > 0 {
		last := items[len(items)-1]
		next = encodeTimelineCursor(&domainfeed.TimelineCursor{PublishedAt: last.PublishedAt, VideoID: last.VideoID})
	}
	return &FeedPage{Scene: scene, Items: items, NextCursor: next, HasMore: hasMore}
}
