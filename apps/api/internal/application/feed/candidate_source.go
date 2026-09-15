package applicationfeed

import (
	applicationrecommendation "FluxFeed/internal/application/recommendation"
	domainfeed "FluxFeed/internal/domain/feed"
	"context"
	"errors"
	"strings"
	"time"
)

// CandidateSource 只负责“从哪里取候选”，不负责卡片组装和 HTTP 输出。
// Strategy 负责场景策略，CandidateSource 负责数据来源，两者解耦后可以独立扩展召回源。
type CandidateSource interface {
	Load(ctx context.Context, req FeedRequest, limit int) (*FeedPage, error)
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
		authorIDs, e := s.repo.ListFollowingPullAuthorIDs(ctx, req.ViewerID)
		if e != nil {
			return nil, ErrLoadFeedFailed
		}
		loaded, ok, e := s.index.ListFollowingIndexPage(ctx, req.ViewerID, authorIDs, cursor, limit+1)
		if e != nil {
			return nil, ErrLoadFeedFailed
		}
		if ok {
			items = loaded
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
