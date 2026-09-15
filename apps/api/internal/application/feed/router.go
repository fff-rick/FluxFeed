package applicationfeed

import (
	domainfeed "FluxFeed/internal/domain/feed"
	"sync"
)

// FeedRouter 负责 scene -> Strategy 的注册与路由。
// 将路由职责从 Service 中拆出后，新增 Feed 场景无需修改核心 Service。
type FeedRouter struct {
	mu         sync.RWMutex
	strategies map[domainfeed.Scene]Strategy
}

func NewFeedRouter() *FeedRouter {
	return &FeedRouter{strategies: make(map[domainfeed.Scene]Strategy)}
}

func (r *FeedRouter) Register(strategy Strategy) {
	if r == nil || strategy == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.strategies[domainfeed.NormalizeScene(strategy.Scene())] = strategy
}

func (r *FeedRouter) Route(scene domainfeed.Scene) (Strategy, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	strategy, ok := r.strategies[domainfeed.NormalizeScene(scene)]
	return strategy, ok
}

func (r *FeedRouter) Range(fn func(Strategy)) {
	if r == nil || fn == nil {
		return
	}
	r.mu.RLock()
	items := make([]Strategy, 0, len(r.strategies))
	for _, strategy := range r.strategies {
		items = append(items, strategy)
	}
	r.mu.RUnlock()
	for _, strategy := range items {
		fn(strategy)
	}
}
