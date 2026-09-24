package infracache

import (
	applicationfeed "FluxFeed/internal/application/feed"
	applicationinteraction "FluxFeed/internal/application/interaction"
	domainfeed "FluxFeed/internal/domain/feed"
	domaininteraction "FluxFeed/internal/domain/interaction"
	inframetrics "FluxFeed/internal/infra/metrics"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const hotWindowMinutes = 60
const hotMinuteBucketTTL = 2 * time.Hour
const hotWindowCacheTTL = 2 * time.Minute
const actionStateTTL = 30 * 24 * time.Hour
const actionStatTTL = 24 * time.Hour
const actionStatJSONTTL = 15 * time.Second
const actionStatCounterShardCount = 16
const followingIndexKeyTTL = 30 * 24 * time.Hour
const localCacheMaxEntries = 10_000
const localPageCacheTTL = 2 * time.Second
const localCardCacheTTL = 30 * time.Second
const localStatCacheTTL = 2 * time.Second
const missingCardCacheTTL = 30 * time.Second
const followingRelationCacheTTL = 10 * time.Minute
const seenCacheTTL = 30 * 24 * time.Hour

// ponytail: 每用户固定 32 KiB；曝光规模显著超过 2 万/月时再按月分片或扩大 Bitmap。
const seenBloomBits = uint64(1 << 18)
const seenBloomHashes = 3

var missingCardValue = []byte("-")

type followingSnapshot struct {
	Oldest   *domainfeed.TimelineCursor `json:"oldest,omitempty"`
	Complete bool                       `json:"complete"`
}

type redisWatchCmdable interface {
	redis.Cmdable
	Pipeline() redis.Pipeliner
	Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
}

type redisActionStatReader interface {
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	Get(ctx context.Context, key string) *redis.StringCmd
}

type redisActionStatWriter interface {
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
}

type redisStatCacheClient interface {
	redisActionStatReader
	redisActionStatWriter
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
}

// FeedCache 使用 Redis 保存 Feed 查询结果。
type FeedCache struct {
	client redisWatchCmdable
	local  *localCache
}

// NewFeedCache 创建 Feed 结果缓存。
func NewFeedCache(client redisWatchCmdable) *FeedCache {
	return &FeedCache{client: client, local: newLocalCache(localCacheMaxEntries)}
}

// GetPage 读取缓存中的轻量 Feed 页。
func (c *FeedCache) GetPage(ctx context.Context, key string) (*applicationfeed.FeedPage, bool, error) {
	page, ok, _, err := c.GetPageState(ctx, key)
	return page, ok, err
}

// GetPageState 在软过期窗口返回 stale=true，供应用层后台刷新并继续服务旧值。
func (c *FeedCache) GetPageState(ctx context.Context, key string) (*applicationfeed.FeedPage, bool, bool, error) {
	if content, ok := c.local.get(key); ok {
		var page applicationfeed.FeedPage
		if json.Unmarshal(content, &page) == nil {
			inframetrics.ObserveCacheRead("page_l1", 1, 1, nil)
			return &page, true, false, nil
		}
	}
	if content, ok := c.local.getStale(key); ok {
		var page applicationfeed.FeedPage
		if json.Unmarshal(content, &page) == nil {
			inframetrics.ObserveCacheRead("page_stale", 1, 1, nil)
			return &page, true, true, nil
		}
	}
	content, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		inframetrics.ObserveCacheRead("page", 1, 0, nil)
		return nil, false, false, nil
	}
	if err != nil {
		inframetrics.ObserveCacheRead("page", 1, 0, err)
		return nil, false, false, err
	}

	var page applicationfeed.FeedPage
	if err := json.Unmarshal(content, &page); err != nil {
		inframetrics.ObserveCacheRead("page", 1, 0, err)
		return nil, false, false, err
	}
	c.local.set(key, content, localPageCacheTTL)
	inframetrics.ObserveCacheRead("page", 1, 1, nil)
	return &page, true, false, nil
}

// SetPage 写入轻量 Feed 页，并设置过期时间。
func (c *FeedCache) SetPage(ctx context.Context, key string, page *applicationfeed.FeedPage, ttl time.Duration) error {
	content, err := json.Marshal(page)
	if err != nil {
		inframetrics.ObserveCacheWrite("page", 1, err)
		return err
	}
	err = c.client.Set(ctx, key, content, ttl).Err()
	// L2 短暂不可用时仍保留本进程的短期结果，避免请求直接打到 MySQL。
	c.local.setStale(key, content, ttl, ttl*2)
	inframetrics.ObserveCacheWrite("page", 1, err)
	return err
}

// GetCards 批量读取视频卡片缓存。
func (c *FeedCache) GetCards(ctx context.Context, videoIDs []int64) (map[int64]*domainfeed.FeedCard, error) {
	cards := map[int64]*domainfeed.FeedCard{}
	if len(videoIDs) == 0 {
		return cards, nil
	}

	misses := make([]int64, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		content, ok := c.local.get(feedCardKey(videoID))
		if !ok {
			misses = append(misses, videoID)
			continue
		}
		if string(content) == string(missingCardValue) {
			cards[videoID] = nil
			continue
		}
		var card domainfeed.FeedCard
		if json.Unmarshal(content, &card) == nil {
			cards[videoID] = &card
		}
	}
	if len(misses) == 0 {
		inframetrics.ObserveCacheRead("card_l1", len(videoIDs), len(cards), nil)
		return cards, nil
	}

	values, err := c.client.MGet(ctx, cacheKeys(misses, feedCardKey)...).Result()
	if err != nil {
		inframetrics.ObserveCacheRead("card", len(misses), 0, err)
		if len(cards) > 0 {
			return cards, nil
		}
		return nil, err
	}
	for index, value := range values {
		content, ok := cacheValueBytes(value)
		if !ok {
			continue
		}
		videoID := misses[index]
		c.local.set(feedCardKey(videoID), content, localCardCacheTTL)
		if string(content) == string(missingCardValue) {
			cards[videoID] = nil
			continue
		}
		var card domainfeed.FeedCard
		if err := json.Unmarshal(content, &card); err != nil {
			continue
		}
		if card.VideoID <= 0 {
			card.VideoID = videoID
		}
		cards[card.VideoID] = &card
	}
	inframetrics.ObserveCacheRead("card", len(videoIDs), len(cards), nil)
	return cards, nil
}

// SetCards 批量写入视频卡片缓存。
func (c *FeedCache) SetCards(ctx context.Context, cards map[int64]*domainfeed.FeedCard, ttl time.Duration) error {
	pipe := c.client.Pipeline()
	queued := false

	for _, card := range cards {
		if card == nil {
			continue
		}
		content, err := json.Marshal(card)
		if err != nil {
			return err
		}
		key := feedCardKey(card.VideoID)
		entryTTL := jitterTTL(key, ttl)
		pipe.Set(ctx, key, content, entryTTL)
		c.local.set(key, content, minTTL(entryTTL, localCardCacheTTL))
		queued = true
	}
	for videoID, card := range cards {
		if card != nil || videoID <= 0 {
			continue
		}
		key := feedCardKey(videoID)
		pipe.Set(ctx, key, missingCardValue, jitterTTL(key, missingCardCacheTTL))
		c.local.set(key, missingCardValue, missingCardCacheTTL)
		queued = true
	}
	if !queued {
		return nil
	}
	_, err := pipe.Exec(ctx)
	inframetrics.ObserveCacheWrite("card", len(cards), err)
	return err
}

// GetStats 批量读取视频计数缓存。
func (c *FeedCache) GetStats(ctx context.Context, videoIDs []int64) (map[int64]*domainfeed.FeedStat, error) {
	stats := make(map[int64]*domainfeed.FeedStat, len(videoIDs))
	misses := make([]int64, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		content, ok := c.local.get(feedStatKey(videoID))
		if !ok {
			misses = append(misses, videoID)
			continue
		}
		var stat domainfeed.FeedStat
		if json.Unmarshal(content, &stat) != nil {
			misses = append(misses, videoID)
			continue
		}
		stats[videoID] = &stat
	}
	if len(misses) == 0 {
		inframetrics.ObserveCacheRead("stat_l1", len(videoIDs), len(stats), nil)
		return stats, nil
	}
	loaded, err := getStats(ctx, c.client, misses)
	if err != nil {
		if len(stats) > 0 {
			return stats, nil
		}
		return nil, err
	}
	for videoID, stat := range loaded {
		stats[videoID] = stat
		if content, err := json.Marshal(stat); err == nil {
			c.local.set(feedStatKey(videoID), content, localStatCacheTTL)
		}
	}
	return stats, nil
}

func getStats(ctx context.Context, client redisStatCacheClient, videoIDs []int64) (map[int64]*domainfeed.FeedStat, error) {
	stats := map[int64]*domainfeed.FeedStat{}
	if len(videoIDs) == 0 {
		return stats, nil
	}

	values, err := client.MGet(ctx, cacheKeys(videoIDs, feedStatKey)...).Result()
	if err != nil {
		inframetrics.ObserveCacheRead("stat", len(videoIDs), 0, err)
		return nil, err
	}
	for index, value := range values {
		content, ok := cacheValueBytes(value)
		if !ok {
			continue
		}
		var stat domainfeed.FeedStat
		if err := json.Unmarshal(content, &stat); err != nil {
			continue
		}
		if stat.VideoID <= 0 {
			stat.VideoID = videoIDs[index]
		}
		stats[stat.VideoID] = &stat
	}
	for _, videoID := range videoIDs {
		if stats[videoID] != nil {
			continue
		}
		stat, ok, err := actionStatFromCache(ctx, client, videoID)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		stats[videoID] = stat
		_ = setActionStatJSON(ctx, client, feedStatKey(videoID), stat)
	}
	inframetrics.ObserveCacheRead("stat", len(videoIDs), len(stats), nil)
	return stats, nil
}

// SetStats 批量写入视频计数缓存。
func (c *FeedCache) SetStats(ctx context.Context, stats map[int64]*domainfeed.FeedStat, ttl time.Duration) error {
	pipe := c.client.Pipeline()
	queued := false

	for _, stat := range stats {
		if stat == nil || stat.VideoID <= 0 {
			continue
		}
		content, err := json.Marshal(stat)
		if err != nil {
			return err
		}
		key := feedStatKey(stat.VideoID)
		entryTTL := jitterTTL(key, ttl)
		pipe.Set(ctx, key, content, entryTTL)
		c.local.set(key, content, minTTL(entryTTL, localStatCacheTTL))
		queued = true
	}
	if !queued {
		return nil
	}
	_, err := pipe.Exec(ctx)
	inframetrics.ObserveCacheWrite("stat", len(stats), err)
	return err
}

// SetVideoStat 写入单个视频的计数缓存，用于评论写入后刷新 Feed 展示。
func (c *FeedCache) SetVideoStat(ctx context.Context, stat *domaininteraction.VideoStat) error {
	if stat == nil || stat.VideoID <= 0 {
		return nil
	}
	feedStat := videoStatToFeedStat(stat)
	err := setActionStatJSON(ctx, c.client, feedStatKey(stat.VideoID), feedStat)
	if err == nil {
		if content, marshalErr := json.Marshal(feedStat); marshalErr == nil {
			c.local.set(feedStatKey(stat.VideoID), content, localStatCacheTTL)
		}
	}
	inframetrics.ObserveCacheWrite("stat", 1, err)
	return err
}

func videoStatToFeedStat(stat *domaininteraction.VideoStat) *domainfeed.FeedStat {
	if stat == nil {
		return nil
	}
	return &domainfeed.FeedStat{
		VideoID:       stat.VideoID,
		LikeCount:     stat.LikeCount,
		CommentCount:  stat.CommentCount,
		FavoriteCount: stat.FavoriteCount,
	}
}

func (c *FeedCache) AddInboxItems(ctx context.Context, authorID int64, userIDs []int64, item *domainfeed.FeedPageItem, maxLen int64) error {
	if authorID <= 0 || item == nil || item.VideoID <= 0 || item.PublishedAt.IsZero() || len(userIDs) == 0 {
		return nil
	}
	if maxLen <= 0 {
		maxLen = 1000
	}
	pipe := c.client.Pipeline()
	member := followingIndexMember(item.VideoID, authorID, item.PublishedAt)
	for _, userID := range userIDs {
		if userID <= 0 {
			continue
		}
		key := followingInboxKey(userID)
		pipe.ZAdd(ctx, key, redis.Z{Score: 0, Member: member})
		pipe.ZRemRangeByRank(ctx, key, 0, -maxLen-1)
		pipe.Expire(ctx, key, followingIndexKeyTTL)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *FeedCache) AddAuthorOutboxItem(ctx context.Context, authorID int64, item *domainfeed.FeedPageItem, maxLen int64) error {
	if authorID <= 0 || item == nil || item.VideoID <= 0 || item.PublishedAt.IsZero() {
		return nil
	}
	if maxLen <= 0 {
		maxLen = 500
	}
	key := followingAuthorOutboxKey(authorID)
	member := followingIndexMember(item.VideoID, authorID, item.PublishedAt)
	pipe := c.client.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: 0, Member: member})
	pipe.ZRemRangeByRank(ctx, key, 0, -maxLen-1)
	pipe.Expire(ctx, key, followingIndexKeyTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *FeedCache) ListFollowingIndexPage(ctx context.Context, viewerID int64, authorIDs []int64, pullAuthorIDs []int64, expectInbox bool, cursor *domainfeed.TimelineCursor, limit int) ([]*domainfeed.FeedPageItem, bool, error) {
	if viewerID <= 0 || limit <= 0 {
		return []*domainfeed.FeedPageItem{}, false, nil
	}
	keys := []string{followingInboxKey(viewerID)}
	for _, authorID := range pullAuthorIDs {
		if authorID > 0 {
			keys = append(keys, followingAuthorOutboxKey(authorID))
		}
	}

	pipe := c.client.Pipeline()
	cardinalityCommands := make([]*redis.IntCmd, 0, len(keys))
	rangeCommands := make([]*redis.StringSliceCmd, 0, len(keys))
	maxMember := "+"
	if cursor != nil {
		maxMember = "(" + followingCursorPrefix(cursor)
	}
	for _, key := range keys {
		cardinalityCommands = append(cardinalityCommands, pipe.ZCard(ctx, key))
		rangeCommands = append(rangeCommands, pipe.ZRevRangeByLex(ctx, key, &redis.ZRangeBy{
			Min:   "-",
			Max:   maxMember,
			Count: int64(limit),
		}))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, false, err
	}

	hasIndex := false
	saturated := false
	saturatedKeys := make([]string, 0)
	for index, cmd := range cardinalityCommands {
		count, err := cmd.Result()
		if err != nil && err != redis.Nil {
			return nil, false, err
		}
		if count > 0 {
			hasIndex = true
		}
		maxLen := int64(500)
		if index == 0 {
			maxLen = 1000
		}
		if count >= maxLen {
			saturated = true
			saturatedKeys = append(saturatedKeys, keys[index])
		}
	}
	if !hasIndex || (expectInbox && cardinalityCommands[0].Val() == 0) {
		return nil, false, nil
	}

	seen := map[int64]struct{}{}
	allowedAuthors := int64Set(authorIDs)
	items := make([]*domainfeed.FeedPageItem, 0, limit*len(rangeCommands))
	truncatedByFilter := false
	for _, cmd := range rangeCommands {
		members, err := cmd.Result()
		if err != nil && err != redis.Nil {
			return nil, false, err
		}
		if len(members) == limit {
			truncatedByFilter = true
		}
		for _, member := range members {
			item, ok := feedPageItemFromFollowingMember(member)
			if !ok {
				continue
			}
			if _, followed := allowedAuthors[item.AuthorID]; !followed {
				continue
			}
			if _, exists := seen[item.VideoID]; exists {
				continue
			}
			seen[item.VideoID] = struct{}{}
			items = append(items, item)
		}
	}
	sortFeedPageItemsByTimeline(items)
	if len(items) < limit && (saturated || truncatedByFilter) {
		return nil, false, nil
	}
	if len(items) >= limit {
		last := items[limit-1]
		for _, key := range saturatedKeys {
			members, err := c.client.ZRange(ctx, key, 0, 0).Result()
			if err != nil || len(members) == 0 {
				return nil, false, err
			}
			oldest, ok := feedPageItemFromFollowingMember(members[0])
			if !ok || followingPageTouchesBoundary(last, oldest) {
				return nil, false, nil
			}
		}
	}
	if len(items) > limit {
		items = items[:limit]
	}
	return items, true, nil
}

func followingPageTouchesBoundary(last *domainfeed.FeedPageItem, oldest *domainfeed.FeedPageItem) bool {
	return last.PublishedAt.Before(oldest.PublishedAt) ||
		(last.PublishedAt.Equal(oldest.PublishedAt) && last.VideoID <= oldest.VideoID)
}

func (c *FeedCache) GetFollowingSnapshot(ctx context.Context, viewerID int64) (*domainfeed.TimelineCursor, bool, bool, error) {
	content, err := c.client.Get(ctx, followingSnapshotKey(viewerID)).Bytes()
	if err == redis.Nil {
		return nil, false, false, nil
	}
	if err != nil {
		return nil, false, false, err
	}
	var snapshot followingSnapshot
	if err := json.Unmarshal(content, &snapshot); err != nil {
		return nil, false, false, err
	}
	return snapshot.Oldest, snapshot.Complete, true, nil
}

func (c *FeedCache) BootstrapFollowingIndex(ctx context.Context, viewerID int64, items []*domainfeed.FeedPageItem, complete bool) error {
	if viewerID <= 0 {
		return nil
	}
	snapshot := followingSnapshot{Complete: complete}
	if len(items) > 0 {
		last := items[len(items)-1]
		snapshot.Oldest = &domainfeed.TimelineCursor{PublishedAt: last.PublishedAt, VideoID: last.VideoID}
	}
	content, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	pipe := c.client.Pipeline()
	key := followingInboxKey(viewerID)
	for _, item := range items {
		if item != nil && item.VideoID > 0 && item.AuthorID > 0 && !item.PublishedAt.IsZero() {
			pipe.ZAdd(ctx, key, redis.Z{Score: 0, Member: followingIndexMember(item.VideoID, item.AuthorID, item.PublishedAt)})
		}
	}
	pipe.ZRemRangeByRank(ctx, key, 0, -1001)
	pipe.Expire(ctx, key, followingIndexKeyTTL)
	pipe.Set(ctx, followingSnapshotKey(viewerID), content, followingIndexKeyTTL)
	_, err = pipe.Exec(ctx)
	return err
}

func (c *FeedCache) InvalidateFollowingIndex(ctx context.Context, viewerID int64) error {
	return c.client.Del(ctx,
		followingInboxKey(viewerID),
		followingSnapshotKey(viewerID),
		followingAuthorsKey(viewerID),
		followingPullAuthorsKey(viewerID),
		followingAuthorsReadyKey(viewerID),
	).Err()
}

// GetFollowingAuthorIDs 读取关注作者快照；ready 键用于区分空集合和缓存未命中。
func (c *FeedCache) GetFollowingAuthorIDs(ctx context.Context, viewerID int64) ([]int64, []int64, bool, error) {
	if viewerID <= 0 {
		return nil, nil, false, nil
	}
	pipe := c.client.Pipeline()
	readyCmd := pipe.Exists(ctx, followingAuthorsReadyKey(viewerID))
	authorCmd := pipe.SMembers(ctx, followingAuthorsKey(viewerID))
	pullCmd := pipe.SMembers(ctx, followingPullAuthorsKey(viewerID))
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		inframetrics.ObserveCacheRead("following", 1, 0, err)
		return nil, nil, false, err
	}
	if readyCmd.Val() == 0 {
		inframetrics.ObserveCacheRead("following", 1, 0, nil)
		return nil, nil, false, nil
	}
	authorIDs, err := parseIDStrings(authorCmd.Val())
	if err != nil {
		return nil, nil, false, err
	}
	pullAuthorIDs, err := parseIDStrings(pullCmd.Val())
	if err != nil {
		return nil, nil, false, err
	}
	inframetrics.ObserveCacheRead("following", 1, 1, nil)
	return authorIDs, pullAuthorIDs, true, nil
}

// SetFollowingAuthorIDs 使用两个 SET 保存全部关注和大 V 子集。
func (c *FeedCache) SetFollowingAuthorIDs(ctx context.Context, viewerID int64, authorIDs []int64, pullAuthorIDs []int64) error {
	if viewerID <= 0 {
		return nil
	}
	ttl := jitterTTL(followingAuthorsReadyKey(viewerID), followingRelationCacheTTL)
	readyTTL := ttl - time.Second
	pipe := c.client.Pipeline()
	authorKey := followingAuthorsKey(viewerID)
	pullKey := followingPullAuthorsKey(viewerID)
	pipe.Del(ctx, authorKey, pullKey)
	if values := redisIDValues(authorIDs); len(values) > 0 {
		pipe.SAdd(ctx, authorKey, values...)
		pipe.Expire(ctx, authorKey, ttl)
	}
	if values := redisIDValues(pullAuthorIDs); len(values) > 0 {
		pipe.SAdd(ctx, pullKey, values...)
		pipe.Expire(ctx, pullKey, ttl)
	}
	// ready 先于集合过期，避免过期边界把已消失的集合误判成“已缓存的空集合”。
	pipe.Set(ctx, followingAuthorsReadyKey(viewerID), "1", readyTTL)
	_, err := pipe.Exec(ctx)
	inframetrics.ObserveCacheWrite("following", len(authorIDs), err)
	return err
}

// MarkSeen 将曝光写入固定大小 Bloom Bitmap，避免每个用户的 Seen Key 无界增长。
func (c *FeedCache) MarkSeen(ctx context.Context, userID int64, videoID int64) error {
	if userID <= 0 || videoID <= 0 {
		return nil
	}
	key := seenKey(userID)
	pipe := c.client.Pipeline()
	for _, offset := range seenOffsets(videoID) {
		pipe.SetBit(ctx, key, int64(offset), 1)
	}
	pipe.Expire(ctx, key, seenCacheTTL)
	_, err := pipe.Exec(ctx)
	inframetrics.ObserveCacheWrite("seen", 1, err)
	return err
}

// SeenVideoIDs 批量查询 Bloom Bitmap；三个位均存在时判定为已曝光。
func (c *FeedCache) SeenVideoIDs(ctx context.Context, userID int64, videoIDs []int64) (map[int64]struct{}, error) {
	seen := make(map[int64]struct{})
	if userID <= 0 || len(videoIDs) == 0 {
		return seen, nil
	}
	pipe := c.client.Pipeline()
	commands := make([][]*redis.IntCmd, len(videoIDs))
	key := seenKey(userID)
	for index, videoID := range videoIDs {
		for _, offset := range seenOffsets(videoID) {
			commands[index] = append(commands[index], pipe.GetBit(ctx, key, int64(offset)))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		inframetrics.ObserveCacheRead("seen", len(videoIDs), 0, err)
		return nil, err
	}
	for index, bits := range commands {
		matched := len(bits) == seenBloomHashes
		for _, bit := range bits {
			if bit.Val() == 0 {
				matched = false
				break
			}
		}
		if matched {
			seen[videoIDs[index]] = struct{}{}
		}
	}
	inframetrics.ObserveCacheRead("seen", len(videoIDs), len(seen), nil)
	return seen, nil
}

// AddHotScore 把一次互动热度写入 1 分钟粒度的热榜桶。
func (c *FeedCache) AddHotScore(ctx context.Context, videoID int64, scoreDelta int, at time.Time) error {
	if videoID <= 0 || scoreDelta == 0 {
		return nil
	}

	key := hotMinuteKey(at)
	if err := c.client.ZIncrBy(ctx, key, float64(scoreDelta), hotRankMember(videoID)).Err(); err != nil {
		return err
	}
	return c.client.Expire(ctx, key, hotMinuteBucketTTL).Err()
}

// ListHotWindowPage 合并最近 60 个分钟桶，返回一小时滑动窗口内的热榜页。
func (c *FeedCache) ListHotWindowPage(ctx context.Context, windowEnd time.Time, offset int, limit int) ([]*domainfeed.FeedPageItem, error) {
	items := []*domainfeed.FeedPageItem{}
	if limit <= 0 {
		return items, nil
	}
	if offset < 0 {
		offset = 0
	}

	windowEnd = windowEnd.UTC().Truncate(time.Minute)
	windowKey := hotWindowKey(windowEnd)
	exists, err := c.client.Exists(ctx, windowKey).Result()
	if err != nil {
		return nil, err
	}
	if exists == 0 {
		if err := c.rebuildHotWindow(ctx, windowKey, windowEnd); err != nil {
			return nil, err
		}
	}

	return c.listHotWindowPage(ctx, windowKey, offset, limit)
}

func (c *FeedCache) rebuildHotWindow(ctx context.Context, windowKey string, windowEnd time.Time) error {
	if _, err := c.client.ZUnionStore(ctx, windowKey, &redis.ZStore{
		Keys:      hotWindowMinuteKeys(windowEnd),
		Aggregate: "SUM",
	}).Result(); err != nil {
		return err
	}

	pipe := c.client.Pipeline()
	pipe.ZRemRangeByScore(ctx, windowKey, "-inf", "0")
	pipe.Expire(ctx, windowKey, hotWindowCacheTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *FeedCache) listHotWindowPage(ctx context.Context, windowKey string, offset int, limit int) ([]*domainfeed.FeedPageItem, error) {
	items := []*domainfeed.FeedPageItem{}
	values, err := c.client.ZRevRangeWithScores(ctx, windowKey, int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		member, ok := value.Member.(string)
		if !ok {
			continue
		}
		videoID, ok := hotRankVideoID(member)
		if !ok {
			continue
		}
		items = append(items, &domainfeed.FeedPageItem{
			VideoID:  videoID,
			HotScore: int(value.Score),
		})
	}
	return items, nil
}

// SetActionState 写入 Redis 行为状态和实时计数，供点赞收藏接口快速返回。
func (c *FeedCache) SetActionState(ctx context.Context, userID int64, videoID int64, actionType string, active bool, idempotencyKey string, initialStat *domaininteraction.VideoStat) (*applicationinteraction.ActionStateResult, error) {
	actionType, err := domaininteraction.NormalizeActionType(actionType)
	if err != nil {
		return nil, err
	}
	idempotencyKey = strings.TrimSpace(idempotencyKey)

	actionKey := interactionActionKey(userID, videoID, actionType)
	counterBaseKey := interactionStatCounterBaseKey(videoID)
	counterShardKey := interactionStatCounterShardKey(videoID, interactionStatCounterShardIndex(userID))
	jsonKey := feedStatKey(videoID)
	targetStatus := domaininteraction.ActionStatusCanceled
	if active {
		targetStatus = domaininteraction.ActionStatusActive
	}

	var result *applicationinteraction.ActionStateResult
	err = c.client.Watch(ctx, func(tx *redis.Tx) error {
		values, err := tx.HGetAll(ctx, actionKey).Result()
		if err != nil {
			return err
		}

		storedStatus, _ := strconv.Atoi(values["status"])
		storedIDKey := values["idempotency_key"]
		effectiveActive := active
		effectiveStatus := targetStatus
		delta := 0
		if storedIDKey == idempotencyKey && idempotencyKey != "" {
			effectiveActive = storedStatus == domaininteraction.ActionStatusActive
			effectiveStatus = storedStatus
			delta = 0
		} else {
			if storedStatus == 0 {
				if active {
					delta = 1
				}
			} else if storedStatus != targetStatus {
				if active {
					delta = 1
				} else {
					delta = -1
				}
			}
		}

		baseStat := actionStatBaseInit(videoID, initialStat)

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, actionKey, map[string]any{
				"status":          effectiveStatus,
				"idempotency_key": idempotencyKey,
				"updated_at":      time.Now().UTC().Format(time.RFC3339Nano),
			})
			pipe.Expire(ctx, actionKey, actionStateTTL)
			queueActionStatBaseInit(ctx, pipe, counterBaseKey, baseStat)
			pipe.Expire(ctx, counterBaseKey, actionStatTTL)
			if delta != 0 {
				pipe.HIncrBy(ctx, counterShardKey, interactionStatField(actionType), int64(delta))
			}
			pipe.Expire(ctx, counterShardKey, actionStatTTL)
			return nil
		})
		if err != nil {
			return err
		}

		result = &applicationinteraction.ActionStateResult{
			VideoID:        videoID,
			ActionType:     actionType,
			Active:         effectiveActive,
			Delta:          delta,
			IdempotencyKey: idempotencyKey,
		}
		return nil
	}, actionKey)
	if err != nil {
		return nil, err
	}

	stat, err := actionStat(ctx, c.client, counterBaseKey, interactionStatCounterShardKeys(videoID), jsonKey, videoID, initialStat)
	if err != nil {
		return nil, err
	}
	result.LikeCount = stat.LikeCount
	result.FavoriteCount = stat.FavoriteCount
	_ = setActionStatJSON(ctx, c.client, jsonKey, stat)
	return result, nil
}

func actionStat(ctx context.Context, client redisActionStatReader, counterBaseKey string, counterShardKeys []string, jsonKey string, videoID int64, initialStat *domaininteraction.VideoStat) (*domainfeed.FeedStat, error) {
	stat, _, err := actionStatWithPresence(ctx, client, counterBaseKey, counterShardKeys, jsonKey, videoID, initialStat)
	if err != nil {
		return nil, err
	}
	if stat == nil {
		return &domainfeed.FeedStat{VideoID: videoID}, nil
	}
	return stat, nil
}

func actionStatFromCache(ctx context.Context, client redisActionStatReader, videoID int64) (*domainfeed.FeedStat, bool, error) {
	return actionStatWithPresence(ctx, client, interactionStatCounterBaseKey(videoID), interactionStatCounterShardKeys(videoID), feedStatKey(videoID), videoID, nil)
}

func actionStatWithPresence(ctx context.Context, client redisActionStatReader, counterBaseKey string, counterShardKeys []string, jsonKey string, videoID int64, initialStat *domaininteraction.VideoStat) (*domainfeed.FeedStat, bool, error) {
	stat := &domainfeed.FeedStat{VideoID: videoID}
	found := false
	values, err := client.HGetAll(ctx, counterBaseKey).Result()
	if err != nil {
		return nil, false, err
	}
	if len(values) > 0 {
		applyActionStatFields(stat, values)
		found = true
	} else {
		fallbackStat, ok, err := actionStatFallback(ctx, client, jsonKey, videoID, initialStat)
		if err != nil {
			return nil, false, err
		}
		if ok {
			stat = fallbackStat
			found = true
		}
	}
	shardFound, err := applyActionStatShardDeltas(ctx, client, stat, counterShardKeys)
	if err != nil {
		return nil, false, err
	}
	found = found || shardFound
	if !found {
		return nil, false, nil
	}
	return stat, true, nil
}

func actionStatFallback(ctx context.Context, client redisActionStatReader, jsonKey string, videoID int64, initialStat *domaininteraction.VideoStat) (*domainfeed.FeedStat, bool, error) {
	stat := &domainfeed.FeedStat{VideoID: videoID}
	content, err := client.Get(ctx, jsonKey).Bytes()
	if err == redis.Nil {
		if initialStat != nil {
			stat.LikeCount = initialStat.LikeCount
			stat.CommentCount = initialStat.CommentCount
			stat.FavoriteCount = initialStat.FavoriteCount
			return stat, true, nil
		}
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if err := json.Unmarshal(content, stat); err != nil {
		return nil, false, nil
	}
	if stat.VideoID <= 0 {
		stat.VideoID = videoID
	}
	return stat, true, nil
}

func actionStatBaseInit(videoID int64, initialStat *domaininteraction.VideoStat) *domaininteraction.VideoStat {
	if initialStat != nil {
		return initialStat
	}
	return &domaininteraction.VideoStat{VideoID: videoID}
}

func queueActionStatBaseInit(ctx context.Context, pipe redis.Pipeliner, counterBaseKey string, initialStat *domaininteraction.VideoStat) {
	stat := &domaininteraction.VideoStat{}
	if initialStat != nil {
		stat = initialStat
	}
	pipe.HSetNX(ctx, counterBaseKey, "like_count", stat.LikeCount)
	pipe.HSetNX(ctx, counterBaseKey, "comment_count", stat.CommentCount)
	pipe.HSetNX(ctx, counterBaseKey, "favorite_count", stat.FavoriteCount)
}

func setActionStatJSON(ctx context.Context, client redisActionStatWriter, jsonKey string, stat *domainfeed.FeedStat) error {
	content, err := json.Marshal(stat)
	if err != nil {
		return err
	}
	return client.Set(ctx, jsonKey, content, actionStatJSONTTL).Err()
}

func applyActionStatShardDeltas(ctx context.Context, client redisActionStatReader, stat *domainfeed.FeedStat, shardKeys []string) (bool, error) {
	if stat == nil || len(shardKeys) == 0 {
		return false, nil
	}

	shardValues, err := loadActionStatShardValues(ctx, client, shardKeys)
	if err != nil {
		return false, err
	}
	found := false
	likeDelta := 0
	favoriteDelta := 0
	for _, values := range shardValues {
		if len(values) > 0 {
			found = true
		}
		likeDelta += actionStatFieldInt(values, "like_count")
		favoriteDelta += actionStatFieldInt(values, "favorite_count")
	}
	stat.LikeCount = clampRedisCount(stat.LikeCount + likeDelta)
	stat.FavoriteCount = clampRedisCount(stat.FavoriteCount + favoriteDelta)
	return found, nil
}

func loadActionStatShardValues(ctx context.Context, client redisActionStatReader, shardKeys []string) ([]map[string]string, error) {
	type pipelineProvider interface {
		Pipeline() redis.Pipeliner
	}

	if provider, ok := client.(pipelineProvider); ok {
		pipe := provider.Pipeline()
		cmds := make([]*redis.MapStringStringCmd, 0, len(shardKeys))
		for _, key := range shardKeys {
			cmds = append(cmds, pipe.HGetAll(ctx, key))
		}
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return nil, err
		}
		values := make([]map[string]string, 0, len(cmds))
		for _, cmd := range cmds {
			value, err := cmd.Result()
			if err != nil && err != redis.Nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}

	values := make([]map[string]string, 0, len(shardKeys))
	for _, key := range shardKeys {
		value, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func applyActionStatFields(stat *domainfeed.FeedStat, values map[string]string) {
	if stat == nil {
		return
	}
	stat.LikeCount = actionStatFieldInt(values, "like_count")
	stat.CommentCount = actionStatFieldInt(values, "comment_count")
	stat.FavoriteCount = actionStatFieldInt(values, "favorite_count")
}

func actionStatFieldInt(values map[string]string, field string) int {
	value, _ := strconv.Atoi(values[field])
	return value
}

func cacheKeys(videoIDs []int64, build func(int64) string) []string {
	keys := make([]string, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		keys = append(keys, build(videoID))
	}
	return keys
}

func jitterTTL(key string, ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return ttl + time.Duration(10+hasher.Sum32()%11)*ttl/100
}

func feedCardKey(videoID int64) string {
	return fmt.Sprintf("video:card:v1:%d", videoID)
}

func feedStatKey(videoID int64) string {
	return fmt.Sprintf("video:stat:v1:%d", videoID)
}

func followingInboxKey(userID int64) string {
	return fmt.Sprintf("feed:following:inbox:v2:%d", userID)
}

func followingAuthorOutboxKey(authorID int64) string {
	return fmt.Sprintf("feed:following:author:v2:%d", authorID)
}

func followingSnapshotKey(userID int64) string {
	return fmt.Sprintf("feed:following:snapshot:v2:%d", userID)
}

func followingAuthorsKey(userID int64) string {
	return fmt.Sprintf("user:following:v1:%d", userID)
}

func followingPullAuthorsKey(userID int64) string {
	return fmt.Sprintf("user:following:pull:v1:%d", userID)
}

func followingAuthorsReadyKey(userID int64) string {
	return fmt.Sprintf("user:following:ready:v1:%d", userID)
}

func seenKey(userID int64) string {
	return fmt.Sprintf("feed:seen:v1:%d", userID)
}

func seenOffsets(videoID int64) []uint64 {
	value := strconv.FormatInt(videoID, 10)
	offsets := make([]uint64, 0, seenBloomHashes)
	for seed := byte(0); seed < seenBloomHashes; seed++ {
		hasher := fnv.New64a()
		_, _ = hasher.Write([]byte{seed})
		_, _ = hasher.Write([]byte(value))
		offsets = append(offsets, hasher.Sum64()%seenBloomBits)
	}
	return offsets
}

func redisIDValues(ids []int64) []any {
	values := make([]any, 0, len(ids))
	for _, id := range ids {
		if id > 0 {
			values = append(values, id)
		}
	}
	return values
}

func parseIDStrings(values []string) ([]int64, error) {
	ids := make([]int64, 0, len(values))
	for _, value := range values {
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid cached id %q", value)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func followingIndexMember(videoID int64, authorID int64, publishedAt time.Time) string {
	// All v2 ZSET scores are zero; fixed-width members give exact timeline order without float collisions.
	return fmt.Sprintf("%019d:%020d:%020d", publishedAt.UTC().UnixNano(), videoID, authorID)
}

func followingCursorPrefix(cursor *domainfeed.TimelineCursor) string {
	return fmt.Sprintf("%019d:%020d", cursor.PublishedAt.UTC().UnixNano(), cursor.VideoID)
}

func feedPageItemFromFollowingMember(member string) (*domainfeed.FeedPageItem, bool) {
	parts := strings.Split(member, ":")
	if len(parts) != 3 {
		return nil, false
	}
	publishedNanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || publishedNanos <= 0 {
		return nil, false
	}
	videoID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || videoID <= 0 {
		return nil, false
	}
	authorID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || authorID <= 0 {
		return nil, false
	}
	return &domainfeed.FeedPageItem{
		VideoID:     videoID,
		AuthorID:    authorID,
		PublishedAt: time.Unix(0, publishedNanos).UTC(),
	}, true
}

func int64Set(values []int64) map[int64]struct{} {
	set := map[int64]struct{}{}
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func sortFeedPageItemsByTimeline(items []*domainfeed.FeedPageItem) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].PublishedAt.Equal(items[j].PublishedAt) {
			return items[i].VideoID > items[j].VideoID
		}
		return items[i].PublishedAt.After(items[j].PublishedAt)
	})
}

func interactionActionKey(userID int64, videoID int64, actionType string) string {
	return fmt.Sprintf("interaction:action:v1:%d:%d:%s", userID, videoID, strings.ToLower(actionType))
}

func interactionStatCounterKey(videoID int64) string {
	return fmt.Sprintf("video:stat:counter:v1:%d", videoID)
}

func interactionStatCounterBaseKey(videoID int64) string {
	return fmt.Sprintf("%s:base", interactionStatCounterKey(videoID))
}

func interactionStatCounterShardKey(videoID int64, shard int) string {
	return fmt.Sprintf("%s:shard:%02d", interactionStatCounterKey(videoID), shard)
}

func interactionStatCounterShardKeys(videoID int64) []string {
	keys := make([]string, 0, actionStatCounterShardCount)
	for shard := 0; shard < actionStatCounterShardCount; shard++ {
		keys = append(keys, interactionStatCounterShardKey(videoID, shard))
	}
	return keys
}

func interactionStatCounterShardIndex(userID int64) int {
	if userID <= 0 {
		return 0
	}
	return int(userID % actionStatCounterShardCount)
}

func interactionStatField(actionType string) string {
	if actionType == domaininteraction.ActionTypeLike {
		return "like_count"
	}
	return "favorite_count"
}

func clampRedisCount(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func hotMinuteKey(at time.Time) string {
	return fmt.Sprintf("feed:hot:minute:v1:%s", at.UTC().Truncate(time.Minute).Format("200601021504"))
}

func hotWindowKey(windowEnd time.Time) string {
	return fmt.Sprintf("feed:hot:window:v1:%d", windowEnd.UTC().Truncate(time.Minute).Unix())
}

func hotWindowMinuteKeys(windowEnd time.Time) []string {
	keys := make([]string, 0, hotWindowMinutes)
	for index := hotWindowMinutes - 1; index >= 0; index-- {
		keys = append(keys, hotMinuteKey(windowEnd.Add(-time.Duration(index)*time.Minute)))
	}
	return keys
}

func hotRankMember(videoID int64) string {
	return fmt.Sprintf("%020d", videoID)
}

func hotRankVideoID(member string) (int64, bool) {
	value := strings.TrimLeft(member, "0")
	if value == "" {
		return 0, false
	}
	videoID, err := strconv.ParseInt(value, 10, 64)
	return videoID, err == nil && videoID > 0
}

func cacheValueBytes(value any) ([]byte, bool) {
	switch typed := value.(type) {
	case nil:
		return nil, false
	case string:
		return []byte(typed), true
	case []byte:
		return typed, true
	default:
		return nil, false
	}
}
