package infracache

import (
	domainfeed "FluxFeed/internal/domain/feed"
	domaininteraction "FluxFeed/internal/domain/interaction"
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestFollowingIndexMemberExactTimelineOrder(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 0, 0, 123, time.UTC)
	members := []string{
		followingIndexMember(1, 7, at),
		followingIndexMember(1_000_001, 7, at),
		followingIndexMember(2, 7, at.Add(time.Nanosecond)),
	}
	sort.Sort(sort.Reverse(sort.StringSlice(members)))
	want := []int64{2, 1_000_001, 1}
	for index, member := range members {
		item, ok := feedPageItemFromFollowingMember(member)
		if !ok || item.VideoID != want[index] {
			t.Fatalf("unexpected index member %q: %+v", member, item)
		}
	}
	cursor := &domainfeed.TimelineCursor{PublishedAt: at, VideoID: 1_000_001}
	if followingIndexMember(cursor.VideoID, 7, at) != followingIndexMember(cursor.VideoID, 7, at.In(time.FixedZone("CST", 8*3600))) {
		t.Fatal("replayed publication must use the same ZSET member")
	}
	if !(members[1] > followingCursorPrefix(cursor) && members[2] < followingCursorPrefix(cursor)) {
		t.Fatal("cursor boundary must exclude the current video and retain older videos")
	}
}

func TestFollowingPageCapacityBoundary(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	oldest := &domainfeed.FeedPageItem{VideoID: 1_000_001, PublishedAt: at}
	for _, test := range []struct {
		item *domainfeed.FeedPageItem
		want bool
	}{
		{&domainfeed.FeedPageItem{VideoID: 1_000_002, PublishedAt: at}, false},
		{&domainfeed.FeedPageItem{VideoID: 1_000_001, PublishedAt: at}, true},
		{&domainfeed.FeedPageItem{VideoID: 1, PublishedAt: at}, true},
		{&domainfeed.FeedPageItem{VideoID: 2_000_000, PublishedAt: at.Add(-time.Nanosecond)}, true},
	} {
		if got := followingPageTouchesBoundary(test.item, oldest); got != test.want {
			t.Fatalf("boundary decision for %+v: got %v, want %v", test.item, got, test.want)
		}
	}
}

type actionStatFakeRedis struct {
	hashes map[string]map[string]string
	values map[string]string
}

func newActionStatFakeRedis() *actionStatFakeRedis {
	return &actionStatFakeRedis{
		hashes: map[string]map[string]string{},
		values: map[string]string{},
	}
}

func (r *actionStatFakeRedis) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	values := r.hashes[key]
	if values == nil {
		values = map[string]string{}
	}
	return redis.NewMapStringStringResult(values, nil)
}

func (r *actionStatFakeRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	value, ok := r.values[key]
	if !ok {
		return redis.NewStringResult("", redis.Nil)
	}
	return redis.NewStringResult(value, nil)
}

func (r *actionStatFakeRedis) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	switch typed := value.(type) {
	case string:
		r.values[key] = typed
	case []byte:
		r.values[key] = string(typed)
	default:
		content, _ := json.Marshal(typed)
		r.values[key] = string(content)
	}
	return redis.NewStatusResult("OK", nil)
}

func (r *actionStatFakeRedis) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	values := make([]any, 0, len(keys))
	for _, key := range keys {
		if value, ok := r.values[key]; ok {
			values = append(values, value)
			continue
		}
		values = append(values, nil)
	}
	return redis.NewSliceResult(values, nil)
}

func TestActionStatAggregatesCounterShards(t *testing.T) {
	ctx := context.Background()
	videoID := int64(1001)
	redisClient := newActionStatFakeRedis()
	redisClient.hashes[interactionStatCounterBaseKey(videoID)] = map[string]string{
		"like_count":     "10",
		"comment_count":  "3",
		"favorite_count": "4",
	}
	redisClient.hashes[interactionStatCounterShardKey(videoID, interactionStatCounterShardIndex(42))] = map[string]string{
		"like_count":     "1",
		"favorite_count": "1",
	}
	redisClient.hashes[interactionStatCounterShardKey(videoID, interactionStatCounterShardIndex(43))] = map[string]string{
		"like_count": "-1",
	}
	redisClient.hashes[interactionStatCounterShardKey(videoID, interactionStatCounterShardIndex(44))] = map[string]string{
		"like_count": "1",
	}

	stat, err := actionStat(ctx, redisClient, interactionStatCounterBaseKey(videoID), interactionStatCounterShardKeys(videoID), feedStatKey(videoID), videoID, nil)
	if err != nil {
		t.Fatalf("actionStat: %v", err)
	}
	if stat.LikeCount != 11 || stat.FavoriteCount != 5 || stat.CommentCount != 3 {
		t.Fatalf("unexpected stat: %+v", stat)
	}
}

func TestActionStatFallsBackToInitialStat(t *testing.T) {
	ctx := context.Background()
	videoID := int64(1002)
	redisClient := newActionStatFakeRedis()
	initial := &domaininteraction.VideoStat{
		VideoID:       videoID,
		LikeCount:     7,
		CommentCount:  2,
		FavoriteCount: 1,
	}
	redisClient.hashes[interactionStatCounterShardKey(videoID, interactionStatCounterShardIndex(42))] = map[string]string{
		"like_count":     "1",
		"favorite_count": "-1",
	}

	stat, err := actionStat(ctx, redisClient, interactionStatCounterBaseKey(videoID), interactionStatCounterShardKeys(videoID), feedStatKey(videoID), videoID, initial)
	if err != nil {
		t.Fatalf("actionStat: %v", err)
	}
	if stat.LikeCount != 8 || stat.FavoriteCount != 0 || stat.CommentCount != 2 {
		t.Fatalf("unexpected stat: %+v", stat)
	}
}

func TestGetStatsReadsShardedCountersOnJSONMiss(t *testing.T) {
	ctx := context.Background()
	videoID := int64(1003)
	redisClient := newActionStatFakeRedis()
	redisClient.hashes[interactionStatCounterBaseKey(videoID)] = map[string]string{
		"like_count":     "2",
		"comment_count":  "1",
		"favorite_count": "0",
	}
	redisClient.hashes[interactionStatCounterShardKey(videoID, interactionStatCounterShardIndex(42))] = map[string]string{
		"like_count":     "1",
		"favorite_count": "1",
	}
	stats, err := getStats(ctx, redisClient, []int64{videoID})
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	stat := stats[videoID]
	if stat == nil || stat.LikeCount != 3 || stat.FavoriteCount != 1 || stat.CommentCount != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if _, ok := redisClient.values[feedStatKey(videoID)]; !ok {
		t.Fatalf("expected sharded stat to be written back to JSON cache")
	}
}

func TestSetVideoStatWritesJSONCache(t *testing.T) {
	ctx := context.Background()
	videoID := int64(1005)
	redisClient := newActionStatFakeRedis()

	err := setActionStatJSON(ctx, redisClient, feedStatKey(videoID), videoStatToFeedStat(&domaininteraction.VideoStat{
		VideoID:       videoID,
		LikeCount:     2,
		CommentCount:  3,
		FavoriteCount: 1,
	}))
	if err != nil {
		t.Fatalf("SetVideoStat: %v", err)
	}

	stats, err := getStats(ctx, redisClient, []int64{videoID})
	if err != nil {
		t.Fatalf("getStats: %v", err)
	}
	stat := stats[videoID]
	if stat == nil || stat.LikeCount != 2 || stat.CommentCount != 3 || stat.FavoriteCount != 1 {
		t.Fatalf("unexpected stat: %+v", stat)
	}
}

func TestActionStatBaseInitUsesInitialStat(t *testing.T) {
	videoID := int64(1004)
	initial := &domaininteraction.VideoStat{
		VideoID:       videoID,
		LikeCount:     1,
		CommentCount:  1,
		FavoriteCount: 1,
	}

	stat := actionStatBaseInit(videoID, initial)
	if stat != initial {
		t.Fatalf("unexpected stat: %+v", stat)
	}
}
