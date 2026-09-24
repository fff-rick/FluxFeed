package infracache

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkLocalCacheGetParallel(b *testing.B) {
	cache := newLocalCache(1024)
	keys := make([]string, 1024)
	for index := range keys {
		keys[index] = "feed:page:benchmark:" + strconv.Itoa(index)
		cache.set(keys[index], []byte("cached-feed-page"), time.Minute)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		index := 0
		for pb.Next() {
			_, _ = cache.get(keys[index%len(keys)])
			index++
		}
	})
}

func BenchmarkSeenOffsets(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		_ = seenOffsets(int64(index + 1))
	}
}
