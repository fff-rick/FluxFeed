package applicationrecommendation

import (
	domainrecommendation "FluxFeed/internal/domain/recommendation"
	"testing"
	"time"
)

func TestMergeAndPreRankRecallCandidates(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	pools := [][]*domainrecommendation.Candidate{
		{
			domainrecommendation.RestoreCandidate(1, 10, 0, 0, 100, 0, domainrecommendation.RecallSourceHot, now.Add(-24*time.Hour)),
			domainrecommendation.RestoreCandidate(2, 20, 0, 0, 1, 0, domainrecommendation.RecallSourceLatest, now),
		},
		{
			domainrecommendation.RestoreCandidate(1, 10, 0, 0, 100, 0, domainrecommendation.RecallSourceFollowing, now.Add(-24*time.Hour)),
		},
	}

	merged := mergeRecallPools(pools)
	if len(merged) != 2 || merged[0].Reason != domainrecommendation.RecallSourceFollowing {
		t.Fatalf("unexpected merged candidates: %+v", merged)
	}
	ranked := preRankCandidates(merged, now, 1)
	if len(ranked) != 1 || ranked[0].VideoID != 1 {
		t.Fatalf("unexpected pre-rank result: %+v", ranked)
	}
}

func TestSelectBySimilarityExcludesSeedAndRanks(t *testing.T) {
	candidates := []*domainrecommendation.Candidate{
		domainrecommendation.RestoreCandidate(1, 10, 0, 0, 0, 0, domainrecommendation.RecallSourceSimilar, time.Now()),
		domainrecommendation.RestoreCandidate(2, 20, 0, 0, 0, 0, domainrecommendation.RecallSourceSimilar, time.Now()),
		domainrecommendation.RestoreCandidate(3, 30, 0, 0, 0, 0, domainrecommendation.RecallSourceSimilar, time.Now()),
	}
	vectors := map[int64][]float64{1: {1, 0}, 2: {0.9, 0.1}, 3: {0, 1}}
	got := selectBySimilarity(vectors[1], candidates, vectors, 1, 1)
	if len(got) != 1 || got[0].VideoID != 2 || got[0].Similarity <= 0.9 {
		t.Fatalf("unexpected similar recall: %+v", got)
	}
}
