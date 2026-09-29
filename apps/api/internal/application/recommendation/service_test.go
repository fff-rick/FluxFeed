package applicationrecommendation

import (
	domainrecommendation "FluxFeed/internal/domain/recommendation"
	"context"
	"errors"
	"testing"
	"time"
)

type degradedRecommendationRepo struct {
	profileErr      error
	vectorErr       error
	candidates      map[string][]*domainrecommendation.Candidate
	candidateErrors map[string]error
	exposures       []*domainrecommendation.Exposure
	exclusions      *domainrecommendation.CandidateExclusions
	limits          chan int
	exposureCalls   *int
	exclusionCalls  *int
	hotCalls        *int
}

func (r degradedRecommendationRepo) ListCandidatesBySource(_ context.Context, _ int64, source string, limit int) ([]*domainrecommendation.Candidate, error) {
	if source == domainrecommendation.RecallSourceHot && r.hotCalls != nil {
		*r.hotCalls++
	}
	if r.limits != nil {
		r.limits <- limit
	}
	return r.candidates[source], r.candidateErrors[source]
}

func TestHotRecallCandidatesAreCachedAndCloned(t *testing.T) {
	hotCalls := 0
	repo := degradedRecommendationRepo{
		candidates: map[string][]*domainrecommendation.Candidate{
			domainrecommendation.RecallSourceHot: {
				domainrecommendation.RestoreCandidate(1, 10, 0, 0, 100, 0, domainrecommendation.RecallSourceHot, time.Now()),
			},
		},
		hotCalls: &hotCalls,
	}
	service := New(repo)

	first, err := service.listCandidatesBySource(context.Background(), 1, domainrecommendation.RecallSourceHot, 200)
	if err != nil || len(first) != 1 {
		t.Fatalf("unexpected first hot recall: %+v, err=%v", first, err)
	}
	first[0].HotScore = 0
	second, err := service.listCandidatesBySource(context.Background(), 2, domainrecommendation.RecallSourceHot, 200)
	if err != nil || len(second) != 1 || second[0].HotScore != 100 {
		t.Fatalf("unexpected cached hot recall: %+v, err=%v", second, err)
	}
	if hotCalls != 1 {
		t.Fatalf("expected one hot repository query, got %d", hotCalls)
	}
}
func (r degradedRecommendationRepo) ListRecentPositiveVideoIDs(context.Context, int64, int) ([]int64, error) {
	return nil, nil
}
func (r degradedRecommendationRepo) ListNegativeCandidateExclusions(context.Context, int64, []int64, []int64, time.Time) (*domainrecommendation.CandidateExclusions, error) {
	if r.exclusionCalls != nil {
		*r.exclusionCalls++
	}
	if r.exclusions != nil {
		return r.exclusions, nil
	}
	return &domainrecommendation.CandidateExclusions{}, nil
}
func (r degradedRecommendationRepo) LoadUserInterestVector(context.Context, int64) ([]float64, bool, error) {
	return nil, false, r.profileErr
}
func (r degradedRecommendationRepo) LoadVideoVectors(context.Context, []int64) (map[int64][]float64, error) {
	return nil, r.vectorErr
}
func (r degradedRecommendationRepo) ListRecentExposures(context.Context, int64, []int64, time.Time) ([]*domainrecommendation.Exposure, error) {
	if r.exposureCalls != nil {
		*r.exposureCalls++
	}
	return r.exposures, nil
}
func (r degradedRecommendationRepo) SaveExposures(context.Context, []*domainrecommendation.ExposureWrite) ([]*domainrecommendation.Exposure, error) {
	return nil, nil
}

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

func TestRankCandidatesFallsBackToHotAndFreshness(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	service := New(degradedRecommendationRepo{profileErr: errors.New("profile unavailable"), vectorErr: errors.New("vectors unavailable")}, WithNow(func() time.Time { return now }))
	candidates := []*domainrecommendation.Candidate{
		domainrecommendation.RestoreCandidate(1, 10, 0, 0, 100, 0, domainrecommendation.RecallSourceHot, now.Add(-time.Hour)),
		domainrecommendation.RestoreCandidate(2, 20, 0, 0, 0, 0, domainrecommendation.RecallSourceLatest, now),
	}
	ranked, err := service.rankCandidates(context.Background(), 42, candidates)
	if err != nil || len(ranked) != 2 || ranked[0].VideoID != 1 || ranked[0].Similarity != 0 {
		t.Fatalf("unexpected fallback ranking: %+v, err=%v", ranked, err)
	}
}

func TestInterestRecallUsesDefaultProfileOnFailure(t *testing.T) {
	service := New(degradedRecommendationRepo{profileErr: errors.New("profile unavailable")})
	candidates := []*domainrecommendation.Candidate{
		domainrecommendation.RestoreCandidate(1, 10, 0, 0, 0, 0, domainrecommendation.RecallSourceInterest, time.Now()),
	}
	got, err := service.personalizeInterestRecall(context.Background(), 42, candidates)
	if err != nil || len(got) != 1 || got[0].VideoID != 1 {
		t.Fatalf("unexpected default-profile recall: %+v, err=%v", got, err)
	}
}

func TestRecallCandidatesKeepsHealthySources(t *testing.T) {
	now := time.Now()
	repo := degradedRecommendationRepo{
		candidates: map[string][]*domainrecommendation.Candidate{
			domainrecommendation.RecallSourceHot: {
				domainrecommendation.RestoreCandidate(1, 10, 0, 0, 20, 0, domainrecommendation.RecallSourceHot, now),
			},
		},
		candidateErrors: map[string]error{
			domainrecommendation.RecallSourceCollaborative: errors.New("collaborative unavailable"),
		},
	}
	got, err := New(repo).recallCandidates(context.Background(), 42)
	if err != nil || len(got) != 1 || got[0].VideoID != 1 {
		t.Fatalf("healthy recall source was discarded: %+v, err=%v", got, err)
	}
}

func TestRecallCandidatesFiltersExposureNegativeVideoAndHiddenAuthor(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	limits := make(chan int, 6)
	exposureCalls, exclusionCalls := 0, 0
	repo := degradedRecommendationRepo{
		candidates: map[string][]*domainrecommendation.Candidate{
			domainrecommendation.RecallSourceHot: {
				domainrecommendation.RestoreCandidate(1, 10, 0, 0, 40, 0, domainrecommendation.RecallSourceHot, now),
				domainrecommendation.RestoreCandidate(2, 20, 0, 0, 30, 0, domainrecommendation.RecallSourceHot, now),
				domainrecommendation.RestoreCandidate(3, 30, 0, 0, 20, 0, domainrecommendation.RecallSourceHot, now),
				domainrecommendation.RestoreCandidate(4, 40, 0, 0, 10, 0, domainrecommendation.RecallSourceHot, now),
			},
		},
		exposures: []*domainrecommendation.Exposure{
			domainrecommendation.RestoreExposure(1, 42, 1, now, now, 1, "recommend"),
		},
		exclusions:     &domainrecommendation.CandidateExclusions{VideoIDs: []int64{2}, AuthorIDs: []int64{30}},
		limits:         limits,
		exposureCalls:  &exposureCalls,
		exclusionCalls: &exclusionCalls,
	}

	got, err := New(repo, WithNow(func() time.Time { return now })).recallCandidates(context.Background(), 42)
	if err != nil || len(got) != 1 || got[0].VideoID != 4 {
		t.Fatalf("unexpected filtered recall candidates: %+v, err=%v", got, err)
	}
	if exposureCalls != 1 || exclusionCalls != 1 {
		t.Fatalf("expected one batch filter query each, exposures=%d exclusions=%d", exposureCalls, exclusionCalls)
	}
	close(limits)
	for limit := range limits {
		if limit != recallPerSource*candidateOverfetchFactor {
			t.Fatalf("unexpected bounded candidate limit: %d", limit)
		}
	}
}
