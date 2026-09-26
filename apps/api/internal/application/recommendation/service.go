package applicationrecommendation

import (
	domainembedding "FluxFeed/internal/domain/embedding"
	domainrecommendation "FluxFeed/internal/domain/recommendation"
	inframetrics "FluxFeed/internal/infra/metrics"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

const defaultLimit = 10
const recallPerSource = 100
const interestRecallPoolSize = 500
const preRankLimit = 200
const defaultRankLimit = 50

var ErrLoadRecommendationFailed = errors.New("failed to load recommendations")
var ErrLoadExposureDecisionsFailed = errors.New("failed to load exposure decisions")
var ErrSaveRecommendationExposureFailed = errors.New("failed to save recommendation exposure")

type Service struct {
	repo domainrecommendation.Repository
	now  func() time.Time
}

type Option func(*Service)

type CandidateRequest struct {
	UserID    int64
	Scene     string
	RequestID string
	Cursor    string
	Limit     int
}

type CandidateResult struct {
	UserID     int64
	Scene      string
	RequestID  string
	Candidates []*domainrecommendation.Candidate
	NextCursor string
	HasMore    bool
}

type ExposureInput struct {
	UserID    int64
	VideoID   int64
	Scene     string
	RequestID string
}

type ExposureDecisionInput struct {
	UserID    int64
	Scene     string
	RequestID string
	VideoIDs  []int64
}

type ExposureDecisionResult struct {
	UserID    int64
	Scene     string
	RequestID string
	Decisions []*domainrecommendation.ExposureDecision
}

type ExposureResult struct {
	Exposures []*domainrecommendation.Exposure
}

type cursorPayload struct {
	RankScore   float64 `json:"rank_score"`
	PublishedAt string  `json:"published_at"`
	VideoID     int64   `json:"video_id"`
}

func New(repo domainrecommendation.Repository, options ...Option) *Service {
	service := &Service{
		repo: repo,
		now:  func() time.Time { return time.Now().UTC().Truncate(time.Minute) },
	}
	for _, option := range options {
		option(service)
	}
	return service
}

func WithNow(now func() time.Time) Option {
	return func(s *Service) {
		if now != nil {
			s.now = now
		}
	}
}

func (s *Service) Recommend(ctx context.Context, input CandidateRequest) (*CandidateResult, error) {
	limit := normalizeLimit(input.Limit)
	cursor, err := parseCursor(input.Cursor)
	if err != nil {
		return nil, err
	}
	req, err := domainrecommendation.NewCandidateRequest(input.UserID, input.Scene, input.RequestID, cursor, limit)
	if err != nil {
		return nil, err
	}

	pool, err := s.recallCandidates(ctx, req.UserID)
	if err != nil {
		return nil, ErrLoadRecommendationFailed
	}
	pool = preRankCandidates(pool, s.now(), preRankLimit)

	ranked, err := s.rankCandidates(ctx, req.UserID, pool)
	if err != nil {
		return nil, ErrLoadRecommendationFailed
	}
	rankLimit := max(defaultRankLimit, limit+1)
	if len(ranked) > rankLimit {
		ranked = ranked[:rankLimit]
	}
	ranked = filterByCursor(ranked, req.Cursor)

	hasMore := len(ranked) > limit
	if hasMore {
		ranked = ranked[:limit]
	}

	nextCursor := ""
	if len(ranked) > 0 {
		nextCursor = encodeCursor(&domainrecommendation.Cursor{
			RankScore:   ranked[len(ranked)-1].RankScore,
			PublishedAt: ranked[len(ranked)-1].PublishedAt,
			VideoID:     ranked[len(ranked)-1].VideoID,
		})
	}
	ranked = interleaveByAuthor(ranked)

	return &CandidateResult{
		UserID:     req.UserID,
		Scene:      req.Scene,
		RequestID:  req.RequestID,
		Candidates: ranked,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (s *Service) recallCandidates(ctx context.Context, userID int64) ([]*domainrecommendation.Candidate, error) {
	sources := []string{
		domainrecommendation.RecallSourceHot,
		domainrecommendation.RecallSourceLatest,
		domainrecommendation.RecallSourceFollowing,
		domainrecommendation.RecallSourceInterest,
		domainrecommendation.RecallSourceSimilar,
		domainrecommendation.RecallSourceCollaborative,
	}
	type recallResult struct {
		candidates []*domainrecommendation.Candidate
		err        error
	}
	results := make([]recallResult, len(sources))
	group, groupCtx := errgroup.WithContext(ctx)
	for index, source := range sources {
		index, source := index, source
		group.Go(func() error {
			startedAt := time.Now()
			limit := recallPerSource
			if source == domainrecommendation.RecallSourceInterest || source == domainrecommendation.RecallSourceSimilar {
				limit = interestRecallPoolSize
			}
			candidates, err := s.repo.ListCandidatesBySource(groupCtx, userID, source, limit)
			if err == nil && source == domainrecommendation.RecallSourceInterest {
				candidates, err = s.personalizeInterestRecall(groupCtx, userID, candidates)
			}
			if err == nil && source == domainrecommendation.RecallSourceSimilar {
				candidates, err = s.personalizeSimilarRecall(groupCtx, userID, candidates)
			}
			results[index] = recallResult{candidates: candidates, err: err}
			inframetrics.ObserveRecommendationRecall(source, len(candidates), time.Since(startedAt), err)
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pools := make([][]*domainrecommendation.Candidate, len(results))
	var firstErr error
	successes := 0
	for index, result := range results {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		successes++
		pools[index] = result.candidates
	}
	if successes == 0 {
		return nil, firstErr
	}
	if firstErr != nil {
		inframetrics.ObserveGovernance("recommendation", "partial_recall")
	}
	return mergeRecallPools(pools), nil
}

func (s *Service) personalizeInterestRecall(ctx context.Context, userID int64, candidates []*domainrecommendation.Candidate) ([]*domainrecommendation.Candidate, error) {
	if len(candidates) == 0 {
		return candidates, nil
	}
	userVector, ok, err := s.repo.LoadUserInterestVector(ctx, userID)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		inframetrics.ObserveGovernance("recommendation", "default_profile")
		return candidates[:min(len(candidates), recallPerSource)], nil
	}
	if !ok {
		return candidates[:min(len(candidates), recallPerSource)], nil
	}
	videoIDs := make([]int64, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate != nil {
			videoIDs = append(videoIDs, candidate.VideoID)
		}
	}
	vectors, err := s.repo.LoadVideoVectors(ctx, videoIDs)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		inframetrics.ObserveGovernance("recommendation", "interest_vector_fallback")
		return candidates[:min(len(candidates), recallPerSource)], nil
	}
	return selectBySimilarity(userVector, candidates, vectors, 0, recallPerSource), nil
}

func (s *Service) personalizeSimilarRecall(ctx context.Context, userID int64, candidates []*domainrecommendation.Candidate) ([]*domainrecommendation.Candidate, error) {
	if len(candidates) == 0 {
		return candidates, nil
	}
	seedIDs, err := s.repo.ListRecentPositiveVideoIDs(ctx, userID, 1)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		inframetrics.ObserveGovernance("recommendation", "similar_seed_fallback")
		return []*domainrecommendation.Candidate{}, nil
	}
	if len(seedIDs) == 0 {
		return []*domainrecommendation.Candidate{}, nil
	}
	videoIDs := make([]int64, 0, len(candidates)+1)
	videoIDs = append(videoIDs, seedIDs[0])
	for _, candidate := range candidates {
		if candidate != nil && candidate.VideoID != seedIDs[0] {
			videoIDs = append(videoIDs, candidate.VideoID)
		}
	}
	vectors, err := s.repo.LoadVideoVectors(ctx, videoIDs)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		inframetrics.ObserveGovernance("recommendation", "similar_vector_fallback")
		return []*domainrecommendation.Candidate{}, nil
	}
	return selectBySimilarity(vectors[seedIDs[0]], candidates, vectors, seedIDs[0], recallPerSource), nil
}

func selectBySimilarity(reference []float64, candidates []*domainrecommendation.Candidate, vectors map[int64][]float64, excludedVideoID int64, limit int) []*domainrecommendation.Candidate {
	selected := make([]*domainrecommendation.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil || candidate.VideoID == excludedVideoID {
			continue
		}
		similarity, err := domainembedding.CosineSimilarity(reference, vectors[candidate.VideoID])
		if err != nil {
			continue
		}
		candidate.Similarity = similarity
		candidate.RankScore = similarity
		selected = append(selected, candidate)
	}
	sortCandidates(selected)
	return selected[:min(len(selected), limit)]
}

func mergeRecallPools(pools [][]*domainrecommendation.Candidate) []*domainrecommendation.Candidate {
	merged := make([]*domainrecommendation.Candidate, 0)
	byVideoID := make(map[int64]*domainrecommendation.Candidate)
	for _, pool := range pools {
		for _, candidate := range pool {
			if candidate == nil || candidate.VideoID <= 0 {
				continue
			}
			if existing := byVideoID[candidate.VideoID]; existing != nil {
				if candidate.Similarity > existing.Similarity {
					existing.Similarity = candidate.Similarity
					if existing.Reason != domainrecommendation.RecallSourceFollowing {
						existing.Reason = candidate.Reason
					}
				}
				if candidate.Reason == domainrecommendation.RecallSourceFollowing {
					existing.Reason = candidate.Reason
				} else if existing.Reason != domainrecommendation.RecallSourceFollowing && candidate.Reason == domainrecommendation.RecallSourceCollaborative {
					existing.Reason = candidate.Reason
				} else if existing.Reason != domainrecommendation.RecallSourceFollowing && candidate.Reason == domainrecommendation.RecallSourceInterest {
					existing.Reason = candidate.Reason
				}
				continue
			}
			value := *candidate
			byVideoID[value.VideoID] = &value
			merged = append(merged, &value)
		}
	}
	return merged
}

func preRankCandidates(candidates []*domainrecommendation.Candidate, now time.Time, limit int) []*domainrecommendation.Candidate {
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		candidate.FreshnessScore = freshnessScore(now, candidate.PublishedAt)
		candidate.RankScore = rankScore(0, candidate.HotScore, candidate.FreshnessScore, false)
		candidate.RankScore += max(candidate.Similarity, 0) * 0.50
		if candidate.Reason == domainrecommendation.RecallSourceFollowing {
			candidate.RankScore += 0.15
		} else if candidate.Reason == domainrecommendation.RecallSourceCollaborative {
			candidate.RankScore += 0.10
		}
	}
	sortCandidates(candidates)
	if limit > 0 && len(candidates) > limit {
		return candidates[:limit]
	}
	return candidates
}

func (s *Service) DecideExposures(ctx context.Context, input ExposureDecisionInput) (*ExposureDecisionResult, error) {
	req, err := domainrecommendation.NewExposureDecisionRequest(input.UserID, input.Scene, input.RequestID, input.VideoIDs)
	if err != nil {
		return nil, err
	}
	if len(req.VideoIDs) == 0 {
		return &ExposureDecisionResult{
			UserID:    req.UserID,
			Scene:     req.Scene,
			RequestID: req.RequestID,
			Decisions: []*domainrecommendation.ExposureDecision{},
		}, nil
	}

	exposures, err := s.repo.ListRecentExposures(ctx, req.UserID, req.VideoIDs, s.now().Add(-domainrecommendation.RecentExposureWindow))
	if err != nil {
		return nil, ErrLoadExposureDecisionsFailed
	}
	exposureByVideoID := make(map[int64]*domainrecommendation.Exposure, len(exposures))
	for _, exposure := range exposures {
		if exposure != nil {
			exposureByVideoID[exposure.VideoID] = exposure
		}
	}

	decisions := make([]*domainrecommendation.ExposureDecision, 0, len(req.VideoIDs))
	for _, videoID := range req.VideoIDs {
		if exposure := exposureByVideoID[videoID]; exposure != nil {
			decisions = append(decisions, domainrecommendation.RestoreExposureDecision(
				videoID,
				false,
				domainrecommendation.ExposureDecisionReasonRecentlyExposed,
				&exposure.LastExposedAt,
			))
			continue
		}
		decisions = append(decisions, domainrecommendation.RestoreExposureDecision(
			videoID,
			true,
			domainrecommendation.ExposureDecisionReasonFresh,
			nil,
		))
	}
	return &ExposureDecisionResult{
		UserID:    req.UserID,
		Scene:     req.Scene,
		RequestID: req.RequestID,
		Decisions: decisions,
	}, nil
}

func (s *Service) SaveExposures(ctx context.Context, inputs []ExposureInput) (*ExposureResult, error) {
	writes := make([]*domainrecommendation.ExposureWrite, 0, len(inputs))
	seen := map[int64]struct{}{}
	for _, input := range inputs {
		write, err := domainrecommendation.NewExposureWrite(input.UserID, input.VideoID, input.Scene, input.RequestID)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[write.VideoID]; exists {
			continue
		}
		seen[write.VideoID] = struct{}{}
		writes = append(writes, write)
	}
	if len(writes) == 0 {
		return &ExposureResult{Exposures: []*domainrecommendation.Exposure{}}, nil
	}

	exposures, err := s.repo.SaveExposures(ctx, writes)
	if err != nil {
		if errors.Is(err, domainrecommendation.ErrVideoNotFound) {
			return nil, err
		}
		return nil, ErrSaveRecommendationExposureFailed
	}
	return &ExposureResult{Exposures: exposures}, nil
}

func (s *Service) rankCandidates(ctx context.Context, userID int64, pool []*domainrecommendation.Candidate) ([]*domainrecommendation.Candidate, error) {
	if len(pool) == 0 {
		return []*domainrecommendation.Candidate{}, nil
	}

	videoIDs := make([]int64, 0, len(pool))
	for _, candidate := range pool {
		if candidate != nil && candidate.VideoID > 0 {
			videoIDs = append(videoIDs, candidate.VideoID)
		}
	}
	vectors, err := s.repo.LoadVideoVectors(ctx, videoIDs)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		inframetrics.ObserveGovernance("recommendation", "rank_vector_fallback")
		vectors = map[int64][]float64{}
	}
	userVector, hasUserVector, err := s.repo.LoadUserInterestVector(ctx, userID)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		inframetrics.ObserveGovernance("recommendation", "default_profile")
		userVector = nil
		hasUserVector = false
	}

	now := s.now()
	ranked := make([]*domainrecommendation.Candidate, 0, len(pool))
	for _, candidate := range pool {
		if candidate == nil {
			continue
		}
		value := *candidate
		value.FreshnessScore = freshnessScore(now, value.PublishedAt)
		if hasUserVector {
			value.Similarity = 0
			if vector := vectors[value.VideoID]; len(vector) > 0 {
				similarity, err := domainembedding.CosineSimilarity(userVector, vector)
				if err == nil {
					value.Similarity = similarity
				}
			}
		}
		value.RankScore = rankScore(value.Similarity, value.HotScore, value.FreshnessScore, hasUserVector)
		value.Reason = recommendationReason(hasUserVector, value.Similarity, value.HotScore, value.Reason)
		ranked = append(ranked, &value)
	}

	sortCandidates(ranked)
	return ranked, nil
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return defaultLimit
	}
	if limit > domainrecommendation.MaxLimit {
		return domainrecommendation.MaxLimit
	}
	return limit
}

func rankScore(similarity float64, hotScore int, freshness float64, hasUserVector bool) float64 {
	hot := math.Log1p(float64(maxInt(hotScore, 0))) / 10
	if hasUserVector {
		return similarity*0.70 + hot*0.20 + freshness*0.10
	}
	return hot*0.65 + freshness*0.35
}

func freshnessScore(now time.Time, publishedAt time.Time) float64 {
	if publishedAt.IsZero() {
		return 0
	}
	hours := now.Sub(publishedAt).Hours()
	if hours < 0 {
		hours = 0
	}
	return 1 / (1 + hours/72)
}

func recommendationReason(hasUserVector bool, similarity float64, hotScore int, recallSource string) string {
	if recallSource == domainrecommendation.RecallSourceSimilar && similarity > 0.05 {
		return domainrecommendation.RecallSourceSimilar
	}
	if hasUserVector && similarity > 0.05 {
		return "interest_match"
	}
	if recallSource == domainrecommendation.RecallSourceFollowing {
		return domainrecommendation.RecallSourceFollowing
	}
	if recallSource == domainrecommendation.RecallSourceCollaborative {
		return domainrecommendation.RecallSourceCollaborative
	}
	if hotScore > 0 {
		return "hot"
	}
	return "fresh"
}

func sortCandidates(candidates []*domainrecommendation.Candidate) {
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if left.RankScore != right.RankScore {
			return left.RankScore > right.RankScore
		}
		if !left.PublishedAt.Equal(right.PublishedAt) {
			return left.PublishedAt.After(right.PublishedAt)
		}
		return left.VideoID > right.VideoID
	})
}

func interleaveByAuthor(candidates []*domainrecommendation.Candidate) []*domainrecommendation.Candidate {
	if len(candidates) <= 2 {
		return candidates
	}
	output := make([]*domainrecommendation.Candidate, 0, len(candidates))
	delayed := make([]*domainrecommendation.Candidate, 0)
	var previousAuthorID int64
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if previousAuthorID != 0 && candidate.AuthorID == previousAuthorID {
			delayed = append(delayed, candidate)
			continue
		}
		output = append(output, candidate)
		previousAuthorID = candidate.AuthorID
	}
	for len(delayed) > 0 {
		progressed := false
		remaining := make([]*domainrecommendation.Candidate, 0)
		for _, candidate := range delayed {
			if len(output) > 0 && output[len(output)-1].AuthorID == candidate.AuthorID {
				remaining = append(remaining, candidate)
				continue
			}
			output = append(output, candidate)
			progressed = true
		}
		if !progressed {
			output = append(output, remaining...)
			break
		}
		delayed = remaining
	}
	return output
}

func filterByCursor(candidates []*domainrecommendation.Candidate, cursor *domainrecommendation.Cursor) []*domainrecommendation.Candidate {
	if cursor == nil {
		return candidates
	}
	filtered := make([]*domainrecommendation.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if candidate.RankScore < cursor.RankScore ||
			(sameScore(candidate.RankScore, cursor.RankScore) && candidate.PublishedAt.Before(cursor.PublishedAt)) ||
			(sameScore(candidate.RankScore, cursor.RankScore) && candidate.PublishedAt.Equal(cursor.PublishedAt) && candidate.VideoID < cursor.VideoID) {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func sameScore(left float64, right float64) bool {
	return math.Abs(left-right) < 0.000000001
}

func parseCursor(raw string) (*domainrecommendation.Cursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	content, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		content, err = base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return nil, domainrecommendation.ErrInvalidCursor
		}
	}
	var payload cursorPayload
	if err := json.Unmarshal(content, &payload); err != nil {
		return nil, domainrecommendation.ErrInvalidCursor
	}
	publishedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(payload.PublishedAt))
	if err != nil {
		return nil, domainrecommendation.ErrInvalidCursor
	}
	cursor := &domainrecommendation.Cursor{
		RankScore:   payload.RankScore,
		PublishedAt: publishedAt,
		VideoID:     payload.VideoID,
	}
	if !cursor.Valid() {
		return nil, domainrecommendation.ErrInvalidCursor
	}
	return cursor, nil
}

func encodeCursor(cursor *domainrecommendation.Cursor) string {
	if cursor == nil || !cursor.Valid() {
		return ""
	}
	content, err := json.Marshal(cursorPayload{
		RankScore:   cursor.RankScore,
		PublishedAt: cursor.PublishedAt.UTC().Format(time.RFC3339Nano),
		VideoID:     cursor.VideoID,
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(content)
}

func maxInt(left int, right int) int {
	if left > right {
		return left
	}
	return right
}
