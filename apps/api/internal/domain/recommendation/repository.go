package domainrecommendation

import (
	"context"
	"time"
)

type Repository interface {
	ListCandidatesBySource(ctx context.Context, userID int64, source string, limit int) ([]*Candidate, error)
	ListRecentPositiveVideoIDs(ctx context.Context, userID int64, limit int) ([]int64, error)
	LoadUserInterestVector(ctx context.Context, userID int64) ([]float64, bool, error)
	LoadVideoVectors(ctx context.Context, videoIDs []int64) (map[int64][]float64, error)
	ListRecentExposures(ctx context.Context, userID int64, videoIDs []int64, since time.Time) ([]*Exposure, error)
	SaveExposures(ctx context.Context, exposures []*ExposureWrite) ([]*Exposure, error)
}
