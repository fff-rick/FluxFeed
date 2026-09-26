package infrarecommendation

import (
	domainembedding "FluxFeed/internal/domain/embedding"
	domainexposure "FluxFeed/internal/domain/exposure"
	domaininteraction "FluxFeed/internal/domain/interaction"
	domainrecommendation "FluxFeed/internal/domain/recommendation"
	domainrelation "FluxFeed/internal/domain/relation"
	domainvideo "FluxFeed/internal/domain/video"
	infraexposure "FluxFeed/internal/infra/persistence/exposure"
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const hotScoreExpression = "COALESCE(vs.like_count, 0) * 3 + COALESCE(vs.comment_count, 0) * 5 + COALESCE(vs.favorite_count, 0) * 4"
const positiveEventWindow = 30 * 24 * time.Hour
const profileSignalLimit = 200

type Repository struct {
	db *gorm.DB
}

type candidateModel struct {
	VideoID     int64
	AuthorID    int64
	HotScore    int
	PublishedAt time.Time
}

type videoVectorModel struct {
	VideoID       int64
	EmbeddingJSON string
}

func New(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) ListCandidatesBySource(ctx context.Context, userID int64, source string, limit int) ([]*domainrecommendation.Candidate, error) {
	if limit <= 0 {
		return []*domainrecommendation.Candidate{}, nil
	}

	var models []candidateModel
	query := r.db.WithContext(ctx).
		Table("video AS v").
		Select("v.id AS video_id, v.author_id, ("+hotScoreExpression+") AS hot_score, v.published_at").
		Joins("LEFT JOIN video_stat AS vs ON vs.video_id = v.id").
		Joins(
			"LEFT JOIN exposures AS e ON e.user_id = ? AND e.video_id = v.id AND e.last_exposed_at >= ?",
			userID,
			time.Now().Add(-domainrecommendation.RecentExposureWindow),
		).
		Where("v.status = ? AND v.published_at IS NOT NULL AND e.video_id IS NULL", domainvideo.StatusPublished)
	switch source {
	case domainrecommendation.RecallSourceHot:
		query = query.Order("hot_score DESC").Order("v.published_at DESC")
	case domainrecommendation.RecallSourceLatest:
		query = query.Order("v.published_at DESC")
	case domainrecommendation.RecallSourceFollowing:
		query = query.Joins("JOIN user_follow AS f ON f.target_user_id = v.author_id AND f.user_id = ? AND f.status = ?", userID, domainrelation.FollowStatusActive).
			Order("v.published_at DESC")
	case domainrecommendation.RecallSourceInterest, domainrecommendation.RecallSourceSimilar:
		// ponytail: 应用层对最多 500 个向量做余弦召回；视频规模需要 ANN 时替换这里。
		query = query.Joins("JOIN video_embedding AS ve ON ve.video_id = v.id AND ve.model = ?", domainembedding.HashNgramModel).
			Order("v.published_at DESC")
	case domainrecommendation.RecallSourceCollaborative:
		// ponytail: 30 天窗口内在线聚合共同观看用户；数据量增大后物化 item-item 共现表。
		query = query.Joins(`JOIN (
			SELECT candidate.video_id, COUNT(DISTINCT peer.user_id) AS affinity
			FROM video_view_events AS mine
			JOIN video_view_events AS peer
			  ON peer.video_id = mine.video_id AND peer.user_id <> mine.user_id
			  AND peer.created_at >= ? AND peer.event_type IN (?, ?)
			JOIN video_view_events AS candidate
			  ON candidate.user_id = peer.user_id AND candidate.video_id <> mine.video_id
			  AND candidate.created_at >= ? AND candidate.event_type IN (?, ?)
			WHERE mine.user_id = ? AND mine.created_at >= ? AND mine.event_type IN (?, ?)
			GROUP BY candidate.video_id
		) AS collaborative ON collaborative.video_id = v.id`,
			time.Now().Add(-positiveEventWindow), domainexposure.EventTypePlay, domainexposure.EventTypeComplete,
			time.Now().Add(-positiveEventWindow), domainexposure.EventTypePlay, domainexposure.EventTypeComplete,
			userID, time.Now().Add(-positiveEventWindow), domainexposure.EventTypePlay, domainexposure.EventTypeComplete,
		).Where("NOT EXISTS (SELECT 1 FROM video_view_events own WHERE own.user_id = ? AND own.video_id = v.id)", userID).
			Order("collaborative.affinity DESC").Order("v.published_at DESC")
	default:
		return []*domainrecommendation.Candidate{}, nil
	}
	err := query.Order("v.id DESC").Limit(limit).Scan(&models).Error
	if err != nil {
		return nil, err
	}

	candidates := make([]*domainrecommendation.Candidate, 0, len(models))
	for _, model := range models {
		candidates = append(candidates, domainrecommendation.RestoreCandidate(
			model.VideoID,
			model.AuthorID,
			0,
			0,
			model.HotScore,
			0,
			source,
			model.PublishedAt,
		))
	}
	return candidates, nil
}

func (r *Repository) ListRecentPositiveVideoIDs(ctx context.Context, userID int64, limit int) ([]int64, error) {
	if limit <= 0 {
		return []int64{}, nil
	}
	var rows []struct{ VideoID int64 }
	err := r.db.WithContext(ctx).Table("video_view_events").Select("video_id").
		Where("user_id = ? AND created_at >= ? AND event_type IN ?", userID, time.Now().Add(-positiveEventWindow), positiveEventTypes()).
		Group("video_id").Order("MAX(created_at) DESC").Limit(limit).Scan(&rows).Error
	videoIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		videoIDs = append(videoIDs, row.VideoID)
	}
	return videoIDs, err
}

func (r *Repository) LoadUserInterestVector(ctx context.Context, userID int64) ([]float64, bool, error) {
	var materialized UserInterestModel
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND model = ?", userID, domainembedding.HashNgramModel).
		Take(&materialized).Error
	if err == nil {
		vector, decodeErr := decodeVector(materialized.EmbeddingJSON)
		return vector, len(vector) > 0, decodeErr
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, err
	}
	return r.calculateUserInterestVector(ctx, userID)
}

// RefreshUserInterestVector 从权威行为数据重算用户向量，重复消费同一事件结果不变。
func (r *Repository) RefreshUserInterestVector(ctx context.Context, userID int64) error {
	vector, ok, err := r.calculateUserInterestVector(ctx, userID)
	if err != nil {
		return err
	}
	if !ok {
		return InvalidateUserInterest(r.db.WithContext(ctx), userID)
	}
	content, err := json.Marshal(vector)
	if err != nil {
		return err
	}
	model := UserInterestModel{
		UserID: userID, Model: domainembedding.HashNgramModel,
		Dimension: len(vector), EmbeddingJSON: string(content),
	}
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "model"}},
		DoUpdates: clause.AssignmentColumns([]string{"dimension", "embedding_json", "updated_at"}),
	}).Create(&model).Error
}

func (r *Repository) calculateUserInterestVector(ctx context.Context, userID int64) ([]float64, bool, error) {
	accumulator := &vectorAccumulator{}
	rows, err := r.db.WithContext(ctx).
		Table("video_view_events AS ev").
		Select("ve.embedding_json, ev.event_type, ev.watch_ms, ev.completed").
		Joins("JOIN video_embedding AS ve ON ve.video_id = ev.video_id AND ve.model = ?", domainembedding.HashNgramModel).
		Where("ev.user_id = ? AND ev.created_at >= ? AND ev.event_type IN ?", userID, time.Now().Add(-positiveEventWindow), positiveEventTypes()).
		Order("ev.created_at DESC").
		Limit(profileSignalLimit).
		Rows()
	if err != nil {
		return nil, false, err
	}
	for rows.Next() {
		var embeddingJSON string
		var eventType string
		var watchMs int
		var completed bool
		if err := rows.Scan(&embeddingJSON, &eventType, &watchMs, &completed); err != nil {
			return nil, false, err
		}
		accumulator.Add(embeddingJSON, eventWeight(eventType, watchMs, completed))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, false, err
	}
	rows.Close()

	actionRows, err := r.db.WithContext(ctx).Table("interaction_action AS ia").
		Select("ve.embedding_json, ia.action_type").
		Joins("JOIN video_embedding AS ve ON ve.video_id = ia.video_id AND ve.model = ?", domainembedding.HashNgramModel).
		Where("ia.user_id = ? AND ia.status = ? AND ia.updated_at >= ?", userID, domaininteraction.ActionStatusActive, time.Now().Add(-positiveEventWindow)).
		Order("ia.updated_at DESC").Limit(profileSignalLimit).Rows()
	if err != nil {
		return nil, false, err
	}
	for actionRows.Next() {
		var embeddingJSON, actionType string
		if err := actionRows.Scan(&embeddingJSON, &actionType); err != nil {
			actionRows.Close()
			return nil, false, err
		}
		weight := 3.0
		if actionType == domaininteraction.ActionTypeFavorite {
			weight = 4
		}
		accumulator.Add(embeddingJSON, weight)
	}
	if err := actionRows.Err(); err != nil {
		actionRows.Close()
		return nil, false, err
	}
	actionRows.Close()

	commentRows, err := r.db.WithContext(ctx).Table("interaction_comment AS c").
		Select("ve.embedding_json").
		Joins("JOIN video_embedding AS ve ON ve.video_id = c.video_id AND ve.model = ?", domainembedding.HashNgramModel).
		Where("c.user_id = ? AND c.status = ? AND c.created_at >= ?", userID, domaininteraction.CommentStatusNormal, time.Now().Add(-positiveEventWindow)).
		Order("c.created_at DESC").Limit(profileSignalLimit).Rows()
	if err != nil {
		return nil, false, err
	}
	for commentRows.Next() {
		var embeddingJSON string
		if err := commentRows.Scan(&embeddingJSON); err != nil {
			commentRows.Close()
			return nil, false, err
		}
		accumulator.Add(embeddingJSON, 5)
	}
	if err := commentRows.Err(); err != nil {
		commentRows.Close()
		return nil, false, err
	}
	commentRows.Close()

	followRows, err := r.db.WithContext(ctx).Table("user_follow AS f").
		Select("ve.embedding_json").
		Joins("JOIN video AS v ON v.author_id = f.target_user_id AND v.status = ? AND v.published_at >= ?", domainvideo.StatusPublished, time.Now().Add(-positiveEventWindow)).
		Joins("JOIN video_embedding AS ve ON ve.video_id = v.id AND ve.model = ?", domainembedding.HashNgramModel).
		Where("f.user_id = ? AND f.status = ?", userID, domainrelation.FollowStatusActive).
		Order("v.published_at DESC").Limit(20).Rows()
	if err != nil {
		return nil, false, err
	}
	for followRows.Next() {
		var embeddingJSON string
		if err := followRows.Scan(&embeddingJSON); err != nil {
			followRows.Close()
			return nil, false, err
		}
		accumulator.Add(embeddingJSON, 0.25)
	}
	if err := followRows.Err(); err != nil {
		followRows.Close()
		return nil, false, err
	}
	followRows.Close()

	return accumulator.Average()
}

type vectorAccumulator struct {
	sum         []float64
	totalWeight float64
}

func (a *vectorAccumulator) Add(content string, weight float64) {
	vector, err := decodeVector(content)
	if err != nil || len(vector) == 0 || weight <= 0 {
		return
	}
	if len(a.sum) == 0 {
		a.sum = make([]float64, len(vector))
	}
	if len(vector) != len(a.sum) {
		return
	}
	for index := range vector {
		a.sum[index] += vector[index] * weight
	}
	a.totalWeight += weight
}

func (a *vectorAccumulator) Average() ([]float64, bool, error) {
	if len(a.sum) == 0 || a.totalWeight == 0 {
		return nil, false, nil
	}
	for index := range a.sum {
		a.sum[index] /= a.totalWeight
	}
	return a.sum, true, nil
}

func InvalidateUserInterest(tx *gorm.DB, userID int64) error {
	return tx.Where("user_id = ? AND model = ?", userID, domainembedding.HashNgramModel).Delete(&UserInterestModel{}).Error
}

func (r *Repository) LoadVideoVectors(ctx context.Context, videoIDs []int64) (map[int64][]float64, error) {
	vectors := map[int64][]float64{}
	if len(videoIDs) == 0 {
		return vectors, nil
	}

	var models []videoVectorModel
	err := r.db.WithContext(ctx).
		Table("video_embedding").
		Select("video_id, embedding_json").
		Where("video_id IN ? AND model = ?", videoIDs, domainembedding.HashNgramModel).
		Scan(&models).
		Error
	if err != nil {
		return nil, err
	}
	for _, model := range models {
		vector, err := decodeVector(model.EmbeddingJSON)
		if err != nil {
			continue
		}
		vectors[model.VideoID] = vector
	}
	return vectors, nil
}

func (r *Repository) ListRecentExposures(ctx context.Context, userID int64, videoIDs []int64, since time.Time) ([]*domainrecommendation.Exposure, error) {
	if len(videoIDs) == 0 {
		return []*domainrecommendation.Exposure{}, nil
	}

	var models []infraexposure.ExposureModel
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND video_id IN ? AND last_exposed_at >= ?", userID, videoIDs, since).
		Find(&models).
		Error
	if err != nil {
		return nil, err
	}
	exposures := make([]*domainrecommendation.Exposure, 0, len(models))
	for _, model := range models {
		exposures = append(exposures, restoreExposure(model))
	}
	return exposures, nil
}

func (r *Repository) SaveExposures(ctx context.Context, writes []*domainrecommendation.ExposureWrite) ([]*domainrecommendation.Exposure, error) {
	if len(writes) == 0 {
		return []*domainrecommendation.Exposure{}, nil
	}

	exposures := make([]*domainrecommendation.Exposure, 0, len(writes))
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, write := range writes {
			if write == nil {
				continue
			}
			if err := ensurePublishedVideo(tx, write.VideoID); err != nil {
				return err
			}
			event := infraexposure.ViewEventModel{
				UserID:    write.UserID,
				VideoID:   write.VideoID,
				Scene:     write.Scene,
				RequestID: stringPtr(write.RequestID),
				EventType: domainexposure.EventTypeExposed,
				WatchMs:   0,
				Completed: false,
			}
			if err := tx.Create(&event).Error; err != nil {
				return err
			}
			model := infraexposure.ExposureModel{
				UserID:         write.UserID,
				VideoID:        write.VideoID,
				FirstExposedAt: event.CreatedAt,
				LastExposedAt:  event.CreatedAt,
				ExposureCount:  1,
				LastScene:      write.Scene,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "user_id"},
					{Name: "video_id"},
				},
				DoUpdates: clause.Assignments(map[string]any{
					"last_exposed_at": gorm.Expr("VALUES(last_exposed_at)"),
					"exposure_count":  gorm.Expr("exposure_count + 1"),
					"last_scene":      gorm.Expr("VALUES(last_scene)"),
					"updated_at":      gorm.Expr("VALUES(updated_at)"),
				}),
			}).Create(&model).Error; err != nil {
				return err
			}

			var saved infraexposure.ExposureModel
			if err := tx.Where("user_id = ? AND video_id = ?", write.UserID, write.VideoID).Take(&saved).Error; err != nil {
				return err
			}
			exposures = append(exposures, restoreExposure(saved))
		}
		return nil
	})
	if err != nil {
		return nil, mapRecommendationError(err)
	}
	return exposures, nil
}

func decodeVector(content string) ([]float64, error) {
	var vector []float64
	if err := json.Unmarshal([]byte(content), &vector); err != nil {
		return nil, err
	}
	return vector, nil
}

func positiveEventTypes() []string {
	return []string{
		domainexposure.EventTypePlay,
		domainexposure.EventTypeComplete,
	}
}

func eventWeight(eventType string, watchMs int, completed bool) float64 {
	switch eventType {
	case domainexposure.EventTypeComplete:
		return 3
	case domainexposure.EventTypePlay:
		weight := 1 + float64(watchMs)/30000
		if weight > 2 {
			weight = 2
		}
		if completed {
			weight += 1
		}
		return weight
	default:
		return 1
	}
}

func ensurePublishedVideo(tx *gorm.DB, videoID int64) error {
	var item struct {
		ID int64
	}
	err := tx.Table("video").
		Select("id").
		Where("id = ? AND status = ?", videoID, domainvideo.StatusPublished).
		Take(&item).
		Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domainrecommendation.ErrVideoNotFound
		}
		return err
	}
	return nil
}

func restoreExposure(model infraexposure.ExposureModel) *domainrecommendation.Exposure {
	return domainrecommendation.RestoreExposure(
		model.ID,
		model.UserID,
		model.VideoID,
		model.FirstExposedAt,
		model.LastExposedAt,
		model.ExposureCount,
		model.LastScene,
	)
}

func mapRecommendationError(err error) error {
	if errors.Is(err, domainrecommendation.ErrVideoNotFound) {
		return err
	}
	return err
}

func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
