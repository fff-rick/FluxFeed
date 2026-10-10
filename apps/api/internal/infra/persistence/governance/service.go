package infragovernance

import (
	applicationeventbus "FluxFeed/internal/application/eventbus"
	applicationinteraction "FluxFeed/internal/application/interaction"
	applicationvideo "FluxFeed/internal/application/video"
	domainfeed "FluxFeed/internal/domain/feed"
	domaininteraction "FluxFeed/internal/domain/interaction"
	domainvideo "FluxFeed/internal/domain/video"
	infrainteraction "FluxFeed/internal/infra/persistence/interaction"
	inframessage "FluxFeed/internal/infra/persistence/message"
	infraoutbox "FluxFeed/internal/infra/persistence/outbox"
	infravideo "FluxFeed/internal/infra/persistence/video"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	TargetVideo     = "video"
	TargetComment   = "comment"
	TargetUser      = "user"
	ReviewPending   = "pending"
	ReviewHuman     = "human_review"
	ReviewPublished = "published"
	ReviewRejected  = "rejected"
	ReviewCanceled  = "canceled"
	VerdictPending  = "pending"
	VerdictPass     = "pass"
	VerdictReject   = "reject"
	VerdictReview   = "review"
)

var ErrNotFound = errors.New("governance resource not found")
var ErrForbidden = errors.New("governance permission denied")
var ErrInvalidTransition = errors.New("invalid content state transition")
var ErrInvalidInput = errors.New("invalid governance input")

type Provider interface {
	Review(component, content string) (verdict, reasonCode, reason string)
}

type RuleProvider struct{}

func (RuleProvider) Review(_ string, content string) (string, string, string) {
	value := strings.ToLower(strings.TrimSpace(content))
	if strings.Contains(value, "[reject]") || strings.Contains(value, "违禁") {
		return VerdictReject, "RULE_REJECT", "内容命中拒绝规则"
	}
	if strings.Contains(value, "[review]") || strings.Contains(value, "人工") {
		return VerdictReview, "RULE_MANUAL", "内容需要人工复审"
	}
	return VerdictPass, "", ""
}

type Service struct {
	db               *gorm.DB
	outbox           *infraoutbox.Repository
	videoTopic       string
	interactionTopic string
	provider         Provider
	visibility       VisibilityInvalidator
	publisher        applicationvideo.PublishedEventPublisher
	commentPublisher applicationinteraction.CommentEventPublisher
}
type VisibilityInvalidator interface {
	InvalidateVideo(context.Context, int64, bool) error
}

func New(db *gorm.DB, outbox *infraoutbox.Repository, videoTopic string, interactionTopic ...string) *Service {
	s := &Service{db: db, outbox: outbox, videoTopic: strings.TrimSpace(videoTopic), provider: RuleProvider{}}
	if len(interactionTopic) > 0 {
		s.interactionTopic = strings.TrimSpace(interactionTopic[0])
	}
	return s
}
func (s *Service) SetVisibilityInvalidator(value VisibilityInvalidator) { s.visibility = value }
func (s *Service) SetProvider(value Provider) {
	if value != nil {
		s.provider = value
	}
}
func (s *Service) SetPublishers(video applicationvideo.PublishedEventPublisher, comment applicationinteraction.CommentEventPublisher) {
	s.publisher = video
	s.commentPublisher = comment
}

func (s *Service) SubmitVideo(ctx context.Context, video *domainvideo.Video) error {
	if video == nil || video.ID <= 0 {
		return ErrInvalidInput
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var assets []MediaAssetModel
		if err := tx.Where("owner_id = ? AND url IN ? AND status IN ?", video.AuthorID, []string{video.MediaURL, video.CoverURL}, []string{"ready", "published", "offline", "rejected"}).Find(&assets).Error; err != nil {
			return err
		}
		if len(assets) != 2 {
			return ErrForbidden
		}
		kinds := map[string]string{}
		for _, asset := range assets {
			kinds[asset.URL] = asset.Kind
		}
		if kinds[video.MediaURL] != "video" || kinds[video.CoverURL] != "cover" {
			return ErrForbidden
		}
		if err := tx.Model(&MediaAssetModel{}).Where("id IN ?", []int64{assets[0].ID, assets[1].ID}).Update("video_id", video.ID).Error; err != nil {
			return err
		}
		version := 1
		var latest ReviewModel
		findErr := tx.Where("target_type=? AND target_id=?", TargetVideo, video.ID).Order("version DESC").Take(&latest).Error
		if findErr == nil {
			if latest.Status == ReviewPending || latest.Status == ReviewHuman {
				return nil
			}
			version = latest.Version + 1
		} else if !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		review := ReviewModel{RequestID: randomID(), TargetType: TargetVideo, TargetID: video.ID, Version: version, Status: ReviewPending}
		if err := tx.Create(&review).Error; err != nil {
			return err
		}
		items := []ReviewItemModel{
			{ReviewID: review.ID, Component: "title", Content: video.Title, Verdict: VerdictPending},
			{ReviewID: review.ID, Component: "video", Content: video.MediaURL, Verdict: VerdictPending},
			{ReviewID: review.ID, Component: "cover", Content: video.CoverURL, Verdict: VerdictPending},
		}
		if err := tx.Create(&items).Error; err != nil {
			return err
		}
		if s.publisher == nil && s.outbox != nil && s.videoTopic != "" {
			event, err := applicationeventbus.New(randomID(), applicationeventbus.TypeContentReviewRequested, video.AuthorID, video.ID, time.Now(), map[string]any{"request_id": review.RequestID, "target_type": TargetVideo, "target_id": video.ID})
			if err != nil {
				return err
			}
			if err := s.outbox.Add(tx, s.videoTopic, event); err != nil {
				return err
			}
		}
		if version > 1 {
			return tx.Create(&StateHistoryModel{TargetType: TargetVideo, TargetID: video.ID, FromStatus: domainvideo.StatusPublished, ToStatus: domainvideo.StatusPendingReview, ActorType: "user", ActorID: video.AuthorID, Reason: "content changed"}).Error
		}
		if err := tx.Create(&StateHistoryModel{TargetType: TargetVideo, TargetID: video.ID, FromStatus: domainvideo.StatusDraft, ToStatus: domainvideo.StatusProcessing, ActorType: "user", ActorID: video.AuthorID, Reason: "submitted"}).Error; err != nil {
			return err
		}
		return tx.Create(&StateHistoryModel{TargetType: TargetVideo, TargetID: video.ID, FromStatus: domainvideo.StatusProcessing, ToStatus: domainvideo.StatusPendingReview, ActorType: "system", ActorID: 0, Reason: "media ready"}).Error
	})
}

func (s *Service) SubmitComment(ctx context.Context, comment *domaininteraction.Comment) error {
	if comment == nil || comment.ID <= 0 {
		return ErrInvalidInput
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		if err := tx.Model(&ReviewModel{}).Where("target_type = ? AND target_id = ?", TargetComment, comment.ID).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return nil
		}
		review := ReviewModel{RequestID: randomID(), TargetType: TargetComment, TargetID: comment.ID, Version: 1, Status: ReviewPending}
		if err := tx.Create(&review).Error; err != nil {
			return err
		}
		if err := tx.Create(&ReviewItemModel{ReviewID: review.ID, Component: "comment", Content: comment.Content, Verdict: VerdictPending}).Error; err != nil {
			return err
		}
		if s.commentPublisher == nil && s.outbox != nil && s.interactionTopic != "" {
			event, err := applicationeventbus.New(randomID(), applicationeventbus.TypeContentReviewRequested, comment.UserID, comment.VideoID, time.Now(), map[string]any{"request_id": review.RequestID, "target_type": TargetComment, "target_id": comment.ID})
			if err != nil {
				return err
			}
			return s.outbox.Add(tx, s.interactionTopic, event)
		}
		return nil
	})
}

func (s *Service) ProcessPending(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = 50
	}
	var reviews []ReviewModel
	if err := s.db.WithContext(ctx).Where("status = ?", ReviewPending).Order("id ASC").Limit(limit).Find(&reviews).Error; err != nil {
		return err
	}
	for i := range reviews {
		if err := s.process(ctx, reviews[i].ID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) process(ctx context.Context, reviewID int64) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var review ReviewModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", reviewID).Take(&review).Error; err != nil {
			return err
		}
		if review.Status != ReviewPending {
			return nil
		}
		var items []ReviewItemModel
		if err := tx.Where("review_id = ?", review.ID).Order("id ASC").Find(&items).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		final := VerdictPass
		for i := range items {
			if items[i].Verdict == VerdictPending {
				items[i].Verdict, items[i].ReasonCode, items[i].Reason = s.provider.Review(items[i].Component, items[i].Content)
				items[i].Provider, items[i].ReviewedAt = "rule", &now
				if err := tx.Save(&items[i]).Error; err != nil {
					return err
				}
			}
			if items[i].Verdict == VerdictReject {
				final = VerdictReject
				review.ReasonCode, review.Reason = items[i].ReasonCode, items[i].Reason
			}
			if items[i].Verdict == VerdictReview && final != VerdictReject {
				final = VerdictReview
				review.ReasonCode, review.Reason = items[i].ReasonCode, items[i].Reason
			}
		}
		if applyVideoHumanGate(&review, items) {
			return tx.Save(&review).Error
		}
		switch final {
		case VerdictPass:
			review.Status = ReviewPublished
			if err := s.publishTarget(tx, &review, 0, "machine review passed"); err != nil {
				return err
			}
		case VerdictReject:
			review.Status = ReviewRejected
			if err := s.rejectTarget(tx, &review, 0, review.ReasonCode, review.Reason); err != nil {
				return err
			}
		default:
			review.Status = ReviewHuman
		}
		return tx.Save(&review).Error
	})
}

func (s *Service) ApplyMachineResult(ctx context.Context, requestID, component, verdict, reasonCode, reason string, providerTime *time.Time) error {
	verdict = strings.ToLower(strings.TrimSpace(verdict))
	if requestID == "" || component == "" || (verdict != VerdictPass && verdict != VerdictReject && verdict != VerdictReview) {
		return ErrInvalidInput
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var review ReviewModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("request_id = ?", requestID).Take(&review).Error; err != nil {
			return mapNotFound(err)
		}
		var item ReviewItemModel
		if err := tx.Where("review_id = ? AND component = ?", review.ID, component).Take(&item).Error; err != nil {
			return mapNotFound(err)
		}
		if item.Verdict != VerdictPending {
			return nil
		}
		now := time.Now().UTC()
		if providerTime != nil && !providerTime.IsZero() {
			now = providerTime.UTC()
		}
		item.Verdict, item.ReasonCode, item.Reason, item.Provider, item.ReviewedAt = verdict, strings.TrimSpace(reasonCode), strings.TrimSpace(reason), "callback", &now
		if err := tx.Save(&item).Error; err != nil {
			return err
		}
		return s.aggregate(tx, &review)
	})
}

func (s *Service) aggregate(tx *gorm.DB, review *ReviewModel) error {
	var items []ReviewItemModel
	if err := tx.Where("review_id = ?", review.ID).Find(&items).Error; err != nil {
		return err
	}
	for _, item := range items {
		if item.Verdict == VerdictPending {
			return nil
		}
	}
	if applyVideoHumanGate(review, items) {
		return tx.Save(review).Error
	}
	for _, item := range items {
		if item.Verdict == VerdictReject {
			review.Status, review.ReasonCode, review.Reason = ReviewRejected, item.ReasonCode, item.Reason
			if err := s.rejectTarget(tx, review, 0, item.ReasonCode, item.Reason); err != nil {
				return err
			}
			return tx.Save(review).Error
		}
	}
	for _, item := range items {
		if item.Verdict == VerdictReview {
			review.Status = ReviewHuman
			return tx.Save(review).Error
		}
	}
	review.Status = ReviewPublished
	if err := s.publishTarget(tx, review, 0, "machine review passed"); err != nil {
		return err
	}
	return tx.Save(review).Error
}

// applyVideoHumanGate keeps machine results as reviewer hints; only a human decision may publish a video.
func applyVideoHumanGate(review *ReviewModel, items []ReviewItemModel) bool {
	if review.TargetType != TargetVideo {
		return false
	}
	review.Status, review.ReasonCode, review.Reason = ReviewHuman, "", ""
	for _, item := range items {
		if item.Verdict == VerdictReject {
			review.ReasonCode, review.Reason = item.ReasonCode, item.Reason
			return true
		}
	}
	for _, item := range items {
		if item.Verdict == VerdictReview {
			review.ReasonCode, review.Reason = item.ReasonCode, item.Reason
			break
		}
	}
	return true
}

func (s *Service) publishTarget(tx *gorm.DB, review *ReviewModel, actorID int64, reason string) error {
	if review.TargetType == TargetVideo {
		var model infravideo.VideoModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", review.TargetID).Take(&model).Error; err != nil {
			return mapNotFound(err)
		}
		from := model.Status
		if from == domainvideo.StatusDeleted {
			review.Status = ReviewCanceled
			return nil
		}
		if from == domainvideo.StatusPublished {
			return nil
		}
		if from != domainvideo.StatusPendingReview && from != domainvideo.StatusRejected && from != domainvideo.StatusAppealPending && from != domainvideo.StatusOffline {
			return ErrInvalidTransition
		}
		if model.PublishedAt == nil {
			now := time.Now().UTC()
			model.PublishedAt = &now
		}
		model.Status, model.ReviewReasonCode, model.ReviewReason = domainvideo.StatusPublished, "", ""
		if err := tx.Save(&model).Error; err != nil {
			return err
		}
		_ = tx.Model(&MediaAssetModel{}).Where("url IN ?", []string{model.MediaURL, model.CoverURL}).Updates(map[string]any{"status": "published", "video_id": model.ID}).Error
		video := domainvideo.RestoreVideo(model.ID, model.AuthorID, model.Title, model.Description, model.MediaURL, model.CoverURL, model.Status, 0, 0, 0, model.PublishedAt, model.CreatedAt, model.UpdatedAt, value(model.IdempotencyKey))
		eventSource := applicationvideo.NewPublishedEvent(video)
		if eventSource != nil && s.publisher == nil && s.outbox != nil && s.videoTopic != "" {
			event, err := applicationeventbus.New(eventSource.EventID, applicationeventbus.TypeVideoPublished, model.AuthorID, model.ID, eventSource.OccurredAt, eventSource)
			if err != nil {
				return err
			}
			if err := s.outbox.Add(tx, s.videoTopic, event); err != nil {
				return err
			}
		}
		if eventSource != nil && s.publisher != nil {
			_ = s.publisher.PublishVideoPublished(context.Background(), eventSource)
		}
		return tx.Create(&StateHistoryModel{TargetType: TargetVideo, TargetID: model.ID, FromStatus: from, ToStatus: model.Status, ActorType: actorType(actorID), ActorID: actorID, Reason: reason}).Error
	}
	var comment infrainteraction.CommentModel
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", review.TargetID).Take(&comment).Error; err != nil {
		return mapNotFound(err)
	}
	from := comment.Status
	if from == domaininteraction.CommentStatusDeleted {
		review.Status = ReviewCanceled
		return nil
	}
	if from == domaininteraction.CommentStatusNormal {
		return nil
	}
	if from != domaininteraction.CommentStatusPendingReview && from != domaininteraction.CommentStatusRejected && from != domaininteraction.CommentStatusAppealPending && from != domaininteraction.CommentStatusOffline {
		return ErrInvalidTransition
	}
	comment.Status, comment.ReviewReasonCode, comment.ReviewReason = domaininteraction.CommentStatusNormal, "", ""
	if err := tx.Save(&comment).Error; err != nil {
		return err
	}
	if err := tx.Model(&infravideo.VideoStatModel{}).Where("video_id = ?", comment.VideoID).Update("comment_count", gorm.Expr("comment_count + 1")).Error; err != nil {
		return err
	}
	var stat infravideo.VideoStatModel
	if err := tx.Where("video_id=?", comment.VideoID).Take(&stat).Error; err != nil {
		return err
	}
	created := domaininteraction.RestoreComment(comment.ID, comment.VideoID, comment.UserID, "", "", comment.Content, comment.Status, value(comment.IdempotencyKey), comment.CreatedAt, comment.UpdatedAt)
	source := applicationinteraction.NewCommentedEvent(created, stat.CommentCount)
	if s.commentPublisher == nil && s.outbox != nil && s.interactionTopic != "" {
		event, err := applicationeventbus.New(source.EventID, applicationeventbus.TypeVideoCommented, source.UserID, source.VideoID, source.OccurredAt, source)
		if err != nil {
			return err
		}
		if err := s.outbox.Add(tx, s.interactionTopic, event); err != nil {
			return err
		}
	}
	if s.commentPublisher != nil {
		_ = s.commentPublisher.PublishVideoCommented(context.Background(), source)
	}
	var video infravideo.VideoModel
	if err := tx.Select("id,author_id").Where("id=?", comment.VideoID).Take(&video).Error; err == nil && video.AuthorID != comment.UserID {
		eventID := source.EventID
		_ = tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&inframessage.MessageModel{UserID: video.AuthorID, Type: "COMMENT", Title: "收到评论", Content: comment.Content, ActorID: comment.UserID, EventID: &eventID}).Error
	}
	return tx.Create(&StateHistoryModel{TargetType: TargetComment, TargetID: comment.ID, FromStatus: from, ToStatus: comment.Status, ActorType: actorType(actorID), ActorID: actorID, Reason: reason}).Error
}

func (s *Service) rejectTarget(tx *gorm.DB, review *ReviewModel, actorID int64, reasonCode, reason string) error {
	if review.TargetType == TargetVideo {
		var model infravideo.VideoModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", review.TargetID).Take(&model).Error; err != nil {
			return mapNotFound(err)
		}
		from := model.Status
		if from == domainvideo.StatusDeleted {
			review.Status = ReviewCanceled
			return nil
		}
		if from == domainvideo.StatusRejected {
			return nil
		}
		model.Status, model.ReviewReasonCode, model.ReviewReason = domainvideo.StatusRejected, reasonCode, reason
		if err := tx.Save(&model).Error; err != nil {
			return err
		}
		_ = tx.Model(&MediaAssetModel{}).Where("url IN ?", []string{model.MediaURL, model.CoverURL}).Update("status", "rejected").Error
		return tx.Create(&StateHistoryModel{TargetType: TargetVideo, TargetID: model.ID, FromStatus: from, ToStatus: model.Status, ActorType: actorType(actorID), ActorID: actorID, Reason: reason}).Error
	}
	var model infrainteraction.CommentModel
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", review.TargetID).Take(&model).Error; err != nil {
		return mapNotFound(err)
	}
	from := model.Status
	if from == domaininteraction.CommentStatusDeleted {
		review.Status = ReviewCanceled
		return nil
	}
	if from == domaininteraction.CommentStatusRejected {
		return nil
	}
	model.Status, model.ReviewReasonCode, model.ReviewReason = domaininteraction.CommentStatusRejected, reasonCode, reason
	if err := tx.Save(&model).Error; err != nil {
		return err
	}
	return tx.Create(&StateHistoryModel{TargetType: TargetComment, TargetID: model.ID, FromStatus: from, ToStatus: model.Status, ActorType: actorType(actorID), ActorID: actorID, Reason: reason}).Error
}

func (s *Service) DecideReview(ctx context.Context, actorID, reviewID int64, decision, reason, requestID, ip string) error {
	decision, reason = strings.ToLower(strings.TrimSpace(decision)), strings.TrimSpace(reason)
	if actorID <= 0 || reviewID <= 0 || reason == "" || (decision != VerdictPass && decision != VerdictReject) {
		return ErrInvalidInput
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var review ReviewModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", reviewID).Take(&review).Error; err != nil {
			return mapNotFound(err)
		}
		if !canDecideReview(review.Status) {
			return ErrInvalidTransition
		}
		before, _ := json.Marshal(review)
		if decision == VerdictPass {
			review.Status = ReviewPublished
			if err := s.publishTarget(tx, &review, actorID, reason); err != nil {
				return err
			}
			if review.Status == ReviewCanceled {
				return ErrInvalidTransition
			}
		} else {
			review.Status, review.ReasonCode, review.Reason = ReviewRejected, "HUMAN_REJECT", reason
			if err := s.rejectTarget(tx, &review, actorID, "HUMAN_REJECT", reason); err != nil {
				return err
			}
			if review.Status == ReviewCanceled {
				return ErrInvalidTransition
			}
		}
		if err := tx.Save(&review).Error; err != nil {
			return err
		}
		after, _ := json.Marshal(review)
		return s.audit(tx, actorID, "review.decide", "review.decide", review.TargetType, review.TargetID, reason, string(before), string(after), requestID, ip)
	})
}

func canDecideReview(status string) bool { return status == ReviewHuman }

type ReviewView struct {
	ReviewModel
	Items []ReviewItemModel `json:"items"`
}

func (s *Service) ListReviews(ctx context.Context, status string, limit int) ([]ReviewView, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	q := s.db.WithContext(ctx).Order("id DESC").Limit(limit)
	if status != "" {
		q = q.Where("status = ?", status)
	}
	var rows []ReviewModel
	if err := q.Find(&rows).Error; err != nil {
		return nil, err
	}
	views := make([]ReviewView, 0, len(rows))
	for _, row := range rows {
		var items []ReviewItemModel
		if err := s.db.WithContext(ctx).Where("review_id=?", row.ID).Order("id").Find(&items).Error; err != nil {
			return nil, err
		}
		views = append(views, ReviewView{ReviewModel: row, Items: items})
	}
	return views, nil
}

func (s *Service) CreateReport(ctx context.Context, userID int64, targetType string, targetID int64, category, description, idempotencyKey string) (*ReportModel, error) {
	targetType, category, description = strings.ToLower(strings.TrimSpace(targetType)), strings.TrimSpace(category), strings.TrimSpace(description)
	if userID <= 0 || targetID <= 0 || category == "" || description == "" || (targetType != TargetVideo && targetType != TargetComment && targetType != TargetUser) {
		return nil, ErrInvalidInput
	}
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if len(idempotencyKey) > 128 {
		return nil, ErrInvalidInput
	}
	if idempotencyKey != "" {
		var existing ReportModel
		if err := s.db.WithContext(ctx).Where("reporter_id=? AND idempotency_key=?", userID, idempotencyKey).Take(&existing).Error; err == nil {
			return &existing, nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}
	report := &ReportModel{ReporterID: userID, TargetType: targetType, TargetID: targetID, Category: category, Description: description, Status: "pending", IdempotencyKey: optional(idempotencyKey)}
	table := map[string]string{TargetVideo: "video", TargetComment: "interaction_comment", TargetUser: "account"}[targetType]
	var count int64
	if err := s.db.WithContext(ctx).Table(table).Where("id=?", targetID).Count(&count).Error; err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, ErrNotFound
	}
	if err := s.db.WithContext(ctx).Create(report).Error; err != nil {
		if idempotencyKey != "" {
			var existing ReportModel
			if loadErr := s.db.WithContext(ctx).Where("reporter_id=? AND idempotency_key=?", userID, idempotencyKey).Take(&existing).Error; loadErr == nil {
				return &existing, nil
			}
		}
		return nil, err
	}
	return report, nil
}

func (s *Service) ListMyReports(ctx context.Context, userID int64) ([]ReportModel, error) {
	var rows []ReportModel
	return rows, s.db.WithContext(ctx).Where("reporter_id = ?", userID).Order("id DESC").Limit(100).Find(&rows).Error
}
func (s *Service) ListReports(ctx context.Context, status string) ([]ReportModel, error) {
	var rows []ReportModel
	q := s.db.WithContext(ctx).Order("id DESC").Limit(100)
	if status != "" {
		q = q.Where("status = ?", status)
	}
	return rows, q.Find(&rows).Error
}

func (s *Service) ResolveReport(ctx context.Context, actorID, reportID int64, resolution, requestID, ip string) error {
	resolution = strings.TrimSpace(resolution)
	if resolution == "" {
		return ErrInvalidInput
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row ReportModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", reportID).Take(&row).Error; err != nil {
			return mapNotFound(err)
		}
		if row.Status == "resolved" {
			return nil
		}
		before, _ := json.Marshal(row)
		row.Status = "resolved"
		row.Resolution = resolution
		row.ResolverID = actorID
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		after, _ := json.Marshal(row)
		return s.audit(tx, actorID, "report.resolve", "report.resolve", row.TargetType, row.TargetID, resolution, string(before), string(after), requestID, ip)
	})
}

func (s *Service) SetBlock(ctx context.Context, blockerID, blockedID int64, active bool) error {
	if blockerID <= 0 || blockedID <= 0 || blockerID == blockedID {
		return ErrInvalidInput
	}
	if active {
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&UserBlockModel{BlockerID: blockerID, BlockedID: blockedID}).Error; err != nil {
				return err
			}
			return tx.Table("user_follow").Where("(user_id=? AND target_user_id=?) OR (user_id=? AND target_user_id=?)", blockerID, blockedID, blockedID, blockerID).Update("status", 2).Error
		})
	}
	return s.db.WithContext(ctx).Where("blocker_id = ? AND blocked_id = ?", blockerID, blockedID).Delete(&UserBlockModel{}).Error
}
func (s *Service) Blocked(ctx context.Context, a, b int64) (bool, error) {
	var n int64
	err := s.db.WithContext(ctx).Model(&UserBlockModel{}).Where("(blocker_id=? AND blocked_id=?) OR (blocker_id=? AND blocked_id=?)", a, b, b, a).Count(&n).Error
	return n > 0, err
}
func (s *Service) UsersVisible(ctx context.Context, viewerID, targetID int64) (bool, error) {
	if viewerID <= 0 {
		return true, nil
	}
	blocked, err := s.Blocked(ctx, viewerID, targetID)
	return !blocked, err
}
func (s *Service) VideoVisible(ctx context.Context, viewerID, videoID int64) (bool, error) {
	var row struct{ AuthorID int64 }
	result := s.db.WithContext(ctx).Table("video").Select("author_id").Where("id=? AND status=?", videoID, domainvideo.StatusPublished).Scan(&row)
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected == 0 {
		return false, nil
	}
	return s.UsersVisible(ctx, viewerID, row.AuthorID)
}

func (s *Service) CreateAppeal(ctx context.Context, userID int64, targetType string, targetID int64, reason, idempotencyKey string) (*AppealModel, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" || userID <= 0 || targetID <= 0 {
		return nil, ErrInvalidInput
	}
	var review ReviewModel
	if err := s.db.WithContext(ctx).Where("target_type=? AND target_id=? AND status=?", targetType, targetID, ReviewRejected).Order("version DESC").Take(&review).Error; err != nil {
		return nil, mapNotFound(err)
	}
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if len(idempotencyKey) > 128 {
		return nil, ErrInvalidInput
	}
	if idempotencyKey != "" {
		var existing AppealModel
		if err := s.db.WithContext(ctx).Where("user_id=? AND idempotency_key=?", userID, idempotencyKey).Take(&existing).Error; err == nil {
			return &existing, nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}
	var pending AppealModel
	if err := s.db.WithContext(ctx).Where("review_id=? AND user_id=? AND status=?", review.ID, userID, "pending").Take(&pending).Error; err == nil {
		return &pending, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	appeal := &AppealModel{TargetType: targetType, TargetID: targetID, ReviewID: review.ID, UserID: userID, Reason: reason, Status: "pending", IdempotencyKey: optional(idempotencyKey)}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var owner struct {
			OwnerID int64 `gorm:"column:owner_id"`
		}
		if targetType == TargetVideo {
			if err := tx.Model(&infravideo.VideoModel{}).Select("author_id AS owner_id").Where("id=? AND status=?", targetID, domainvideo.StatusRejected).Take(&owner).Error; err != nil {
				return ErrForbidden
			}
		} else if targetType == TargetComment {
			if err := tx.Model(&infrainteraction.CommentModel{}).Select("user_id AS owner_id").Where("id=? AND status=?", targetID, domaininteraction.CommentStatusRejected).Take(&owner).Error; err != nil {
				return ErrForbidden
			}
		} else {
			return ErrInvalidInput
		}
		if owner.OwnerID != userID {
			return ErrForbidden
		}
		if err := tx.Create(appeal).Error; err != nil {
			return err
		}
		if targetType == TargetVideo {
			return tx.Model(&infravideo.VideoModel{}).Where("id=? AND author_id=? AND status=?", targetID, userID, domainvideo.StatusRejected).Update("status", domainvideo.StatusAppealPending).Error
		}
		return tx.Model(&infrainteraction.CommentModel{}).Where("id=? AND user_id=? AND status=?", targetID, userID, domaininteraction.CommentStatusRejected).Update("status", domaininteraction.CommentStatusAppealPending).Error
	})
	if err != nil && idempotencyKey != "" {
		var existing AppealModel
		if loadErr := s.db.WithContext(ctx).Where("user_id=? AND idempotency_key=?", userID, idempotencyKey).Take(&existing).Error; loadErr == nil {
			return &existing, nil
		}
	}
	return appeal, err
}
func (s *Service) ListMyAppeals(ctx context.Context, userID int64) ([]AppealModel, error) {
	var rows []AppealModel
	return rows, s.db.WithContext(ctx).Where("user_id=?", userID).Order("id DESC").Find(&rows).Error
}
func (s *Service) ListAppeals(ctx context.Context, status string) ([]AppealModel, error) {
	var rows []AppealModel
	q := s.db.WithContext(ctx).Order("id DESC")
	if status != "" {
		q = q.Where("status=?", status)
	}
	return rows, q.Find(&rows).Error
}
func (s *Service) DecideAppeal(ctx context.Context, actorID, appealID int64, decision, reason, requestID, ip string) error {
	reason = strings.TrimSpace(reason)
	decision = strings.ToLower(strings.TrimSpace(decision))
	if reason == "" || (decision != VerdictPass && decision != VerdictReject) {
		return ErrInvalidInput
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var a AppealModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", appealID).Take(&a).Error; err != nil {
			return mapNotFound(err)
		}
		if a.Status != "pending" {
			return nil
		}
		var review ReviewModel
		if err := tx.Where("id=?", a.ReviewID).Take(&review).Error; err != nil {
			return err
		}
		before, _ := json.Marshal(a)
		if decision == VerdictPass {
			if err := s.publishTarget(tx, &review, actorID, reason); err != nil {
				return err
			}
			if review.Status == ReviewCanceled {
				return ErrInvalidTransition
			}
			review.Status, review.Reason = ReviewPublished, reason
			a.Status = "approved"
		} else {
			if err := s.rejectTarget(tx, &review, actorID, "APPEAL_REJECT", reason); err != nil {
				return err
			}
			if review.Status == ReviewCanceled {
				return ErrInvalidTransition
			}
			review.Status, review.Reason = ReviewRejected, reason
			a.Status = "rejected"
		}
		if err := tx.Save(&review).Error; err != nil {
			return err
		}
		a.DecisionReason = reason
		a.ReviewerID = actorID
		if err := tx.Save(&a).Error; err != nil {
			return err
		}
		after, _ := json.Marshal(a)
		return s.audit(tx, actorID, "appeal.decide", "appeal.decide", a.TargetType, a.TargetID, reason, string(before), string(after), requestID, ip)
	})
}

func (s *Service) HasPermission(ctx context.Context, userID int64, permission string) (bool, error) {
	var n int64
	err := s.db.WithContext(ctx).Table("admin_user_role ur").Joins("JOIN admin_role_permission rp ON rp.role_id=ur.role_id").Joins("JOIN admin_permission p ON p.id=rp.permission_id").Where("ur.user_id=? AND p.name=?", userID, permission).Count(&n).Error
	return n > 0, err
}
func (s *Service) ListAudit(ctx context.Context) ([]AuditLogModel, error) {
	var rows []AuditLogModel
	return rows, s.db.WithContext(ctx).Order("id DESC").Limit(200).Find(&rows).Error
}
func (s *Service) audit(tx *gorm.DB, actorID int64, permission, action, targetType string, targetID int64, reason, before, after, requestID, ip string) error {
	return tx.Create(&AuditLogModel{ActorID: actorID, Permission: permission, Action: action, TargetType: targetType, TargetID: targetID, Reason: reason, BeforeJSON: before, AfterJSON: after, RequestID: requestID, IP: ip}).Error
}

func (s *Service) RecordAsset(ctx context.Context, ownerID int64, kind, url, path string) (int64, error) {
	row := MediaAssetModel{OwnerID: ownerID, Kind: kind, URL: url, Path: path, Status: "ready"}
	err := s.db.WithContext(ctx).Create(&row).Error
	return row.ID, err
}
func (s *Service) AssetByURL(ctx context.Context, url string) (*MediaAssetModel, error) {
	var row MediaAssetModel
	err := s.db.WithContext(ctx).Where("url=?", url).Take(&row).Error
	return &row, mapNotFound(err)
}
func (s *Service) AssetAccess(ctx context.Context, url string) (int64, string, string, string, error) {
	row, err := s.AssetByURL(ctx, url)
	if err != nil {
		return 0, "", "", "", err
	}
	if row.VideoID > 0 {
		var video struct {
			Status int `gorm:"column:status"`
		}
		if err := s.db.WithContext(ctx).Table("video").Select("status").Where("id=?", row.VideoID).Take(&video).Error; err != nil || video.Status != domainvideo.StatusPublished {
			row.Status = "offline"
		}
	}
	return row.OwnerID, row.Kind, row.Path, row.Status, nil
}

func (s *Service) AccountActive(ctx context.Context, userID int64) (bool, error) {
	var account struct {
		Status int `gorm:"column:status"`
	}
	err := s.db.WithContext(ctx).Table("account").Select("status").Where("id = ?", userID).Take(&account).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return account.Status == 1, err
}

func (s *Service) FilterVisible(ctx context.Context, viewerID int64, items []*domainfeed.FeedPageItem) ([]*domainfeed.FeedPageItem, error) {
	if len(items) == 0 {
		return items, nil
	}
	ids := make([]int64, 0, len(items))
	for _, item := range items {
		if item != nil {
			ids = append(ids, item.VideoID)
		}
	}
	var visible []int64
	q := s.db.WithContext(ctx).Table("video").Where("id IN ? AND status=?", ids, domainvideo.StatusPublished)
	if viewerID > 0 {
		q = q.Where("author_id NOT IN (SELECT blocked_id FROM user_block WHERE blocker_id=?) AND author_id NOT IN (SELECT blocker_id FROM user_block WHERE blocked_id=?)", viewerID, viewerID)
	}
	if err := q.Pluck("id", &visible).Error; err != nil {
		return nil, err
	}
	allowed := make(map[int64]struct{}, len(visible))
	for _, id := range visible {
		allowed[id] = struct{}{}
	}
	result := make([]*domainfeed.FeedPageItem, 0, len(items))
	for _, item := range items {
		if item != nil {
			if _, ok := allowed[item.VideoID]; ok {
				result = append(result, item)
			}
		}
	}
	return result, nil
}
func (s *Service) FilterVisibleComments(ctx context.Context, viewerID int64, items []*domaininteraction.Comment) ([]*domaininteraction.Comment, error) {
	if viewerID <= 0 {
		return items, nil
	}
	var blocked []int64
	if err := s.db.WithContext(ctx).Table("user_block").Where("blocker_id=? OR blocked_id=?", viewerID, viewerID).Pluck("CASE WHEN blocker_id = ? THEN blocked_id ELSE blocker_id END", &blocked).Error; err != nil {
		return nil, err
	}
	set := map[int64]struct{}{}
	for _, id := range blocked {
		set[id] = struct{}{}
	}
	result := make([]*domaininteraction.Comment, 0, len(items))
	for _, item := range items {
		if _, ok := set[item.UserID]; !ok {
			result = append(result, item)
		}
	}
	return result, nil
}

func (s *Service) ModerateContent(ctx context.Context, actorID int64, targetType string, targetID int64, restore bool, reason, requestID, ip string) error {
	reason = strings.TrimSpace(reason)
	if reason == "" || targetID <= 0 {
		return ErrInvalidInput
	}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if targetType == TargetVideo {
			var row infravideo.VideoModel
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", targetID).Take(&row).Error; err != nil {
				return mapNotFound(err)
			}
			before, _ := json.Marshal(row)
			from := row.Status
			if restore {
				if row.Status != domainvideo.StatusOffline {
					return ErrInvalidTransition
				}
				review := ReviewModel{TargetType: TargetVideo, TargetID: row.ID}
				if err := s.publishTarget(tx, &review, actorID, reason); err != nil {
					return err
				}
				row.Status = domainvideo.StatusPublished
			} else {
				if row.Status != domainvideo.StatusPublished {
					return ErrInvalidTransition
				}
				row.Status = domainvideo.StatusOffline
				if err := tx.Save(&row).Error; err != nil {
					return err
				}
				_ = tx.Model(&MediaAssetModel{}).Where("url IN ?", []string{row.MediaURL, row.CoverURL}).Update("status", "offline").Error
			}
			if err := s.visibilityEvent(tx, row.ID, row.AuthorID, row.Status); err != nil {
				return err
			}
			after, _ := json.Marshal(row)
			if !restore {
				if err := tx.Create(&StateHistoryModel{TargetType: TargetVideo, TargetID: row.ID, FromStatus: from, ToStatus: row.Status, ActorType: "admin", ActorID: actorID, Reason: reason}).Error; err != nil {
					return err
				}
			}
			return s.audit(tx, actorID, "content.moderate", "content.moderate", TargetVideo, targetID, reason, string(before), string(after), requestID, ip)
		}
		if targetType != TargetComment {
			return ErrInvalidInput
		}
		var row infrainteraction.CommentModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", targetID).Take(&row).Error; err != nil {
			return mapNotFound(err)
		}
		before, _ := json.Marshal(row)
		from := row.Status
		if restore {
			if row.Status != domaininteraction.CommentStatusOffline {
				return ErrInvalidTransition
			}
			review := ReviewModel{TargetType: TargetComment, TargetID: row.ID}
			if err := s.publishTarget(tx, &review, actorID, reason); err != nil {
				return err
			}
			row.Status = domaininteraction.CommentStatusNormal
		} else {
			if row.Status != domaininteraction.CommentStatusNormal {
				return ErrInvalidTransition
			}
			row.Status = domaininteraction.CommentStatusOffline
			if err := tx.Save(&row).Error; err != nil {
				return err
			}
			if err := tx.Model(&infravideo.VideoStatModel{}).Where("video_id=?", row.VideoID).Update("comment_count", gorm.Expr("GREATEST(comment_count - 1, 0)")).Error; err != nil {
				return err
			}
		}
		after, _ := json.Marshal(row)
		if !restore {
			if err := tx.Create(&StateHistoryModel{TargetType: TargetComment, TargetID: row.ID, FromStatus: from, ToStatus: row.Status, ActorType: "admin", ActorID: actorID, Reason: reason}).Error; err != nil {
				return err
			}
		}
		return s.audit(tx, actorID, "content.moderate", "content.moderate", TargetComment, targetID, reason, string(before), string(after), requestID, ip)
	})
	if err == nil && targetType == TargetVideo && s.visibility != nil {
		_ = s.visibility.InvalidateVideo(ctx, targetID, !restore)
	}
	return err
}

func (s *Service) SetUserBanned(ctx context.Context, actorID, userID int64, banned bool, reason, requestID, ip string) error {
	reason = strings.TrimSpace(reason)
	if reason == "" || userID <= 0 {
		return ErrInvalidInput
	}
	var videoIDs []int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var before struct {
			ID     int64
			Status int
		}
		if err := tx.Table("account").Select("id,status").Where("id=?", userID).Take(&before).Error; err != nil {
			return mapNotFound(err)
		}
		status := 1
		if banned {
			status = 2
		}
		if err := tx.Table("account").Where("id=?", userID).Update("status", status).Error; err != nil {
			return err
		}
		if banned {
			if err := tx.Model(&infravideo.VideoModel{}).Where("author_id=? AND status=?", userID, domainvideo.StatusPublished).Pluck("id", &videoIDs).Error; err != nil {
				return err
			}
			for _, id := range videoIDs {
				if err := tx.Create(&StateHistoryModel{TargetType: TargetVideo, TargetID: id, FromStatus: domainvideo.StatusPublished, ToStatus: domainvideo.StatusOffline, ActorType: "admin", ActorID: actorID, Reason: reason}).Error; err != nil {
					return err
				}
				if err := s.visibilityEvent(tx, id, userID, domainvideo.StatusOffline); err != nil {
					return err
				}
			}
			if err := tx.Model(&infravideo.VideoModel{}).Where("author_id=? AND status=?", userID, domainvideo.StatusPublished).Update("status", domainvideo.StatusOffline).Error; err != nil {
				return err
			}
			var commentIDs []int64
			if err := tx.Model(&infrainteraction.CommentModel{}).Where("user_id=? AND status=?", userID, domaininteraction.CommentStatusNormal).Pluck("id", &commentIDs).Error; err != nil {
				return err
			}
			for _, id := range commentIDs {
				if err := tx.Create(&StateHistoryModel{TargetType: TargetComment, TargetID: id, FromStatus: domaininteraction.CommentStatusNormal, ToStatus: domaininteraction.CommentStatusOffline, ActorType: "admin", ActorID: actorID, Reason: reason}).Error; err != nil {
					return err
				}
			}
			if err := tx.Model(&infrainteraction.CommentModel{}).Where("user_id=? AND status=?", userID, domaininteraction.CommentStatusNormal).Update("status", domaininteraction.CommentStatusOffline).Error; err != nil {
				return err
			}
			if err := tx.Exec("UPDATE video_stat SET comment_count = (SELECT COUNT(*) FROM interaction_comment WHERE interaction_comment.video_id = video_stat.video_id AND interaction_comment.status = ?) WHERE video_id IN (SELECT video_id FROM interaction_comment WHERE user_id = ?)", domaininteraction.CommentStatusNormal, userID).Error; err != nil {
				return err
			}
		}
		b, _ := json.Marshal(before)
		a, _ := json.Marshal(map[string]any{"id": userID, "status": status})
		action := "user.unban"
		if banned {
			action = "user.ban"
		}
		return s.audit(tx, actorID, "user.ban", action, TargetUser, userID, reason, string(b), string(a), requestID, ip)
	})
	if err == nil && banned && s.visibility != nil {
		for _, id := range videoIDs {
			_ = s.visibility.InvalidateVideo(ctx, id, true)
		}
	}
	return err
}

func (s *Service) AssignRole(ctx context.Context, actorID, userID int64, roleName, reason, requestID, ip string) error {
	roleName, reason = strings.TrimSpace(roleName), strings.TrimSpace(reason)
	if roleName == "" || reason == "" {
		return ErrInvalidInput
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var role RoleModel
		if err := tx.Where("name=?", roleName).Take(&role).Error; err != nil {
			return mapNotFound(err)
		}
		var userCount int64
		if err := tx.Table("account").Where("id=?", userID).Count(&userCount).Error; err != nil {
			return err
		}
		if userCount == 0 {
			return ErrNotFound
		}
		var before []UserRoleModel
		_ = tx.Where("user_id=?", userID).Find(&before).Error
		if err := tx.Where("user_id=?", userID).Delete(&UserRoleModel{}).Error; err != nil {
			return err
		}
		if err := tx.Create(&UserRoleModel{UserID: userID, RoleID: role.ID}).Error; err != nil {
			return err
		}
		if err := tx.Table("account").Where("id=?", userID).Update("role", roleName).Error; err != nil {
			return err
		}
		b, _ := json.Marshal(before)
		a, _ := json.Marshal(map[string]any{"role": roleName})
		return s.audit(tx, actorID, "role.assign", "role.assign", TargetUser, userID, reason, string(b), string(a), requestID, ip)
	})
}

func (s *Service) visibilityEvent(tx *gorm.DB, videoID, authorID int64, status int) error {
	if s.outbox == nil || s.videoTopic == "" {
		return nil
	}
	event, err := applicationeventbus.New(randomID(), applicationeventbus.TypeContentVisibilityChanged, authorID, videoID, time.Now(), map[string]any{"video_id": videoID, "status": domainvideo.StatusName(status)})
	if err != nil {
		return err
	}
	return s.outbox.Add(tx, s.videoTopic, event)
}

func actorType(id int64) string {
	if id > 0 {
		return "admin"
	}
	return "machine"
}
func randomID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err == nil {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
func value(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
func optional(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}
func mapNotFound(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNotFound
	}
	return err
}
