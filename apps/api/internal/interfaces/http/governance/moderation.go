package interfaceshttpgovernance

import (
	infragovernance "FluxFeed/internal/infra/persistence/governance"
	interfaceshttpmiddleware "FluxFeed/internal/interfaces/http/middleware"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type ModerationHandler struct{ service *infragovernance.Service }

func NewModeration(service *infragovernance.Service) *ModerationHandler {
	return &ModerationHandler{service: service}
}

type reportRequest struct {
	TargetType  string `json:"target_type"`
	TargetID    int64  `json:"target_id"`
	Category    string `json:"category"`
	Description string `json:"description"`
}
type reasonRequest struct {
	Reason     string `json:"reason"`
	Decision   string `json:"decision"`
	Resolution string `json:"resolution"`
	Role       string `json:"role"`
}
type machineResultRequest struct {
	RequestID    string     `json:"request_id"`
	Component    string     `json:"component"`
	Verdict      string     `json:"verdict"`
	ReasonCode   string     `json:"reason_code"`
	Reason       string     `json:"reason"`
	ProviderTime *time.Time `json:"provider_time"`
}

func (h *ModerationHandler) CreateReport(c *gin.Context) {
	var req reportRequest
	if c.ShouldBindJSON(&req) != nil {
		bad(c)
		return
	}
	row, err := h.service.CreateReport(c.Request.Context(), userID(c), req.TargetType, req.TargetID, req.Category, req.Description, c.GetHeader("Idempotency-Key"))
	if err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.JSON(http.StatusCreated, row)
}
func (h *ModerationHandler) ListMyReports(c *gin.Context) {
	rows, err := h.service.ListMyReports(c.Request.Context(), userID(c))
	if err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": rows})
}
func (h *ModerationHandler) Block(c *gin.Context) {
	id, ok := pathID(c, "userId")
	if !ok {
		return
	}
	if err := h.service.SetBlock(c.Request.Context(), userID(c), id, true); err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
func (h *ModerationHandler) Unblock(c *gin.Context) {
	id, ok := pathID(c, "userId")
	if !ok {
		return
	}
	if err := h.service.SetBlock(c.Request.Context(), userID(c), id, false); err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
func (h *ModerationHandler) CreateAppeal(c *gin.Context) {
	id, ok := pathID(c, "targetId")
	if !ok {
		return
	}
	var req reasonRequest
	if c.ShouldBindJSON(&req) != nil {
		bad(c)
		return
	}
	row, err := h.service.CreateAppeal(c.Request.Context(), userID(c), c.Param("targetType"), id, req.Reason, c.GetHeader("Idempotency-Key"))
	if err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.JSON(http.StatusCreated, row)
}
func (h *ModerationHandler) ListMyAppeals(c *gin.Context) {
	rows, err := h.service.ListMyAppeals(c.Request.Context(), userID(c))
	if err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": rows})
}
func (h *ModerationHandler) MachineResult(c *gin.Context) {
	var req machineResultRequest
	if c.ShouldBindJSON(&req) != nil {
		bad(c)
		return
	}
	if err := h.service.ApplyMachineResult(c.Request.Context(), req.RequestID, req.Component, req.Verdict, req.ReasonCode, req.Reason, req.ProviderTime); err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *ModerationHandler) ListReviews(c *gin.Context) {
	rows, err := h.service.ListReviews(c.Request.Context(), c.Query("status"), queryLimit(c))
	if err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": rows})
}
func (h *ModerationHandler) DecideReview(c *gin.Context) {
	id, ok := pathID(c, "reviewId")
	if !ok {
		return
	}
	var req reasonRequest
	if c.ShouldBindJSON(&req) != nil {
		bad(c)
		return
	}
	if err := h.service.DecideReview(c.Request.Context(), userID(c), id, req.Decision, req.Reason, c.GetHeader("X-Request-ID"), c.ClientIP()); err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
func (h *ModerationHandler) ListReports(c *gin.Context) {
	rows, err := h.service.ListReports(c.Request.Context(), c.Query("status"))
	if err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": rows})
}
func (h *ModerationHandler) ResolveReport(c *gin.Context) {
	id, ok := pathID(c, "reportId")
	if !ok {
		return
	}
	var req reasonRequest
	if c.ShouldBindJSON(&req) != nil {
		bad(c)
		return
	}
	if err := h.service.ResolveReport(c.Request.Context(), userID(c), id, req.Resolution, c.GetHeader("X-Request-ID"), c.ClientIP()); err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
func (h *ModerationHandler) ListAppeals(c *gin.Context) {
	rows, err := h.service.ListAppeals(c.Request.Context(), c.Query("status"))
	if err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": rows})
}
func (h *ModerationHandler) DecideAppeal(c *gin.Context) {
	id, ok := pathID(c, "appealId")
	if !ok {
		return
	}
	var req reasonRequest
	if c.ShouldBindJSON(&req) != nil {
		bad(c)
		return
	}
	if err := h.service.DecideAppeal(c.Request.Context(), userID(c), id, req.Decision, req.Reason, c.GetHeader("X-Request-ID"), c.ClientIP()); err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
func (h *ModerationHandler) Offline(c *gin.Context) { h.moderate(c, false) }
func (h *ModerationHandler) Restore(c *gin.Context) { h.moderate(c, true) }
func (h *ModerationHandler) moderate(c *gin.Context, restore bool) {
	id, ok := pathID(c, "targetId")
	if !ok {
		return
	}
	var req reasonRequest
	if c.ShouldBindJSON(&req) != nil {
		bad(c)
		return
	}
	if err := h.service.ModerateContent(c.Request.Context(), userID(c), c.Param("targetType"), id, restore, req.Reason, c.GetHeader("X-Request-ID"), c.ClientIP()); err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
func (h *ModerationHandler) Ban(c *gin.Context)   { h.ban(c, true) }
func (h *ModerationHandler) Unban(c *gin.Context) { h.ban(c, false) }
func (h *ModerationHandler) ban(c *gin.Context, banned bool) {
	id, ok := pathID(c, "userId")
	if !ok {
		return
	}
	var req reasonRequest
	if c.ShouldBindJSON(&req) != nil {
		bad(c)
		return
	}
	if err := h.service.SetUserBanned(c.Request.Context(), userID(c), id, banned, req.Reason, c.GetHeader("X-Request-ID"), c.ClientIP()); err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
func (h *ModerationHandler) AssignRole(c *gin.Context) {
	id, ok := pathID(c, "userId")
	if !ok {
		return
	}
	var req reasonRequest
	if c.ShouldBindJSON(&req) != nil {
		bad(c)
		return
	}
	if err := h.service.AssignRole(c.Request.Context(), userID(c), id, req.Role, req.Reason, c.GetHeader("X-Request-ID"), c.ClientIP()); err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
func (h *ModerationHandler) ListAudit(c *gin.Context) {
	rows, err := h.service.ListAudit(c.Request.Context())
	if err != nil {
		writeGovernanceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": rows})
}

func RequirePermission(service *infragovernance.Service, permission string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ok, err := service.HasPermission(c.Request.Context(), userID(c), permission)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "permission check failed"})
			return
		}
		if !ok {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "permission denied"})
			return
		}
		c.Next()
	}
}
func RequireUserVisible(service *infragovernance.Service, param string) gin.HandlerFunc {
	return func(c *gin.Context) {
		targetID, err := strconv.ParseInt(strings.TrimSpace(c.Param(param)), 10, 64)
		if err != nil || targetID <= 0 {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid user id"})
			return
		}
		visible, checkErr := service.UsersVisible(c.Request.Context(), userID(c), targetID)
		if checkErr != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "visibility check failed"})
			return
		}
		if !visible {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "user not found"})
			return
		}
		c.Next()
	}
}
func userID(c *gin.Context) int64 {
	v, _ := c.Get(interfaceshttpmiddleware.ContextUserIDKey)
	id, _ := v.(int64)
	return id
}
func pathID(c *gin.Context, name string) (int64, bool) {
	id, err := strconv.ParseInt(strings.TrimSpace(c.Param(name)), 10, 64)
	if err != nil || id <= 0 {
		bad(c)
		return 0, false
	}
	return id, true
}
func queryLimit(c *gin.Context) int { v, _ := strconv.Atoi(c.Query("limit")); return v }
func bad(c *gin.Context)            { c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"}) }
func writeGovernanceError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, infragovernance.ErrInvalidInput), errors.Is(err, infragovernance.ErrInvalidTransition):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case errors.Is(err, infragovernance.ErrForbidden):
		c.JSON(http.StatusForbidden, gin.H{"error": "permission denied"})
	case errors.Is(err, infragovernance.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "resource not found"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
	}
}
