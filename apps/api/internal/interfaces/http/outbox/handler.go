package interfaceshttpoutbox

import (
	infraoutbox "FluxFeed/internal/infra/persistence/outbox"
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type Store interface {
	ListEvents(ctx context.Context, status string, limit int) ([]*infraoutbox.EventRecord, error)
}

type Handler struct{ store Store }

func New(store Store) *Handler { return &Handler{store: store} }

func (h *Handler) List(c *gin.Context) {
	status := strings.TrimSpace(c.Query("status"))
	if status == "" {
		status = "failed"
	}
	limit := 50
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > 100 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
			return
		}
		limit = parsed
	}
	events, err := h.store.ListEvents(c.Request.Context(), status, limit)
	if errors.Is(err, infraoutbox.ErrInvalidStatus) {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": events})
}
