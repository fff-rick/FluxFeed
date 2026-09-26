package interfaceshttpgovernance

import (
	applicationgovernance "FluxFeed/internal/application/governance"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	switches *applicationgovernance.Switches
}

type updateSwitchRequest struct {
	Enabled *bool `json:"enabled"`
}

func New(switches *applicationgovernance.Switches) *Handler {
	return &Handler{switches: switches}
}

func (h *Handler) List(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"switches": h.switches.Snapshot()})
}

func (h *Handler) Update(c *gin.Context) {
	var req updateSwitchRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.Enabled == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "enabled is required"})
		return
	}
	key := c.Param("key")
	if !h.switches.Set(key, *req.Enabled) {
		c.JSON(http.StatusNotFound, gin.H{"error": "degrade switch not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key, "enabled": *req.Enabled})
}
