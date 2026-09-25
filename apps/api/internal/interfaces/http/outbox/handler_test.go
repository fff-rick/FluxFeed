package interfaceshttpoutbox

import (
	infraoutbox "FluxFeed/internal/infra/persistence/outbox"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

type memoryStore struct {
	status string
	limit  int
}

func (s *memoryStore) ListEvents(_ context.Context, status string, limit int) ([]*infraoutbox.EventRecord, error) {
	s.status, s.limit = status, limit
	return []*infraoutbox.EventRecord{{EventID: "evt-1", Status: status}}, nil
}

func TestListDefaultsToFailedEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &memoryStore{}
	router := gin.New()
	router.GET("/internal/outbox-events", New(store).List)

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal/outbox-events", nil))
	if response.Code != http.StatusOK || store.status != "failed" || store.limit != 50 {
		t.Fatalf("code=%d status=%q limit=%d", response.Code, store.status, store.limit)
	}
}
