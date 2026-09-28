package infrarecommendation

import (
	"context"
	"math"
	"os"
	"strings"
	"testing"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestFeedbackFilterSeparatesVideoAndAuthorExclusions(t *testing.T) {
	filter := feedbackFilter("candidate.video_id", "candidate.author_id")
	for _, expected := range []string{
		"negative_video_event.video_id = candidate.video_id",
		"candidate.author_id NOT IN",
		"hidden_video.author_id",
	} {
		if !strings.Contains(filter, expected) {
			t.Fatalf("feedback filter missing %q: %s", expected, filter)
		}
	}
	if strings.Contains(filter, " OR ") {
		t.Fatalf("feedback filter must keep video and author exclusions in separate anti-joins: %s", filter)
	}
}

func TestNewUserInterestModelMaterializesEmptyAndPopulatedProfiles(t *testing.T) {
	tests := []struct {
		name      string
		vector    []float64
		dimension int
		json      string
	}{
		{name: "empty", vector: nil, dimension: 0, json: `[]`},
		{name: "populated", vector: []float64{0.25, 0.75}, dimension: 2, json: `[0.25,0.75]`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model, err := newUserInterestModel(42, test.vector)
			if err != nil {
				t.Fatal(err)
			}
			if model.UserID != 42 || model.Dimension != test.dimension || model.EmbeddingJSON != test.json {
				t.Fatalf("unexpected materialized profile: %+v", model)
			}
		})
	}
}

func TestLoadUserInterestVectorMaterializesEmptyProfileIntegration(t *testing.T) {
	dsn := os.Getenv("FLUXFEED_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("FLUXFEED_MYSQL_TEST_DSN is not set")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()

	const userID int64 = 9_000_000_001
	if err := db.Where("user_id = ?", userID).Delete(&UserInterestModel{}).Error; err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Where("user_id = ?", userID).Delete(&UserInterestModel{}).Error; err != nil {
			t.Errorf("clean materialized profile: %v", err)
		}
	}()

	repository := New(db)
	for attempt := 0; attempt < 2; attempt++ {
		vector, ok, err := repository.LoadUserInterestVector(context.Background(), userID)
		if err != nil || ok || len(vector) != 0 {
			t.Fatalf("unexpected empty profile load: vector=%v ok=%v err=%v", vector, ok, err)
		}
	}
	var materialized UserInterestModel
	if err := db.Where("user_id = ?", userID).Take(&materialized).Error; err != nil {
		t.Fatal(err)
	}
	if materialized.Dimension != 0 || materialized.EmbeddingJSON != `[]` {
		t.Fatalf("unexpected stored empty profile: %+v", materialized)
	}

	if err := repository.saveUserInterestVector(context.Background(), userID, []float64{0.25, 0.75}); err != nil {
		t.Fatal(err)
	}
	vector, ok, err := repository.LoadUserInterestVector(context.Background(), userID)
	if err != nil || !ok || len(vector) != 2 || vector[0] != 0.25 || vector[1] != 0.75 {
		t.Fatalf("unexpected populated profile load: vector=%v ok=%v err=%v", vector, ok, err)
	}
	if err := repository.RefreshUserInterestVector(context.Background(), userID); err != nil {
		t.Fatal(err)
	}
	if err := db.Where("user_id = ?", userID).Take(&materialized).Error; err != nil {
		t.Fatal(err)
	}
	if materialized.Dimension != 0 || materialized.EmbeddingJSON != `[]` {
		t.Fatalf("refresh must retain an empty materialization: %+v", materialized)
	}
}

func TestVectorAccumulatorUsesSignalWeights(t *testing.T) {
	accumulator := &vectorAccumulator{}
	accumulator.Add(`[1,0]`, 3)
	accumulator.Add(`[0,1]`, 1)
	vector, ok, err := accumulator.Average()
	if err != nil || !ok || len(vector) != 2 || math.Abs(vector[0]-0.75) > 0.0001 || math.Abs(vector[1]-0.25) > 0.0001 {
		t.Fatalf("unexpected weighted vector: %v, ok=%v, err=%v", vector, ok, err)
	}
}

func TestEventWeightDistinguishesFeedbackStrength(t *testing.T) {
	click := eventWeight("click", 0, false)
	play := eventWeight("play", 5000, false)
	validPlay := eventWeight("valid_play", 5000, false)
	finish := eventWeight("finish", 5000, true)
	if !(click < play && play < validPlay && validPlay < finish) {
		t.Fatalf("unexpected behavior weights: click=%v play=%v valid=%v finish=%v", click, play, validPlay, finish)
	}
}
