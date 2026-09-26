package infrarecommendation

import (
	"math"
	"testing"
)

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
