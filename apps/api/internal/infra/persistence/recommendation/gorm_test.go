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
