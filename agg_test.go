// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"reflect"
	"testing"
)

func TestAgg(t *testing.T) {
	var f fold
	f.add(3)
	f.add(1)
	f.add(5)
	if got := []float64{
		f.value(Sum),
		f.value(Count),
		f.value(Min),
		f.value(Max),
		f.value(Mean),
		f.value(First),
		f.value(Last),
	}; !reflect.DeepEqual(got, []float64{9, 3, 1, 5, 3, 3, 5}) {
		t.Fatalf("agg values: %v", got)
	}
	if (fold{}).value(Mean) != 0 {
		t.Fatal("empty mean should be zero")
	}
	var merged fold
	merged.merge(sampleBucket{})
	merged.merge(sampleBucket{Count: 1, Sum: 10, Min: 10, Max: 10, First: 10, Last: 10})
	merged.merge(sampleBucket{Count: 1, Sum: 1, Min: 1, Max: 11, First: 1, Last: 1})
	if merged.min != 1 || merged.max != 11 || merged.first != 10 || merged.last != 1 {
		t.Fatalf("merge: %+v", merged)
	}
}
