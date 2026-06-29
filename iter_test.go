// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"testing"
	"time"
)

func TestIter(t *testing.T) {
	var got []int
	points([]point{{value: 1}, {value: 2}})(func(_ time.Time, value float64) bool {
		got = append(got, int(value))
		return false
	})
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("iter: %v", got)
	}
}
