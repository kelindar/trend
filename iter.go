// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"iter"
	"time"
)

type point struct {
	at    time.Time
	value float64
}

func points(items []point) iter.Seq2[time.Time, float64] {
	return func(yield func(time.Time, float64) bool) {
		for _, item := range items {
			if !yield(item.at, item.value) {
				return
			}
		}
	}
}
