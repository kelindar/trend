// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

// Agg identifies a fixed aggregation.
type Agg uint8

const (
	Sum Agg = iota
	Count
	Min
	Max
	Mean
	First
	Last
)

type fold struct {
	count uint64
	sum   float64
	min   float64
	max   float64
	first float64
	last  float64
}

func (f *fold) Add(v float64) {
	if f.count == 0 {
		f.min, f.max, f.first = v, v, v
	}
	f.count++
	f.sum += v
	if v < f.min {
		f.min = v
	}
	if v > f.max {
		f.max = v
	}
	f.last = v
}

func (f *fold) Merge(b sampleBucket) {
	if b.Count == 0 {
		return
	}
	if f.count == 0 {
		f.min, f.max, f.first = b.Min, b.Max, b.First
	}
	f.count += b.Count
	f.sum += b.Sum
	if b.Min < f.min {
		f.min = b.Min
	}
	if b.Max > f.max {
		f.max = b.Max
	}
	f.last = b.Last
}

func (f fold) Value(agg Agg) float64 {
	switch agg {
	case Count:
		return float64(f.count)
	case Min:
		return f.min
	case Max:
		return f.max
	case Mean:
		if f.count == 0 {
			return 0
		}
		return f.sum / float64(f.count)
	case First:
		return f.first
	case Last:
		return f.last
	default:
		return f.sum
	}
}
