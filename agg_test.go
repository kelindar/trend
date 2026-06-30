// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"iter"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAgg(t *testing.T) {
	var f fold
	f.Add(3)
	f.Add(1)
	f.Add(5)
	got := []float64{
		f.Value(Sum),
		f.Value(Count),
		f.Value(Min),
		f.Value(Max),
		f.Value(Mean),
		f.Value(First),
		f.Value(Last),
	}
	assert.Equal(t, []float64{9, 3, 1, 5, 3, 3, 5}, got)
	assert.Zero(t, (fold{}).Value(Mean))

	var merged fold
	merged.Merge(sampleBucket{})
	merged.Merge(sampleBucket{Count: 1, Sum: 10, Min: 10, Max: 10, First: 10, Last: 10})
	merged.Merge(sampleBucket{Count: 1, Sum: 1, Min: 1, Max: 11, First: 1, Last: 1})
	assert.Equal(t, 1.0, merged.min)
	assert.Equal(t, 11.0, merged.max)
	assert.Equal(t, 10.0, merged.first)
	assert.Equal(t, 1.0, merged.last)
}

var (
	iterBenchCount int
	iterBenchSum   float64
	iterBenchSeq   iter.Seq2[time.Time, float64]
)

func iterBenchYield(_ time.Time, value float64) bool {
	iterBenchCount++
	iterBenchSum += value
	return true
}

func BenchmarkSampleIterators(b *testing.B) {
	data := sampleData{}
	for i := range uint64(1024) {
		data.Add(i*10, float64(i), i, 1)
	}
	for i := range uint64(256) {
		data.Buckets = append(data.Buckets, sampleBucket{
			Time:  i * 40,
			Count: 4,
			Sum:   float64(i * 4),
			Min:   float64(i),
			Max:   float64(i + 3),
			First: float64(i),
			Last:  float64(i + 3),
		})
	}
	raw := data
	raw.Buckets = nil

	b.Run("values", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			data.values(1000, 8000, iterBenchYield)
		}
	})
	b.Run("values_escape", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			iterBenchSeq = func(yield func(time.Time, float64) bool) {
				data.values(1000, 8000, yield)
			}
		}
	})
	b.Run("range_raw", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			raw.rangeValues(1000, 8000, 60, Sum, iterBenchYield)
		}
	})
	b.Run("range_raw_escape", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			iterBenchSeq = func(yield func(time.Time, float64) bool) {
				raw.rangeValues(1000, 8000, 60, Sum, yield)
			}
		}
	})
	b.Run("range_mixed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			data.rangeValues(1000, 8000, 60, Sum, iterBenchYield)
		}
	})
	b.Run("range_mixed_escape", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			iterBenchSeq = func(yield func(time.Time, float64) bool) {
				data.rangeValues(1000, 8000, 60, Sum, yield)
			}
		}
	})
}

func BenchmarkCounterIterators(b *testing.B) {
	data := counterData{}
	for i := range uint64(1024) {
		data.Add(i*10, 1, i, i)
	}
	for i := range uint64(256) {
		data.Buckets = append(data.Buckets, counterBucket{
			Time: i * 40,
			Sum:  i * 4,
		})
	}
	raw := data
	raw.Buckets = nil

	b.Run("values", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			data.values(1000, 8000, iterBenchYield)
		}
	})
	b.Run("values_escape", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			iterBenchSeq = func(yield func(time.Time, float64) bool) {
				data.values(1000, 8000, yield)
			}
		}
	})
	b.Run("range_raw", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			raw.rangeValues(1000, 8000, 60, Sum, iterBenchYield)
		}
	})
	b.Run("range_raw_escape", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			iterBenchSeq = func(yield func(time.Time, float64) bool) {
				raw.rangeValues(1000, 8000, 60, Sum, yield)
			}
		}
	})
	b.Run("range_mixed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			data.rangeValues(1000, 8000, 60, Sum, iterBenchYield)
		}
	})
	b.Run("range_mixed_escape", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			iterBenchSeq = func(yield func(time.Time, float64) bool) {
				data.rangeValues(1000, 8000, 60, Sum, yield)
			}
		}
	})
}
