// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"reflect"
	"testing"
	"time"
)

func TestAgg(t *testing.T) {
	var f fold
	f.Add(3)
	f.Add(1)
	f.Add(5)
	if got := []float64{
		f.Value(Sum),
		f.Value(Count),
		f.Value(Min),
		f.Value(Max),
		f.Value(Mean),
		f.Value(First),
		f.Value(Last),
	}; !reflect.DeepEqual(got, []float64{9, 3, 1, 5, 3, 3, 5}) {
		t.Fatalf("agg values: %v", got)
	}
	if (fold{}).Value(Mean) != 0 {
		t.Fatal("empty mean should be zero")
	}
	var merged fold
	merged.Merge(sampleBucket{})
	merged.Merge(sampleBucket{Count: 1, Sum: 10, Min: 10, Max: 10, First: 10, Last: 10})
	merged.Merge(sampleBucket{Count: 1, Sum: 1, Min: 1, Max: 11, First: 1, Last: 1})
	if merged.min != 1 || merged.max != 11 || merged.first != 10 || merged.last != 1 {
		t.Fatalf("merge: %+v", merged)
	}
}

var (
	iterBenchCount int
	iterBenchSum   float64
)

func iterBenchYield(_ time.Time, value float64) bool {
	iterBenchCount++
	iterBenchSum += value
	return true
}

func BenchmarkSampleIterators(b *testing.B) {
	data := sampleData{}
	for i := uint64(0); i < 1024; i++ {
		data.Add(i*10, float64(i), i, 1)
	}
	for i := uint64(0); i < 256; i++ {
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
		for i := 0; i < b.N; i++ {
			data.values(1000, 8000)(iterBenchYield)
		}
	})
	b.Run("range_raw", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			raw.rangeValues(1000, 8000, 60, Sum)(iterBenchYield)
		}
	})
	b.Run("range_mixed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			data.rangeValues(1000, 8000, 60, Sum)(iterBenchYield)
		}
	})
}

func BenchmarkCounterIterators(b *testing.B) {
	data := counterData{}
	for i := uint64(0); i < 1024; i++ {
		data.Add(i*10, 1, i, i)
	}
	for i := uint64(0); i < 256; i++ {
		data.Buckets = append(data.Buckets, counterBucket{
			Time: i * 40,
			Sum:  i * 4,
		})
	}
	raw := data
	raw.Buckets = nil

	b.Run("values", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			data.values(1000, 8000)(iterBenchYield)
		}
	})
	b.Run("range_raw", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			raw.rangeValues(1000, 8000, 60, Sum)(iterBenchYield)
		}
	})
	b.Run("range_mixed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			data.rangeValues(1000, 8000, 60, Sum)(iterBenchYield)
		}
	})
}
