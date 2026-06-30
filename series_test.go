// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"reflect"
	"testing"
)

func TestMerge(t *testing.T) {
	var a, b series
	a.Samples.Add(1, 1, 1, 1)
	b.Samples.Add(1, 2, 2, 1)
	b.Samples.Buckets = []sampleBucket{{Time: 10, Count: 1, Sum: 2, Min: 2, Max: 2, First: 2, Last: 2}}
	a.Samples.Buckets = []sampleBucket{{Time: 10, Count: 1, Sum: 4, Min: 4, Max: 4, First: 4, Last: 4}}
	a.Merge(&b)
	if a.Samples.Data[0] != 2 {
		t.Fatal("newer sample did not win")
	}
	got := a.Samples.Buckets[0]
	if got.Count != 2 || got.Sum != 6 || got.Min != 2 || got.Max != 4 || got.First != 4 || got.Last != 2 {
		t.Fatalf("bucket merge: %+v", got)
	}

	a, b = series{}, series{}
	a.Counters.Add(1, 1, 1, 2)
	b.Counters.Add(1, 1, 1, 2)
	a.Merge(&b)
	values := collect(t, a.Counters.values(1, 1))
	if len(values) != 1 || !reflect.DeepEqual(values, []float64{2}) {
		t.Fatalf("counter merge: %v", values)
	}
}

func TestSeriesAppend(t *testing.T) {
	var a, b series
	b.Samples.Add(1, 1, 1, 1)
	b.Samples.Buckets = []sampleBucket{{Time: 1, Count: 1, Sum: 1}}
	b.Counters.Add(1, 1, 1, 1)
	b.Counters.Buckets = []counterBucket{{Time: 1, Sum: 1}}
	a.Append(nil)
	a.Append(&b)
	if len(a.Samples.Time) != 1 || len(a.Samples.Buckets) != 1 || len(a.Counters.Time) != 1 || len(a.Counters.Buckets) != 1 {
		t.Fatalf("append: %+v", a)
	}
}

func FuzzSampleMerge(f *testing.F) {
	f.Add(uint64(1), float64(1), uint64(1), uint64(2), float64(2), uint64(2))
	f.Fuzz(func(t *testing.T, ts uint64, v1 float64, c1 uint64, r uint64, v2 float64, c2 uint64) {
		var a, b, ab, ba series
		a.Samples.Add(ts, v1, c1, r)
		b.Samples.Add(ts, v2, c2, r+1)
		ab.Merge(&a)
		ab.Merge(&b)
		ba.Merge(&b)
		ba.Merge(&a)
		if len(ab.Samples.Data) != 1 || len(ba.Samples.Data) != 1 || ab.Samples.Data[0] != ba.Samples.Data[0] {
			t.Fatalf("sample merge not commutative: %+v %+v", ab.Samples, ba.Samples)
		}
		ab.Merge(&a)
		if len(ab.Samples.Data) != 1 {
			t.Fatalf("sample merge not idempotent: %+v", ab.Samples)
		}
	})
}

func FuzzCounterMerge(f *testing.F) {
	f.Add(uint64(1), uint64(1), uint64(2), uint64(3))
	f.Fuzz(func(t *testing.T, ts, replica, clock, value uint64) {
		var a, b, c, left, right series
		a.Counters.Add(ts, replica, clock, value)
		b.Counters.Add(ts, replica+1, clock+1, value+1)
		c.Counters.Add(ts+1, replica, clock+2, value+2)
		left.Merge(&a)
		left.Merge(&b)
		left.Merge(&c)
		right.Merge(&b)
		right.Merge(&c)
		right.Merge(&a)
		if len(collect(t, left.Counters.values(0, ^uint64(0)))) != len(collect(t, right.Counters.values(0, ^uint64(0)))) {
			t.Fatalf("counter merge not associative/commutative")
		}
		left.Merge(&a)
		if len(collect(t, left.Counters.values(ts, ts))) == 0 {
			t.Fatalf("counter merge lost value")
		}
	})
}
