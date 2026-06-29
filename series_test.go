// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"reflect"
	"testing"
)

func TestMerge(t *testing.T) {
	var a, b series
	a.Samples.add(1, 1, 1, 1)
	b.Samples.add(1, 2, 2, 1)
	b.Samples.Buckets = []sampleBucket{{Time: 10, Count: 1, Sum: 2, Min: 2, Max: 2, First: 2, Last: 2}}
	a.Samples.Buckets = []sampleBucket{{Time: 10, Count: 1, Sum: 4, Min: 4, Max: 4, First: 4, Last: 4}}
	a.merge(&b)
	if a.Samples.Data[0] != 2 {
		t.Fatal("newer sample did not win")
	}
	got := a.Samples.Buckets[0]
	if got.Count != 2 || got.Sum != 6 || got.Min != 2 || got.Max != 4 || got.First != 4 || got.Last != 2 {
		t.Fatalf("bucket merge: %+v", got)
	}

	a, b = series{}, series{}
	a.Counters.add(1, 1, 1, 2)
	b.Counters.add(1, 1, 1, 2)
	a.merge(&b)
	values := a.Counters.values(1, 1)
	if len(values) != 1 || !reflect.DeepEqual([]float64{values[0].value}, []float64{2}) {
		t.Fatalf("counter merge: %v", values)
	}
}

func TestSeriesAppend(t *testing.T) {
	var a, b series
	b.Samples.add(1, 1, 1, 1)
	b.Samples.Buckets = []sampleBucket{{Time: 1, Count: 1, Sum: 1}}
	b.Counters.add(1, 1, 1, 1)
	b.Counters.Buckets = []counterBucket{{Time: 1, Sum: 1}}
	a.append(nil)
	a.append(&b)
	if len(a.Samples.Time) != 1 || len(a.Samples.Buckets) != 1 || len(a.Counters.Time) != 1 || len(a.Counters.Buckets) != 1 {
		t.Fatalf("append: %+v", a)
	}
}

func FuzzSampleMerge(f *testing.F) {
	f.Add(uint64(1), float64(1), uint64(1), uint64(2), float64(2), uint64(2))
	f.Fuzz(func(t *testing.T, ts uint64, v1 float64, c1 uint64, r uint64, v2 float64, c2 uint64) {
		var a, b, ab, ba series
		a.Samples.add(ts, v1, c1, r)
		b.Samples.add(ts, v2, c2, r+1)
		ab.merge(&a)
		ab.merge(&b)
		ba.merge(&b)
		ba.merge(&a)
		if len(ab.Samples.Data) != 1 || len(ba.Samples.Data) != 1 || ab.Samples.Data[0] != ba.Samples.Data[0] {
			t.Fatalf("sample merge not commutative: %+v %+v", ab.Samples, ba.Samples)
		}
		ab.merge(&a)
		if len(ab.Samples.Data) != 1 {
			t.Fatalf("sample merge not idempotent: %+v", ab.Samples)
		}
	})
}

func FuzzCounterMerge(f *testing.F) {
	f.Add(uint64(1), uint64(1), uint64(2), uint64(3))
	f.Fuzz(func(t *testing.T, ts, replica, clock, value uint64) {
		var a, b, c, left, right series
		a.Counters.add(ts, replica, clock, value)
		b.Counters.add(ts, replica+1, clock+1, value+1)
		c.Counters.add(ts+1, replica, clock+2, value+2)
		left.merge(&a)
		left.merge(&b)
		left.merge(&c)
		right.merge(&b)
		right.merge(&c)
		right.merge(&a)
		if len(left.Counters.values(0, ^uint64(0))) != len(right.Counters.values(0, ^uint64(0))) {
			t.Fatalf("counter merge not associative/commutative")
		}
		left.merge(&a)
		if len(left.Counters.values(ts, ts)) == 0 {
			t.Fatalf("counter merge lost value")
		}
	})
}
