// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSamples(t *testing.T) {
	var s series
	s.Samples.Add(1, 1, 1, 1)
	s.Samples.Add(2, 3, 2, 1)
	s.Samples.Buckets = []sampleBucket{{Time: 10, Count: 1, Sum: 4, Min: 4, Max: 4, First: 4, Last: 4}}
	if got := s.Samples.rangeValues(1, 10, 0, Sum); len(got) != 3 {
		t.Fatalf("zero sample span: %v", got)
	}
	if got := s.Samples.rangeValues(1, 10, 10, Max); len(got) != 2 {
		t.Fatalf("sample range: %v", got)
	}

	var raw series
	raw.Samples.Add(1, 1, 1, 1)
	raw.Samples.Add(61, 2, 2, 1)
	if got := raw.Samples.rangeValues(2, 120, 60, Sum); len(got) != 1 || got[0].value != 2 {
		t.Fatalf("raw sample range: %v", got)
	}
	if got := raw.Samples.rangeValues(200, 300, 60, Sum); len(got) != 0 {
		t.Fatalf("empty raw sample range: %v", got)
	}
	if got := raw.Samples.rangeValues(1, 120, 60, Sum); len(got) != 2 {
		t.Fatalf("raw sample buckets: %v", got)
	}
}

func TestSampleState(t *testing.T) {
	if bucketOf(10, 0) != 10 {
		t.Fatal("zero span bucket")
	}
	var s series
	s.Merge(nil)
	s.Compact(time.Now(), 0)

	a := combineSampleBucket(sampleBucket{}, sampleBucket{Count: 1, Sum: 1})
	b := combineSampleBucket(a, sampleBucket{})
	if a.Count != 1 || b.Count != 1 {
		t.Fatal("empty bucket combine")
	}
	c := combineSampleBucket(sampleBucket{Count: 1, Sum: 1, Min: 1, Max: 1}, sampleBucket{Count: 1, Sum: 2, Min: 2, Max: 3})
	if c.Max != 3 {
		t.Fatal("max branch not merged")
	}

	var samples sampleData
	samples.Compact(1, 1)
	samples = sampleData{
		Time:    []uint64{1, 2, 3, 20},
		Data:    []float64{5, 1, 7, 9},
		Clock:   []uint64{1, 2, 3, 4},
		Replica: []uint64{1, 1, 1, 1},
		Buckets: []sampleBucket{{Time: 0, Count: 1, Sum: 2, Min: 2, Max: 2, First: 2, Last: 2}},
	}
	samples.Compact(10, 10)
	if len(samples.Time) != 1 || samples.Buckets[0].Min != 1 || samples.Buckets[0].Max != 7 {
		t.Fatalf("sample compact: %+v", samples.Buckets)
	}
	if got := mergeSampleBuckets(nil, []sampleBucket{{Time: 1, Count: 1}}); len(got) != 1 {
		t.Fatalf("sample bucket insert: %v", got)
	}
}

func TestSampleErrors(t *testing.T) {
	store := newMemStore()
	store.loadErr = errTest
	db, _ := New(store)
	now := time.Now()
	if _, err := db.Samples("x").Range(context.Background(), now, now, time.Second, Sum); !errors.Is(err, errTest) {
		t.Fatal("expected sample range error")
	}
}
