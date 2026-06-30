// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCounters(t *testing.T) {
	var s series
	s.Counters.Add(1, 1, 1, 2)
	s.Counters.Buckets = []counterBucket{{Time: 10, Sum: 3}}
	if got := collectCall(t, func(y func(time.Time, float64) bool) {
		s.Counters.rangeValues(1, 10, 0, Sum, y)
	}); len(got) != 2 {
		t.Fatalf("zero counter span: %v", got)
	}
	if got := collectCall(t, func(y func(time.Time, float64) bool) {
		s.Counters.rangeValues(1, 10, 10, Count, y)
	}); len(got) != 2 {
		t.Fatalf("counter range: %v", got)
	}
	if got := collectCall(t, func(y func(time.Time, float64) bool) {
		s.Counters.rangeValues(1, 10, 10, Sum, y)
	}); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("counter range sum: %v", got)
	}

	var raw series
	raw.Counters.Add(1, 1, 1, 1)
	raw.Counters.Add(61, 1, 2, 2)
	if got := collectCall(t, func(y func(time.Time, float64) bool) {
		raw.Counters.rangeValues(2, 120, 60, Sum, y)
	}); len(got) != 1 || got[0] != 2 {
		t.Fatalf("raw counter range: %v", got)
	}
	if got := collectCall(t, func(y func(time.Time, float64) bool) {
		raw.Counters.rangeValues(200, 300, 60, Sum, y)
	}); len(got) != 0 {
		t.Fatalf("empty raw counter range: %v", got)
	}
	if got := collectCall(t, func(y func(time.Time, float64) bool) {
		raw.Counters.rangeValues(1, 120, 60, Sum, y)
	}); len(got) != 2 {
		t.Fatalf("raw counter buckets: %v", got)
	}
}

func TestCounterIteratorsStop(t *testing.T) {
	counters := counterData{
		Time:  []uint64{1, 61},
		Value: []uint64{1, 2},
	}
	calls := 0
	counters.values(1, 61, func(time.Time, float64) bool {
		calls++
		return false
	})
	if calls != 1 {
		t.Fatalf("counter values stop: %d", calls)
	}
	calls = 0
	counterData{Time: []uint64{1}, Value: []uint64{1}}.values(1, 1, func(time.Time, float64) bool {
		calls++
		return false
	})
	if calls != 1 {
		t.Fatalf("raw counter values stop: %d", calls)
	}
	mixed := counterData{
		Time:  []uint64{1, 2, 2},
		Value: []uint64{1, 2, 3},
		Buckets: []counterBucket{
			{Time: 1, Sum: 1},
			{Time: 2, Sum: 4},
		},
	}
	if got := collectCall(t, func(y func(time.Time, float64) bool) {
		mixed.values(2, 2, y)
	}); len(got) != 1 || got[0] != 9 {
		t.Fatalf("counter values merge: %v", got)
	}
	if got := collectCall(t, func(y func(time.Time, float64) bool) {
		counterData{Time: []uint64{2}, Value: []uint64{1}}.values(1, 1, y)
	}); len(got) != 0 {
		t.Fatalf("counter values raw cutoff: %v", got)
	}
	if got := collectCall(t, func(y func(time.Time, float64) bool) {
		counterData{Buckets: []counterBucket{{Time: 2, Sum: 1}}}.values(1, 1, y)
	}); len(got) != 0 {
		t.Fatalf("counter values bucket cutoff: %v", got)
	}

	calls = 0
	counters.rangeValues(1, 61, 60, Sum, func(time.Time, float64) bool {
		calls++
		return false
	})
	if calls != 1 {
		t.Fatalf("counter range stop: %d", calls)
	}

	counters.Buckets = []counterBucket{{Time: 1, Sum: 1}}
	calls = 0
	counters.rangeValues(1, 61, 60, Sum, func(time.Time, float64) bool {
		calls++
		return false
	})
	if calls != 1 {
		t.Fatalf("bucket counter range stop: %d", calls)
	}
	if got := collectCall(t, func(y func(time.Time, float64) bool) {
		mixed.rangeValues(2, 2, 60, Sum, y)
	}); len(got) != 1 || got[0] != 9 {
		t.Fatalf("counter range merge: %v", got)
	}
}

func TestCounterState(t *testing.T) {
	counters := counterData{
		Time:    []uint64{10, 20},
		Replica: []uint64{1, 1},
		Clock:   []uint64{1, 2},
		Value:   []uint64{1, 2},
		Buckets: []counterBucket{{Time: 1, Sum: 1}},
	}
	counters.Compact(15, 10)
	if len(counters.Time) != 1 || len(counters.Buckets) != 2 {
		t.Fatalf("counter compact: %+v", counters)
	}
	if got := mergeCounterBuckets([]counterBucket{{Time: 1, Sum: 1}}, []counterBucket{{Time: 2, Sum: 2}}); len(got) != 2 {
		t.Fatalf("counter bucket merge: %v", got)
	}
}

func TestCounterErrors(t *testing.T) {
	store := newMemStore()
	store.loadErr = errTest
	db, _ := New(store)
	now := time.Now()
	if _, err := db.Counters("x").Values(context.Background(), now, now); !errors.Is(err, errTest) {
		t.Fatal("expected counter values error")
	}
	if _, err := db.Counters("x").Range(context.Background(), now, now, time.Second, Sum); !errors.Is(err, errTest) {
		t.Fatal("expected counter range error")
	}
}
