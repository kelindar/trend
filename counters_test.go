// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCounters(t *testing.T) {
	var s pending
	s.Counters.Add(1, 1, 1, 2)
	s.Counters.Buckets = []counterBucket{{Time: 10, Sum: 3}}
	got := collectCall(t, func(y func(time.Time, float64) bool) {
		s.Counters.rangeValues(1, 10, 0, Sum, y)
	})
	assert.Len(t, got, 2)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		s.Counters.rangeValues(1, 10, 10, Count, y)
	})
	assert.Len(t, got, 2)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		s.Counters.rangeValues(1, 10, 10, Sum, y)
	})
	assert.Equal(t, []float64{2, 3}, got)

	var raw pending
	raw.Counters.Add(1, 1, 1, 1)
	raw.Counters.Add(61, 1, 2, 2)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		raw.Counters.rangeValues(2, 120, 60, Sum, y)
	})
	assert.Equal(t, []float64{2}, got)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		raw.Counters.rangeValues(200, 300, 60, Sum, y)
	})
	assert.Empty(t, got)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		raw.Counters.rangeValues(1, 120, 60, Sum, y)
	})
	assert.Len(t, got, 2)
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
	assert.Equal(t, 1, calls)
	calls = 0
	one := counterData{Time: []uint64{1}, Value: []uint64{1}}
	one.values(1, 1, func(time.Time, float64) bool {
		calls++
		return false
	})
	assert.Equal(t, 1, calls)
	mixed := counterData{
		Time:  []uint64{1, 2, 2},
		Value: []uint64{1, 2, 3},
		Buckets: []counterBucket{
			{Time: 1, Sum: 1},
			{Time: 2, Sum: 4},
		},
	}
	got := collectCall(t, func(y func(time.Time, float64) bool) {
		mixed.values(2, 2, y)
	})
	assert.Equal(t, []float64{9}, got)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		one := counterData{Time: []uint64{2}, Value: []uint64{1}}
		one.values(1, 1, y)
	})
	assert.Empty(t, got)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		one := counterData{Buckets: []counterBucket{{Time: 2, Sum: 1}}}
		one.values(1, 1, y)
	})
	assert.Empty(t, got)

	calls = 0
	counters.rangeValues(1, 61, 60, Sum, func(time.Time, float64) bool {
		calls++
		return false
	})
	assert.Equal(t, 1, calls)

	counters.Buckets = []counterBucket{{Time: 1, Sum: 1}}
	calls = 0
	counters.rangeValues(1, 61, 60, Sum, func(time.Time, float64) bool {
		calls++
		return false
	})
	assert.Equal(t, 1, calls)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		mixed.rangeValues(2, 2, 60, Sum, y)
	})
	assert.Equal(t, []float64{9}, got)

	buckets := counterData{
		Buckets: []counterBucket{{Time: 1, Sum: 1}, {Time: 2, Sum: 2}},
	}
	calls = 0
	buckets.values(1, 2, func(time.Time, float64) bool {
		calls++
		return false
	})
	assert.Equal(t, 1, calls)

	buckets = counterData{
		Buckets: []counterBucket{{Time: 1, Sum: 1}, {Time: 61, Sum: 2}},
	}
	calls = 0
	buckets.rangeValues(1, 61, 60, Sum, func(time.Time, float64) bool {
		calls++
		return false
	})
	assert.Equal(t, 1, calls)
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
	assert.Len(t, counters.Time, 1)
	assert.Len(t, counters.Buckets, 2)
	assert.Len(t, mergeCounterBuckets([]counterBucket{{Time: 1, Sum: 1}}, []counterBucket{{Time: 2, Sum: 2}}), 2)
}

func TestCounterErrors(t *testing.T) {
	store := newMemStore()
	store.loadErr = errTest
	db, _ := New(store)
	now := time.Now()
	_, err := db.Counters("x").Values(context.Background(), now, now)
	assert.ErrorIs(t, err, errTest)
	_, err = db.Counters("x").Range(context.Background(), now, now, time.Second, Sum)
	assert.ErrorIs(t, err, errTest)
}
