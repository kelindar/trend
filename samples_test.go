// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSamples(t *testing.T) {
	var s pending
	s.Samples.Add(1, 1, 1, 1)
	s.Samples.Add(2, 3, 2, 1)
	s.Samples.Buckets = []sampleBucket{{Time: 10, Count: 1, Sum: 4, Min: 4, Max: 4, First: 4, Last: 4}}
	got := collectCall(t, func(y func(time.Time, float64) bool) {
		s.Samples.rangeValues(1, 10, 0, Sum, y)
	})
	assert.Len(t, got, 3)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		s.Samples.rangeValues(1, 10, 10, Max, y)
	})
	assert.Len(t, got, 2)

	var raw pending
	raw.Samples.Add(1, 1, 1, 1)
	raw.Samples.Add(61, 2, 2, 1)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		raw.Samples.rangeValues(2, 120, 60, Sum, y)
	})
	assert.Equal(t, []float64{2}, got)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		raw.Samples.rangeValues(200, 300, 60, Sum, y)
	})
	assert.Empty(t, got)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		raw.Samples.rangeValues(1, 120, 60, Sum, y)
	})
	assert.Len(t, got, 2)
}

func TestSampleIteratorsStop(t *testing.T) {
	buffered := sampleData{
		Time: []uint64{2},
		Data: []float64{2},
		Buckets: []sampleBucket{
			{Time: 1, Count: 1, Sum: 1},
		},
	}
	calls := 0
	buffered.values(1, 2, func(time.Time, float64) bool {
		calls++
		return false
	})
	assert.Equal(t, 1, calls)

	raw := sampleData{
		Time: []uint64{1, 61},
		Data: []float64{1, 2},
	}
	calls = 0
	raw.values(1, 61, func(time.Time, float64) bool {
		calls++
		return false
	})
	assert.Equal(t, 1, calls)
	got := collectCall(t, func(y func(time.Time, float64) bool) {
		buffered.values(2, 2, y)
	})
	assert.Equal(t, []float64{2}, got)
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		raw.values(61, 61, y)
	})
	assert.Equal(t, []float64{2}, got)

	calls = 0
	raw.rangeValues(1, 61, 60, Sum, func(time.Time, float64) bool {
		calls++
		return false
	})
	assert.Equal(t, 1, calls)

	calls = 0
	buffered.rangeValues(1, 2, 60, Sum, func(time.Time, float64) bool {
		calls++
		return false
	})
	assert.Equal(t, 1, calls)
	mixed := sampleData{
		Time: []uint64{1, 2},
		Data: []float64{1, 2},
		Buckets: []sampleBucket{
			{Time: 1, Count: 1, Sum: 1},
			{Time: 2, Count: 1, Sum: 4},
		},
	}
	got = collectCall(t, func(y func(time.Time, float64) bool) {
		mixed.rangeValues(2, 2, 60, Sum, y)
	})
	assert.Equal(t, []float64{6}, got)
}

func TestSampleState(t *testing.T) {
	assert.Equal(t, uint64(10), bucketOf(10, 0))
	var s pending
	s.Merge(nil)
	s.Compact(time.Now(), 0)

	a := combineSampleBucket(sampleBucket{}, sampleBucket{Count: 1, Sum: 1})
	b := combineSampleBucket(a, sampleBucket{})
	assert.Equal(t, uint64(1), a.Count)
	assert.Equal(t, uint64(1), b.Count)
	c := combineSampleBucket(sampleBucket{Count: 1, Sum: 1, Min: 1, Max: 1}, sampleBucket{Count: 1, Sum: 2, Min: 2, Max: 3})
	assert.Equal(t, 3.0, c.Max)

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
	assert.Len(t, samples.Time, 1)
	assert.Equal(t, 1.0, samples.Buckets[0].Min)
	assert.Equal(t, 7.0, samples.Buckets[0].Max)
	assert.Len(t, mergeSampleBuckets(nil, []sampleBucket{{Time: 1, Count: 1}}), 1)
}

func TestSampleErrors(t *testing.T) {
	store := newMemStore()
	store.loadErr = errTest
	db, _ := New(store)
	now := time.Now()
	_, err := db.Samples("x").Range(context.Background(), now, now, time.Second, Sum)
	assert.ErrorIs(t, err, errTest)
}
