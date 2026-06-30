// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMerge(t *testing.T) {
	var a, b pending
	a.Samples.Add(1, 1, 1, 1)
	b.Samples.Add(1, 2, 2, 1)
	b.Samples.Buckets = []sampleBucket{{Time: 10, Count: 1, Sum: 2, Min: 2, Max: 2, First: 2, Last: 2}}
	a.Samples.Buckets = []sampleBucket{{Time: 10, Count: 1, Sum: 4, Min: 4, Max: 4, First: 4, Last: 4}}
	a.Merge(&b)
	assert.Equal(t, 2.0, a.Samples.Data[0])
	got := a.Samples.Buckets[0]
	assert.Equal(t, sampleBucket{Time: 10, Count: 2, Sum: 6, Min: 2, Max: 4, First: 4, Last: 2}, got)

	a, b = pending{}, pending{}
	a.Counters.Add(1, 1, 1, 2)
	b.Counters.Add(1, 1, 1, 2)
	a.Merge(&b)
	values := collectCall(t, func(y func(time.Time, float64) bool) {
		a.Counters.values(1, 1, y)
	})
	assert.Equal(t, []float64{2}, values)
}

func TestAppend(t *testing.T) {
	t.Run("if after", func(t *testing.T) {
		var current pending
		current.Samples.Add(1, 1, 1, 1)
		raw, err := current.marshal()
		require.NoError(t, err)

		var next pending
		for i := range uint64(segmentSize) {
			next.Samples.Add(2+i, float64(2+i), 2+i, 1)
		}
		raw, err = series(raw).append(&next)
		require.NoError(t, err)
		decoded, err := decode(raw)
		require.NoError(t, err)
		got := collectCall(t, func(y func(time.Time, float64) bool) {
			decoded.Samples.values(1, 2, y)
		})
		assert.Equal(t, []float64{1, 2}, got)

		var overlap pending
		overlap.Samples.Add(2, 9, 9, 1)
		raw, err = series(raw).append(&overlap)
		require.NoError(t, err)
		decoded, err = decode(raw)
		require.NoError(t, err)
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			decoded.Samples.values(2, 2, y)
		})
		assert.Equal(t, []float64{9}, got)
	})

	t.Run("merges unordered large delta", func(t *testing.T) {
		var current pending
		current.Samples.Add(0, 0, 1, 1)
		raw, err := current.marshal()
		require.NoError(t, err)

		var next pending
		for i := range uint64(segmentSize) {
			t := uint64(segmentSize) - i
			next.Samples.Add(t, float64(t), i+2, 1)
			next.Counters.Add(t, 1, i+2, 1)
		}
		next.Samples.Add(10, 99, segmentSize+3, 1)

		raw, err = series(raw).append(&next)
		require.NoError(t, err)
		decoded, err := decode(raw)
		require.NoError(t, err)

		require.Len(t, decoded.Samples.Time, segmentSize+1)
		for i := 1; i < len(decoded.Samples.Time); i++ {
			assert.Greater(t, decoded.Samples.Time[i], decoded.Samples.Time[i-1])
		}
		got := collectCall(t, func(y func(time.Time, float64) bool) {
			decoded.Samples.values(10, 10, y)
		})
		assert.Equal(t, []float64{99}, got)

		require.Len(t, decoded.Counters.Time, segmentSize)
		for i := 1; i < len(decoded.Counters.Time); i++ {
			assert.GreaterOrEqual(t, decoded.Counters.Time[i], decoded.Counters.Time[i-1])
		}
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			decoded.Counters.values(10, 10, y)
		})
		assert.Equal(t, []float64{1}, got)
	})
}

func FuzzSampleMerge(f *testing.F) {
	f.Add(uint64(1), float64(1), uint64(1), uint64(2), float64(2), uint64(2))
	f.Fuzz(func(t *testing.T, ts uint64, v1 float64, c1 uint64, r uint64, v2 float64, c2 uint64) {
		var a, b, ab, ba pending
		a.Samples.Add(ts, v1, c1, r)
		b.Samples.Add(ts, v2, c2, r+1)
		ab.Merge(&a)
		ab.Merge(&b)
		ba.Merge(&b)
		ba.Merge(&a)
		require.Len(t, ab.Samples.Data, 1)
		require.Len(t, ba.Samples.Data, 1)
		assert.Equal(t, ab.Samples.Data[0], ba.Samples.Data[0])
		ab.Merge(&a)
		require.Len(t, ab.Samples.Data, 1)
	})
}

func FuzzCounterMerge(f *testing.F) {
	f.Add(uint64(1), uint64(1), uint64(2), uint64(3))
	f.Fuzz(func(t *testing.T, ts, replica, clock, value uint64) {
		var a, b, c, left, right pending
		a.Counters.Add(ts, replica, clock, value)
		b.Counters.Add(ts, replica+1, clock+1, value+1)
		c.Counters.Add(ts+1, replica, clock+2, value+2)
		left.Merge(&a)
		left.Merge(&b)
		left.Merge(&c)
		right.Merge(&b)
		right.Merge(&c)
		right.Merge(&a)
		leftValues := collectCall(t, func(y func(time.Time, float64) bool) {
			left.Counters.values(0, ^uint64(0), y)
		})
		rightValues := collectCall(t, func(y func(time.Time, float64) bool) {
			right.Counters.values(0, ^uint64(0), y)
		})
		assert.Len(t, leftValues, len(rightValues))
		left.Merge(&a)
		got := collectCall(t, func(y func(time.Time, float64) bool) {
			left.Counters.values(ts, ts, y)
		})
		require.NotEmpty(t, got)
	})
}
