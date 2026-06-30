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

func TestSeries(t *testing.T) {
	t.Run("encoded samples", func(t *testing.T) {
		var p pending
		p.Samples.Add(10, 1, 1, 1)
		p.Samples.Add(20, 2, 2, 1)
		p.Samples.Buckets = []sampleBucket{{Time: 100, Count: 1, Sum: 9, Min: 9, Max: 9, First: 9, Last: 9}}
		s := marshaled(t, &p)

		got := collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.sampleValues(10, 100, y))
		})
		assert.Equal(t, []float64{9, 1, 2}, got)

		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.sampleRange(10, 100, 10, Sum, y))
		})
		assert.NotEmpty(t, got)

		calls := 0
		require.NoError(t, s.sampleValues(10, 20, func(time.Time, float64) bool {
			calls++
			return false
		}))
		assert.Equal(t, 1, calls)
	})

	t.Run("encoded counters", func(t *testing.T) {
		var p pending
		p.Counters.Add(10, 1, 1, 2)
		p.Counters.Add(20, 1, 2, 3)
		p.Counters.Add(20, 1, 3, 4)
		p.Counters.Buckets = []counterBucket{{Time: 100, Sum: 7}}
		s := marshaled(t, &p)

		got := collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.counterValues(10, 100, y))
		})
		assert.Equal(t, []float64{7, 2, 7}, got)

		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.counterRange(10, 100, 10, Sum, y))
		})
		assert.NotEmpty(t, got)

		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.counterRange(10, 100, 0, Sum, y))
		})
		assert.Equal(t, []float64{7, 2, 7}, got)

		calls := 0
		require.NoError(t, s.counterValues(10, 20, func(time.Time, float64) bool {
			calls++
			return false
		}))
		assert.Equal(t, 1, calls)
	})

	t.Run("compact buckets only", func(t *testing.T) {
		var p pending
		p.Samples.Add(1, 1, 1, 1)
		p.Samples.Compact(2, 10)
		p.Counters.Add(1, 1, 1, 5)
		p.Counters.Compact(2, 10)
		s := marshaled(t, &p)

		got := collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.sampleValues(0, 100, y))
		})
		assert.NotEmpty(t, got)
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.sampleRange(0, 100, 10, Sum, y))
		})
		assert.NotEmpty(t, got)
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.counterValues(0, 100, y))
		})
		assert.NotEmpty(t, got)
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.counterRange(0, 100, 10, Sum, y))
		})
		assert.NotEmpty(t, got)
	})

	t.Run("pending state", func(t *testing.T) {
		var nilPending *pending
		assert.Equal(t, 0, nilPending.Count())
		assert.True(t, nilPending.appendable())

		tm, ok := (sampleData{Buckets: []sampleBucket{{Time: 5, Count: 1, Sum: 1, Min: 1, Max: 1, First: 1, Last: 1}}}).minTime()
		assert.True(t, ok)
		assert.Equal(t, uint64(5), tm)

		tm, ok = (counterData{Buckets: []counterBucket{{Time: 7, Sum: 1}}}).minTime()
		assert.True(t, ok)
		assert.Equal(t, uint64(7), tm)

		assert.False(t, sampleBucketsIncreasing([]sampleBucket{{Time: 2}, {Time: 1}}))
		assert.False(t, counterBucketsIncreasing([]counterBucket{{Time: 2}, {Time: 1}}))
		assert.False(t, nondecreasing([]uint64{2, 1}))

		var p pending
		p.Samples.Buckets = []sampleBucket{{Time: 5, Count: 1, Sum: 1, Min: 1, Max: 1, First: 1, Last: 1}}
		tm, ok = p.minTime()
		assert.True(t, ok)
		assert.Equal(t, uint64(5), tm)

		assert.Nil(t, clonePending(nil))
		cloned := clonePending(&p)
		require.NotNil(t, cloned)
		assert.Equal(t, p.Samples.Buckets, cloned.Samples.Buckets)
	})

	t.Run("skip segment window", func(t *testing.T) {
		var p pending
		for i := range uint64(segmentSize) {
			p.Samples.Add(i+1, float64(i), i+1, 1)
		}
		for i := range uint64(segmentSize) {
			p.Samples.Add(segmentSize+i+1, float64(i), segmentSize+i+1, 1)
		}
		s := marshaled(t, &p)
		got := collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.sampleValues(segmentSize+1, segmentSize+5, y))
		})
		assert.NotEmpty(t, got)
	})

	t.Run("read errors", func(t *testing.T) {
		s := sampleSeriesSegment(t, 1, 1, 1, []byte{1})
		assert.Error(t, s.sampleValues(1, 1, func(time.Time, float64) bool { return true }))
		assert.Error(t, s.sampleRange(1, 1, 10, Sum, func(time.Time, float64) bool { return true }))

		s = counterSeriesSegment(t, 1, 1, 1, []byte{1})
		assert.Error(t, s.counterValues(1, 1, func(time.Time, float64) bool { return true }))
		assert.Error(t, s.counterRange(1, 1, 10, Sum, func(time.Time, float64) bool { return true }))
	})

	t.Run("range stop", func(t *testing.T) {
		var p pending
		p.Samples.Add(10, 1, 1, 1)
		s := marshaled(t, &p)
		calls := 0
		require.NoError(t, s.sampleRange(10, 10, 10, Sum, func(time.Time, float64) bool {
			calls++
			return false
		}))
		assert.Equal(t, 1, calls)

		p.Counters.Add(10, 1, 1, 2)
		s = marshaled(t, &p)
		calls = 0
		require.NoError(t, s.counterRange(10, 10, 10, Sum, func(time.Time, float64) bool {
			calls++
			return false
		}))
		assert.Equal(t, 1, calls)
	})

	t.Run("min time merge", func(t *testing.T) {
		var p pending
		p.Samples.Buckets = []sampleBucket{{Time: 20, Count: 1, Sum: 1, Min: 1, Max: 1, First: 1, Last: 1}}
		p.Counters.Time = []uint64{5}
		tm, ok := p.minTime()
		assert.True(t, ok)
		assert.Equal(t, uint64(5), tm)

		tm, ok = (counterData{Time: []uint64{3}, Buckets: []counterBucket{{Time: 10, Sum: 1}}}).minTime()
		assert.True(t, ok)
		assert.Equal(t, uint64(3), tm)
	})

	t.Run("encoded edge cases", func(t *testing.T) {
		var early pending
		early.Samples.Add(1, 1, 1, 1)
		early.Counters.Add(1, 1, 1, 1)
		sEarly, err := early.marshal()
		require.NoError(t, err)

		var late pending
		late.Samples.Add(100, 2, 2, 1)
		late.Counters.Add(100, 1, 2, 2)
		s, err := series(sEarly).append(&late)
		require.NoError(t, err)
		ser := series(s)

		got := collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, ser.sampleValues(50, 99, y))
		})
		assert.Empty(t, got)
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, ser.counterValues(50, 99, y))
		})
		assert.Empty(t, got)

		require.NoError(t, ser.sampleRange(1, 1, 0, Sum, func(time.Time, float64) bool { return true }))

		var buckets pending
		buckets.Samples.Buckets = []sampleBucket{{Time: 1, Count: 1, Sum: 1, Min: 1, Max: 1, First: 1, Last: 1}}
		buckets.Counters.Buckets = []counterBucket{{Time: 1, Sum: 1}}
		sBuckets := marshaled(t, &buckets)
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, sBuckets.sampleValues(50, 99, y))
		})
		assert.Empty(t, got)
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, sBuckets.counterValues(50, 99, y))
		})
		assert.Empty(t, got)
	})

	t.Run("encoded decode errors", func(t *testing.T) {
		assert.Error(t, sampleBucketSeriesSegment(t, 1, 1, 1, []byte{1}).sampleValues(1, 1, func(time.Time, float64) bool { return true }))
		assert.Error(t, sampleBucketSeriesSegment(t, 1, 1, 1, []byte{1}).sampleRange(1, 1, 10, Sum, func(time.Time, float64) bool { return true }))
		assert.Error(t, counterBucketSeriesSegment(t, 1, 1, 1, []byte{1}).counterValues(1, 1, func(time.Time, float64) bool { return true }))
		assert.Error(t, counterBucketSeriesSegment(t, 1, 1, 1, []byte{1}).counterRange(1, 1, 10, Sum, func(time.Time, float64) bool { return true }))

		payload := appendSampleRaw(nil, []uint64{1}, []float64{1}, []uint64{1}, []uint64{1})
		s := invalidZipSeriesSegment(t, segmentSamples, 1, 1, 1, payload)
		assert.Error(t, s.sampleValues(1, 1, func(time.Time, float64) bool { return true }))
		assert.Error(t, s.sampleRange(1, 1, 10, Sum, func(time.Time, float64) bool { return true }))

		samplePayload := appendSampleRaw(nil, []uint64{20}, []float64{1}, []uint64{1}, []uint64{1})
		counterPayload := appendCounterRaw(nil, []uint64{20}, []uint64{1}, []uint64{1}, []uint64{1})
		assert.Error(t, garbageZipSeriesSegment(t, segmentCounters, 20, 20, 1, len(counterPayload), 4).counterValues(20, 20, func(time.Time, float64) bool { return true }))
		assert.Error(t, garbageZipSeriesSegment(t, segmentCounters, 20, 20, 1, len(counterPayload), 4).counterRange(20, 20, 10, Sum, func(time.Time, float64) bool { return true }))
		assert.Error(t, garbageZipSeriesSegment(t, segmentSamples, 20, 20, 1, len(samplePayload), 4).sampleValues(20, 20, func(time.Time, float64) bool { return true }))
		assert.Error(t, garbageZipSeriesSegment(t, segmentSampleBuckets, 20, 20, 1, 1, 4).sampleValues(20, 20, func(time.Time, float64) bool { return true }))
		assert.Error(t, garbageZipSeriesSegment(t, segmentSampleBuckets, 20, 20, 1, 1, 4).sampleRange(20, 20, 10, Sum, func(time.Time, float64) bool { return true }))
		assert.Error(t, garbageZipSeriesSegment(t, segmentSamples, 20, 20, 1, len(samplePayload), 4).sampleRange(20, 20, 10, Sum, func(time.Time, float64) bool { return true }))

		bucketPayload := appendCounterBuckets(nil, []counterBucket{{Time: 20, Sum: 1}})
		assert.Error(t, garbageZipSeriesSegment(t, segmentCounterBuckets, 20, 20, 1, len(bucketPayload), 4).counterValues(20, 20, func(time.Time, float64) bool { return true }))
		assert.Error(t, garbageZipSeriesSegment(t, segmentCounterBuckets, 20, 20, 1, len(bucketPayload), 4).counterRange(20, 20, 10, Sum, func(time.Time, float64) bool { return true }))
	})

	t.Run("encoded yield stop", func(t *testing.T) {
		var p pending
		p.Samples.Add(10, 1, 1, 1)
		p.Samples.Buckets = []sampleBucket{{Time: 20, Count: 1, Sum: 2, Min: 2, Max: 2, First: 2, Last: 2}}
		s := marshaled(t, &p)
		calls := 0
		require.NoError(t, s.sampleValues(10, 20, func(time.Time, float64) bool {
			calls++
			return false
		}))
		assert.Equal(t, 1, calls)

		p = pending{}
		p.Samples.Add(10, 1, 1, 1)
		p.Samples.Add(70, 2, 2, 1)
		s = marshaled(t, &p)
		calls = 0
		require.NoError(t, s.sampleRange(10, 70, 60, Sum, func(time.Time, float64) bool {
			calls++
			return false
		}))
		assert.Equal(t, 2, calls)

		p = pending{}
		p.Samples.Buckets = []sampleBucket{
			{Time: 10, Count: 1, Sum: 1, Min: 1, Max: 1, First: 1, Last: 1},
			{Time: 70, Count: 1, Sum: 2, Min: 2, Max: 2, First: 2, Last: 2},
		}
		s = marshaled(t, &p)
		calls = 0
		require.NoError(t, s.sampleRange(10, 70, 60, Sum, func(time.Time, float64) bool {
			calls++
			return false
		}))
		assert.Equal(t, 2, calls)

		p = pending{}
		p.Counters.Add(10, 1, 1, 2)
		p.Counters.Add(10, 1, 2, 3)
		p.Counters.Add(20, 1, 3, 4)
		s = marshaled(t, &p)
		calls = 0
		require.NoError(t, s.counterValues(10, 20, func(time.Time, float64) bool {
			calls++
			return calls < 2
		}))
		assert.Equal(t, 2, calls)

		p = pending{}
		p.Counters.Buckets = []counterBucket{{Time: 20, Sum: 1}, {Time: 25, Sum: 2}}
		s = marshaled(t, &p)
		calls = 0
		require.NoError(t, s.counterValues(20, 25, func(time.Time, float64) bool {
			calls++
			return false
		}))
		assert.Equal(t, 1, calls)

		p = pending{}
		p.Counters.Add(450, 1, 1, 1)
		p.Counters.Add(510, 1, 2, 2)
		s = marshaled(t, &p)
		calls = 0
		require.NoError(t, s.counterRange(450, 510, 60, Sum, func(time.Time, float64) bool {
			calls++
			return false
		}))
		assert.Equal(t, 2, calls)

		p = pending{}
		p.Counters.Buckets = []counterBucket{{Time: 10, Sum: 1}, {Time: 25, Sum: 2}}
		s = marshaled(t, &p)
		got := collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.counterValues(20, 30, y))
		})
		assert.Equal(t, []float64{2}, got)

		p = pending{}
		p.Counters.Buckets = []counterBucket{{Time: 10, Sum: 2}, {Time: 70, Sum: 3}}
		s = marshaled(t, &p)
		calls = 0
		require.NoError(t, s.counterRange(10, 70, 60, Sum, func(time.Time, float64) bool {
			calls++
			return calls < 2
		}))
		assert.Equal(t, 2, calls)

		p = pending{}
		p.Samples.Buckets = []sampleBucket{
			{Time: 10, Count: 1, Sum: 1, Min: 1, Max: 1, First: 1, Last: 1},
			{Time: 70, Count: 1, Sum: 2, Min: 2, Max: 2, First: 2, Last: 2},
		}
		s = marshaled(t, &p)
		calls = 0
		require.NoError(t, s.sampleRange(10, 70, 60, Sum, func(time.Time, float64) bool {
			calls++
			return calls < 2
		}))
		assert.Equal(t, 2, calls)

		p = pending{}
		p.Counters.Buckets = []counterBucket{{Time: 10, Sum: 2}, {Time: 70, Sum: 3}}
		s = marshaled(t, &p)
		calls = 0
		require.NoError(t, s.counterRange(10, 70, 60, Sum, func(time.Time, float64) bool {
			calls++
			return false
		}))
		assert.Equal(t, 2, calls)
	})

	t.Run("multi segment paths", func(t *testing.T) {
		var base pending
		for i := range uint64(segmentSize) {
			base.Counters.Add(i, 1, i+1, 1)
			base.Samples.Add(i, float64(i), i+1, 1)
		}
		baseRaw, err := base.marshal()
		require.NoError(t, err)

		var tail pending
		tail.Counters.Add(500, 1, 1, 9)
		tail.Samples.Add(500, 9, 1, 1)
		tail.Counters.Buckets = []counterBucket{{Time: 600, Sum: 3}}
		tail.Samples.Buckets = []sampleBucket{{Time: 600, Count: 1, Sum: 4, Min: 4, Max: 4, First: 4, Last: 4}}
		ser, err := series(baseRaw).append(&tail)
		require.NoError(t, err)
		s := series(ser)

		got := collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.counterValues(1100, 1200, y))
		})
		assert.Empty(t, got)
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.sampleValues(1100, 1200, y))
		})
		assert.Empty(t, got)

		trailing := append(series(marshaled(t, codecSeries(1))), 0xff)
		assert.Error(t, series(trailing).sampleValues(1, 1, func(time.Time, float64) bool { return true }))
		assert.Error(t, series(trailing).counterValues(1, 1, func(time.Time, float64) bool { return true }))
		assert.Error(t, series(trailing).sampleRange(1, 1, 10, Sum, func(time.Time, float64) bool { return true }))
		assert.Error(t, series(trailing).counterRange(1, 1, 10, Sum, func(time.Time, float64) bool { return true }))

		payload := appendCounterRaw(nil, []uint64{500}, []uint64{9}, []uint64{1}, []uint64{1})
		assert.Error(t, invalidZipSeriesSegment(t, segmentCounters, 500, 500, 1, payload).counterValues(500, 500, func(time.Time, float64) bool { return true }))
		assert.Error(t, invalidZipSeriesSegment(t, segmentCounters, 500, 500, 1, payload).counterRange(500, 500, 10, Sum, func(time.Time, float64) bool { return true }))

		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.counterRange(450, 520, 60, Sum, y))
		})
		assert.NotEmpty(t, got)
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.sampleRange(450, 520, 60, Sum, y))
		})
		assert.NotEmpty(t, got)

		assert.Error(t, sampleSeriesSegment(t, 500, 500, 1, []byte{1}).sampleValues(500, 500, func(time.Time, float64) bool { return true }))
		assert.Error(t, sampleSeriesSegment(t, 500, 500, 1, []byte{1}).sampleRange(500, 500, 10, Sum, func(time.Time, float64) bool { return true }))

		p := pending{}
		p.Counters.Buckets = []counterBucket{{Time: 10, Sum: 1}, {Time: 25, Sum: 2}, {Time: 40, Sum: 3}}
		s = marshaled(t, &p)
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.counterRange(20, 30, 10, Sum, y))
		})
		assert.Equal(t, []float64{2}, got)

		p = pending{}
		p.Samples.Buckets = []sampleBucket{
			{Time: 10, Count: 1, Sum: 1, Min: 1, Max: 1, First: 1, Last: 1},
			{Time: 25, Count: 1, Sum: 2, Min: 2, Max: 2, First: 2, Last: 2},
			{Time: 40, Count: 1, Sum: 3, Min: 3, Max: 3, First: 3, Last: 3},
		}
		s = marshaled(t, &p)
		got = collectCall(t, func(y func(time.Time, float64) bool) {
			require.NoError(t, s.sampleRange(20, 30, 10, Sum, y))
		})
		assert.Equal(t, []float64{2}, got)

		baseRaw, err = base.marshal()
		require.NoError(t, err)
		tail = pending{}
		tail.Samples.Add(500, 9, 1, 1)
		ser, err = series(baseRaw).append(&tail)
		require.NoError(t, err)
		require.NoError(t, series(ser).sampleRange(1100, 1200, 10, Sum, func(time.Time, float64) bool { return true }))
	})
}
