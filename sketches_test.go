// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSketches(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithReplica("sketches"))
	require.NoError(t, err)
	sketches := db.Sketches("latency")
	at := time.Unix(60, 0)
	for _, value := range []float64{10, -10, 5, 0} {
		require.NoError(t, sketches.Add(ctx, at, value))
	}
	require.NoError(t, sketches.Add(ctx, at.Add(time.Second), 20))

	values, err := sketches.Values(ctx, at, at.Add(time.Second))
	require.NoError(t, err)
	got := collectSketches(values)
	require.Len(t, got, 2)
	assert.Equal(t, uint64(4), got[0].Count())
	assert.Equal(t, 5.0, got[0].Sum())
	assert.Equal(t, -10.0, got[0].Min())
	assert.Equal(t, 10.0, got[0].Max())
	assert.Equal(t, 1.25, got[0].Mean())
	assert.Equal(t, -10.0, got[0].Quantile(0))
	assert.Equal(t, 0.0, got[0].Quantile(0.5))
	assert.Equal(t, 5.0, got[0].Quantile(0.75))
	assert.Equal(t, 10.0, got[0].Quantile(1))

	ranged, err := sketches.Range(ctx, at, at.Add(time.Second), time.Minute)
	require.NoError(t, err)
	merged := collectSketches(ranged)
	require.Len(t, merged, 1)
	assert.Equal(t, uint64(5), merged[0].Count())
	assert.Equal(t, 25.0, merged[0].Sum())
	assert.Equal(t, 20.0, merged[0].Max())

	t.Run("invalid and empty values", func(t *testing.T) {
		assert.Error(t, sketches.Add(ctx, at, math.NaN()))
		assert.Error(t, sketches.Add(ctx, at, math.Inf(1)))
		assert.Equal(t, uint64(0), (Sketch{}).Count())
		assert.Zero(t, (Sketch{}).Sum())
		assert.True(t, math.IsNaN((Sketch{}).Min()))
		assert.True(t, math.IsNaN((Sketch{}).Max()))
		assert.True(t, math.IsNaN((Sketch{}).Mean()))
		assert.True(t, math.IsNaN((Sketch{}).Quantile(0.5)))
		assert.True(t, math.IsNaN(got[0].Quantile(-0.1)))
		assert.True(t, math.IsNaN(got[0].Quantile(1.1)))
		assert.True(t, math.IsNaN(got[0].Quantile(math.NaN())))
	})
}

func TestSketchCompaction(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithCompaction(time.Hour, time.Minute))
	require.NoError(t, err)
	sketches := db.Sketches("latency")
	at := time.Now().Add(-2 * time.Hour).Truncate(time.Minute)
	for _, value := range []float64{-100, -1, 0, 1, 100} {
		require.NoError(t, sketches.Add(ctx, at, value))
	}
	require.NoError(t, sketches.Compact(ctx))

	values, err := sketches.Values(ctx, at, at.Add(time.Minute))
	require.NoError(t, err)
	got := collectSketches(values)
	require.Len(t, got, 1)
	assert.Equal(t, uint64(5), got[0].Count())
	assert.Equal(t, 0.0, got[0].Sum())
	assert.Equal(t, -100.0, got[0].Min())
	assert.Equal(t, 100.0, got[0].Max())
	assert.InDelta(t, -1, got[0].Quantile(0.25), 0.01)
	assert.Equal(t, 0.0, got[0].Quantile(0.5))
	assert.InDelta(t, 1, got[0].Quantile(0.75), 0.01)

	// A late raw observation merges with the compacted bucket on reads.
	require.NoError(t, sketches.Add(ctx, at, 200))
	values, err = sketches.Values(ctx, at, at)
	require.NoError(t, err)
	got = collectSketches(values)
	require.Len(t, got, 1)
	assert.Equal(t, uint64(6), got[0].Count())
	assert.Equal(t, 200.0, got[0].Max())
}

func TestSketchBuffer(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithFlushEvery(time.Hour))
	require.NoError(t, err)
	sketches := db.Sketches("x")
	require.NoError(t, sketches.Add(ctx, time.Unix(1, 0), 1))
	require.NoError(t, sketches.Add(ctx, time.Unix(2, 0), 2))
	values, err := sketches.Values(ctx, time.Unix(1, 0), time.Unix(2, 0))
	require.NoError(t, err)
	got := collectSketches(values)
	require.Len(t, got, 2)
	first := got[0]
	assert.Equal(t, 1.0, first.Quantile(0.5))
	require.NoError(t, db.Flush(ctx))
	assert.Equal(t, 1.0, first.Quantile(0.5))
}

func TestSketchCodec(t *testing.T) {
	var a, b sketchData
	a.Add(1, 10, 1, 1)
	b.Add(1, 20, 2, 1)
	a.Merge(b)
	a.Merge(b)
	assert.Len(t, a.Time, 2)

	var left, right sketchData
	left.Add(2, 1, 7, 3)
	right.Add(2, 2, 7, 3)
	var leftRight, rightLeft sketchData
	leftRight.Merge(left)
	leftRight.Merge(right)
	rightLeft.Merge(right)
	rightLeft.Merge(left)
	assert.Equal(t, leftRight.Data, rightLeft.Data)
	assert.Equal(t, []float64{2}, leftRight.Data)

	a.Compact(2, 10)
	require.Len(t, a.Buckets, 1)
	decoded, err := decodeSketch(a.Buckets[0].Data)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), decoded.count)

	var state pending
	state.Sketches.Add(1, -1, 1, 1)
	state.Sketches.Add(1, 1, 2, 1)
	state.Sketches.Buckets = a.Buckets
	raw, err := state.marshal()
	require.NoError(t, err)
	roundTrip, err := decode(raw)
	require.NoError(t, err)
	assert.Equal(t, state.Sketches, roundTrip.Sketches)

	var only pending
	only.Sketches.Add(1, 2, 3, 4)
	onlyRaw, err := only.marshal()
	require.NoError(t, err)
	var sketchSegment segment
	require.NoError(t, series(onlyRaw).scan(func(current segment) bool {
		if current.kind == segmentSketches {
			sketchSegment = current
		}
		return true
	}))
	sketchRaw, err := sketchSegment.decodeInto(nil)
	require.NoError(t, err)
	sampleRaw := appendSampleRaw(nil, []uint64{1}, []float64{2}, []uint64{3}, []uint64{4})
	assert.Equal(t, sampleRaw, sketchRaw)
}

func TestSketchBinLimit(t *testing.T) {
	var value sketchValue
	for i := -550; i < 550; i++ {
		value.addApprox(math.Pow(sketchGamma, float64(i)))
	}
	assert.LessOrEqual(t, len(value.negative)+len(value.positive), sketchMaxBins)
	encoded := value.encodeApprox()
	decoded, err := decodeSketch(encoded)
	require.NoError(t, err)
	assert.Equal(t, uint64(1100), decoded.count)
	assert.Equal(t, value.min, decoded.min)
	assert.Equal(t, value.max, decoded.max)

	var reverse sketchValue
	for i := 549; i >= -550; i-- {
		reverse.addApprox(math.Pow(sketchGamma, float64(i)))
	}
	reverseDecoded, err := decodeSketch(reverse.encodeApprox())
	require.NoError(t, err)
	assert.Equal(t, decoded.zero, reverseDecoded.zero)
	assert.Equal(t, decoded.positive, reverseDecoded.positive)
}

func TestSketchErrors(t *testing.T) {
	value := sketchValue{count: 1, min: 1, max: 1, sum: 1}
	_, err := decodeSketch(appendSketchHeader(nil, 99, &value))
	assert.ErrorIs(t, err, errVarintCodec)
	_, err = decodeSketch(appendSketchExact(nil, &value))
	assert.Error(t, err)
	_, err = decodeSketch(appendSketchApprox(nil, &value))
	assert.ErrorIs(t, err, errShapeCodec)

	tooMany := sketchValue{count: sketchMaxBins + 1, min: 1, max: 1, positive: make(map[int]uint64)}
	for i := range sketchMaxBins + 1 {
		tooMany.positive[i] = 1
	}
	_, err = decodeSketch(appendSketchApprox(nil, &tooMany))
	assert.ErrorIs(t, err, errLargeCodec)

	buf := encodeBuffers.Get().(*codecBuffer)
	bad := appendSegment(nil, segmentSketches, 1, 1, 1, []byte{1}, buf)
	putEncodeBuffer(buf)
	var good pending
	good.Sketches.Add(10, 10, 1, 1)
	goodRaw, err := good.marshal()
	require.NoError(t, err)
	mixed := series(append(append([]byte{version}, bad...), goodRaw[1:]...))
	var got []Sketch
	require.NoError(t, mixed.sketchRange(10, 10, 0, func(_ time.Time, value Sketch) bool {
		got = append(got, value)
		return true
	}))
	require.Len(t, got, 1)
	assert.Equal(t, 10.0, got[0].Min())
	assert.Error(t, mixed.sketchRange(1, 1, 0, func(time.Time, Sketch) bool { return true }))
}

func TestSketchState(t *testing.T) {
	var sketch sketchValue
	sketch.addApprox(-5)
	sketch.addApprox(-3)
	sketch.addApprox(2)
	sketch.addApprox(7)
	for range sketchMaxBins {
		sketch.addApprox(-math.Pow(sketchGamma, float64(600)))
	}
	assert.LessOrEqual(t, len(sketch.negative)+len(sketch.positive), sketchMaxBins)

	var exact sketchValue
	exact.addExact(1)
	exact.addExact(2)
	exact.approximate()
	assert.Nil(t, exact.exact)

	var left, right sketchValue
	left.addExact(1)
	right.addApprox(3)
	require.NoError(t, left.mergeBytes(right.encodeApprox()))
	assert.Nil(t, left.exact)

	var both sketchValue
	both.addExact(1)
	other := sketchValue{}
	other.addExact(2)
	require.NoError(t, both.mergeBytes(appendSketchExact(nil, &other)))
	assert.Equal(t, []float64{1, 2}, both.exact)

	var empty sketchValue
	empty.addApprox(1)
	require.NoError(t, empty.mergeBytes(nil))

	var data sketchData
	data.Buckets = []sketchBucket{
		{Time: 5, Data: sketch.encodeApprox()},
		{Time: 10, Data: sketch.encodeApprox()},
	}
	min, ok := data.minTime()
	require.True(t, ok)
	assert.Equal(t, uint64(5), min)
	assert.False(t, sketchBucketsIncreasing([]sketchBucket{{Time: 2}, {Time: 2}}))

	data.Add(100, 1, 1, 1)
	data.Add(1, 2, 1, 1)
	data.Compact(50, 10)
	require.Len(t, data.Time, 1)
	assert.Equal(t, uint64(100), data.Time[0])
	require.NotEmpty(t, data.Buckets)

	var buffered pending
	buffered.Sketches.Buckets = []sketchBucket{
		{Time: 5, Data: sketch.encodeApprox()},
		{Time: 10, Data: sketch.encodeApprox()},
	}
	min, ok = buffered.minTime()
	require.True(t, ok)
	assert.Equal(t, uint64(5), min)

	cloned := clonePending(&buffered)
	require.Len(t, cloned.Sketches.Buckets, len(buffered.Sketches.Buckets))
	assert.Equal(t, buffered.Sketches.Buckets[0].Data, cloned.Sketches.Buckets[0].Data)

	var timed sketchData
	timed.Add(3, 1, 1, 1)
	timed.Buckets = buffered.Sketches.Buckets
	min, ok = timed.minTime()
	require.True(t, ok)
	assert.Equal(t, uint64(3), min)

	var negOnly sketchValue
	for i := range sketchMaxBins + 5 {
		negOnly.addApprox(-math.Pow(sketchGamma, float64(i+1)))
	}
	assert.LessOrEqual(t, len(negOnly.negative), sketchMaxBins)
	assert.Zero(t, len(negOnly.positive))

	var fresh sketchValue
	var otherExact sketchValue
	otherExact.addExact(3)
	require.NoError(t, fresh.mergeBytes(appendSketchExact(nil, &otherExact)))
	assert.Equal(t, []float64{3}, fresh.exact)

	var badMerge sketchValue
	assert.Error(t, badMerge.mergeBytes([]byte{sketchApprox, 0xff}))

	var mixedBins sketchValue
	for i := range sketchMaxBins/2 + 5 {
		mixedBins.addApprox(-math.Pow(sketchGamma, float64(i+1)))
		mixedBins.addApprox(math.Pow(sketchGamma, float64(i+200)))
	}
	assert.LessOrEqual(t, len(mixedBins.negative)+len(mixedBins.positive), sketchMaxBins)
}

func TestSketchExactXOR(t *testing.T) {
	var small sketchValue
	for _, value := range []float64{1, 2, 3, 4, 5} {
		small.addExact(value)
	}
	stored, err := storedSketch(appendSketchExact(nil, &small))
	require.NoError(t, err)
	assert.Equal(t, byte(sketchExactXOR), stored.kind)
	assert.Equal(t, 3.0, stored.Quantile(0.5))
	assert.Equal(t, 1.0, stored.Quantile(0))
	assert.Equal(t, 5.0, stored.Quantile(1))
	stored.kind = 99
	assert.True(t, math.IsNaN(stored.Quantile(0.5)))
	stored.kind = sketchExactXOR
	assert.True(t, math.IsNaN(exactQuantile(nil, 1)))

	corrupt := stored
	corrupt.data = corrupt.data[:len(corrupt.data)/2]
	assert.True(t, math.IsNaN(corrupt.Quantile(0.5)))

	var large sketchValue
	for i := range segmentSize + 16 {
		large.addExact(float64(i))
	}
	largeStored, err := storedSketch(appendSketchExact(nil, &large))
	require.NoError(t, err)
	assert.Equal(t, uint64(segmentSize+16), largeStored.Count())
	assert.Greater(t, largeStored.Quantile(0.5), 500.0)
	assert.Less(t, largeStored.Quantile(0.5), 600.0)
}

func TestSketchPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("bucket only values", func(t *testing.T) {
		var sketch sketchValue
		sketch.addApprox(-2)
		sketch.addApprox(4)
		var only pending
		only.Sketches.Buckets = []sketchBucket{{Time: 10, Data: sketch.encodeApprox()}}
		raw := marshaled(t, &only)
		got := collectSketches(func(yield func(time.Time, Sketch) bool) {
			_ = raw.sketchRange(10, 10, 0, yield)
		})
		require.Len(t, got, 1)
		assert.Equal(t, uint64(2), got[0].Count())
		assert.InDelta(t, 4, got[0].Quantile(0.75), 0.5)

		var spread sketchValue
		spread.addApprox(-100)
		spread.addApprox(-50)
		spread.addApprox(100)
		spread.addApprox(200)
		assert.Greater(t, spread.encode().Quantile(0.9), 0.0)
	})

	t.Run("raw range with span", func(t *testing.T) {
		var data pending
		for i := range 4 {
			data.Sketches.Add(uint64(i*30), float64(i), 1, 1)
		}
		raw := marshaled(t, &data)
		got := collectSketches(func(yield func(time.Time, Sketch) bool) {
			_ = raw.sketchRange(0, 120, 60, yield)
		})
		require.Len(t, got, 2)
		assert.Equal(t, uint64(2), got[0].Count())
	})

	t.Run("mixed raw and buckets", func(t *testing.T) {
		var bucket sketchValue
		bucket.addApprox(1)
		var mixed pending
		mixed.Sketches.Buckets = []sketchBucket{{Time: 60, Data: bucket.encodeApprox()}}
		mixed.Sketches.Add(120, 9, 1, 1)
		raw := marshaled(t, &mixed)
		got := collectSketches(func(yield func(time.Time, Sketch) bool) {
			_ = raw.sketchRange(0, 200, 60, yield)
		})
		require.Len(t, got, 2)
		assert.Equal(t, 9.0, got[1].Max())
	})

	t.Run("range load error", func(t *testing.T) {
		store := newMemStore()
		store.loadErr = errTest
		db, err := New(store)
		require.NoError(t, err)
		_, err = db.Sketches("x").Range(ctx, time.Unix(1, 0), time.Unix(2, 0), time.Minute)
		assert.ErrorIs(t, err, errTest)
	})

	t.Run("empty series", func(t *testing.T) {
		assert.NoError(t, series(nil).sketchRange(1, 2, 0, func(time.Time, Sketch) bool { return true }))
	})

	t.Run("bucket iterator stop", func(t *testing.T) {
		var sketch sketchValue
		sketch.addApprox(1)
		var only pending
		only.Sketches.Buckets = []sketchBucket{{Time: 10, Data: sketch.encodeApprox()}}
		raw := marshaled(t, &only)
		assert.NoError(t, raw.sketchRange(10, 10, 0, func(time.Time, Sketch) bool { return false }))
	})

	t.Run("raw range decode errors", func(t *testing.T) {
		buf := encodeBuffers.Get().(*codecBuffer)
		bad := appendSegment(nil, segmentSketches, 1, 1, 1, []byte{1}, buf)
		putEncodeBuffer(buf)
		assert.Error(t, series(append([]byte{version}, bad...)).sketchRange(1, 1, 0, func(time.Time, Sketch) bool { return true }))

		var good pending
		good.Sketches.Add(1, math.Inf(1), 1, 1)
		raw := marshaled(t, &good)
		assert.Error(t, raw.sketchRange(1, 1, 0, func(time.Time, Sketch) bool { return true }))
	})

	t.Run("mixed range decode errors", func(t *testing.T) {
		var bucket sketchValue
		bucket.addApprox(1)
		var mixed pending
		mixed.Sketches.Buckets = []sketchBucket{{Time: 1, Data: bucket.encodeApprox()}}
		mixed.Sketches.Add(2, math.Inf(1), 1, 1)
		raw := marshaled(t, &mixed)
		assert.Error(t, raw.sketchRange(1, 2, 0, func(time.Time, Sketch) bool { return true }))
	})

	t.Run("invalid segments", func(t *testing.T) {
		var sketch sketchValue
		sketch.addApprox(1)
		var bucket pending
		bucket.Sketches.Buckets = []sketchBucket{{Time: 10, Data: sketch.encodeApprox()}}
		payload, err := bucket.marshal()
		require.NoError(t, err)
		var seg segment
		require.NoError(t, series(payload).scan(func(current segment) bool {
			if current.kind == segmentSketchBuckets {
				seg = current
			}
			return true
		}))
		decoded, err := seg.decodeInto(nil)
		require.NoError(t, err)

		invalidBucket := invalidZipSeriesSegment(t, segmentSketchBuckets, 10, 10, 1, decoded)
		assert.Error(t, invalidBucket.sketchRange(10, 10, 0, func(time.Time, Sketch) bool { return true }))

		var raw pending
		raw.Sketches.Add(1, 1, 1, 1)
		rawPayload := marshaled(t, &raw)
		var rawSeg segment
		require.NoError(t, rawPayload.scan(func(current segment) bool {
			if current.kind == segmentSketches {
				rawSeg = current
			}
			return true
		}))
		rawDecoded, err := rawSeg.decodeInto(nil)
		require.NoError(t, err)
		invalidRaw := invalidZipSeriesSegment(t, segmentSketches, 1, 1, 1, rawDecoded)
		assert.Error(t, invalidRaw.sketchRange(1, 1, 0, func(time.Time, Sketch) bool { return true }))

		corruptBucket := invalidZipSeriesSegment(t, segmentSketchBuckets, 10, 10, 1, []byte{0xff})
		assert.Error(t, corruptBucket.sketchRange(10, 10, 0, func(time.Time, Sketch) bool { return true }))
	})

	t.Run("out of range and span stop", func(t *testing.T) {
		var data pending
		data.Sketches.Add(1, 1, 1, 1)
		data.Sketches.Add(100, 2, 1, 1)
		raw := marshaled(t, &data)
		got := collectSketches(func(yield func(time.Time, Sketch) bool) {
			_ = raw.sketchRange(1, 1, 60, yield)
		})
		require.Len(t, got, 1)
		calls := 0
		assert.NoError(t, raw.sketchRange(1, 1, 60, func(time.Time, Sketch) bool {
			calls++
			return false
		}))
		assert.Equal(t, 1, calls)

		empty := collectSketches(func(yield func(time.Time, Sketch) bool) {
			_ = raw.sketchRange(50, 60, 0, yield)
		})
		assert.Empty(t, empty)
	})

	t.Run("bucket out of range", func(t *testing.T) {
		var sketch sketchValue
		sketch.addApprox(1)
		var only pending
		only.Sketches.Buckets = []sketchBucket{
			{Time: 5, Data: sketch.encodeApprox()},
			{Time: 10, Data: sketch.encodeApprox()},
			{Time: 15, Data: sketch.encodeApprox()},
		}
		raw := marshaled(t, &only)
		got := collectSketches(func(yield func(time.Time, Sketch) bool) {
			_ = raw.sketchRange(10, 10, 0, yield)
		})
		require.Len(t, got, 1)
	})
}

func TestSketchDecode(t *testing.T) {
	t.Run("header and stored", func(t *testing.T) {
		kind, out, _, err := decodeSketchHeader(nil)
		require.NoError(t, err)
		assert.Zero(t, kind)
		assert.Zero(t, out.count)

		var value sketchValue
		value.addApprox(1)
		value.count = 99
		_, err = storedSketch(appendSketchApprox(nil, &value))
		assert.ErrorIs(t, err, errShapeCodec)

		value = sketchValue{count: 1, min: 1, max: 1, positive: map[int]uint64{1: 1}}
		tooMany := value
		for i := range sketchMaxBins + 1 {
			tooMany.positive[i] = 1
		}
		_, err = storedSketch(appendSketchApprox(nil, &tooMany))
		assert.ErrorIs(t, err, errLargeCodec)
	})

	t.Run("decode sketch", func(t *testing.T) {
		_, err := decodeSketch([]byte{sketchExact, 0xff})
		assert.Error(t, err)

		value := sketchValue{count: 1, min: 1, max: 1}
		_, err = decodeSketch(appendSketchApprox(nil, &value))
		assert.ErrorIs(t, err, errShapeCodec)
	})

	t.Run("bins each", func(t *testing.T) {
		var value sketchValue
		value.positive = map[int]uint64{1: 1}
		payload := appendSketchApprox(nil, &value)
		r := codecReader{data: payload[len(appendSketchHeader(nil, sketchApprox, &value)):]}
		r.uvarint()
		r.sketchBinsEach(func(int, uint64) bool { return true })
		assert.NoError(t, r.err)

		bad := appendSketchApprox(nil, &sketchValue{
			count:    1,
			min:      1,
			max:      1,
			positive: map[int]uint64{1: 0},
		})
		_, err := decodeSketch(bad)
		assert.ErrorIs(t, err, errShapeCodec)
	})

	t.Run("decode sketches rejects nan", func(t *testing.T) {
		var data sketchData
		var p pending
		p.Sketches.Add(1, math.NaN(), 1, 1)
		raw := marshaled(t, &p)
		var seg segment
		require.NoError(t, raw.scan(func(current segment) bool {
			seg = current
			return false
		}))
		decoded, err := seg.decodeInto(nil)
		require.NoError(t, err)
		assert.ErrorIs(t, decodeSketches(decoded, 1, &data), errShapeCodec)

		truncated := decoded[:len(decoded)/2]
		assert.Error(t, decodeSketches(truncated, 1, &data))
	})

	t.Run("scan buckets yield error", func(t *testing.T) {
		var sketch sketchValue
		sketch.addApprox(1)
		var p pending
		p.Sketches.Buckets = []sketchBucket{{Time: 1, Data: sketch.encodeApprox()}}
		raw := marshaled(t, &p)
		var seg segment
		require.NoError(t, raw.scan(func(current segment) bool {
			seg = current
			return current.kind == segmentSketchBuckets
		}))
		decoded, err := seg.decodeInto(nil)
		require.NoError(t, err)
		assert.ErrorIs(t, scanSketchBuckets(decoded, 1, func(uint64, []byte) error {
			return errTest
		}), errTest)
	})

	t.Run("stored and decode edge cases", func(t *testing.T) {
		_, err := storedSketch([]byte{sketchExact, 0xff})
		assert.Error(t, err)

		_, err = storedSketch(appendSketchHeader(nil, 99, &sketchValue{count: 1, min: 1, max: 1}))
		assert.ErrorIs(t, err, errVarintCodec)

		var value sketchValue
		value.addExact(1)
		payload := appendSketchExact(nil, &value)
		payload = append(payload[:len(payload)-1], payload[len(payload)-1]^0xff)
		_, err = decodeSketch(payload)
		assert.Error(t, err)

		value = sketchValue{count: ^uint64(0) >> 1, min: 1, max: 1}
		value.count++
		_, err = decodeSketch(appendSketchExact(nil, &value))
		assert.ErrorIs(t, err, errLargeCodec)

		value = sketchValue{count: 1, min: 1, max: 1, positive: make(map[int]uint64)}
		for i := range sketchMaxBins + 1 {
			value.positive[i] = 1
		}
		value.count = uint64(len(value.positive))
		_, err = decodeSketch(appendSketchApprox(nil, &value))
		assert.ErrorIs(t, err, errLargeCodec)

		value = sketchValue{count: 99, min: 1, max: 1, zero: 1}
		_, err = decodeSketch(appendSketchApprox(nil, &value))
		assert.ErrorIs(t, err, errShapeCodec)

		var storedValue sketchValue
		storedValue.addApprox(1)
		storedValue.count = 99
		_, err = storedSketch(storedValue.encodeApprox())
		assert.ErrorIs(t, err, errShapeCodec)
	})
}

func TestSketchIterator(t *testing.T) {
	var data pending
	data.Sketches.Add(1, 1, 1, 1)
	data.Sketches.Add(2, 2, 2, 1)
	raw, err := data.marshal()
	require.NoError(t, err)
	calls := 0
	require.NoError(t, series(raw).sketchRange(1, 2, 0, func(time.Time, Sketch) bool {
		calls++
		return false
	}))
	assert.Equal(t, 1, calls)

	store := newMemStore()
	store.loadErr = errTest
	db, err := New(store)
	require.NoError(t, err)
	_, err = db.Sketches("x").Values(context.Background(), time.Unix(1, 0), time.Unix(2, 0))
	assert.ErrorIs(t, err, errTest)
}

func FuzzSketchQuantile(f *testing.F) {
	f.Add(1.0)
	f.Add(-1000.0)
	f.Add(0.0)
	f.Fuzz(func(t *testing.T, value float64) {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			t.Skip()
		}
		var sketch sketchValue
		sketch.addApprox(value)
		got := sketch.encode().Quantile(0.5)
		if value == 0 {
			assert.Zero(t, got)
			return
		}
		if math.Abs(value) < 0x1p-1022 {
			t.Skip()
		}
		assert.LessOrEqual(t, math.Abs(got-value), sketchAlpha*math.Abs(value))
	})
}

func collectSketches(values func(func(time.Time, Sketch) bool)) []Sketch {
	out := make([]Sketch, 0)
	values(func(_ time.Time, value Sketch) bool {
		out = append(out, value)
		return true
	})
	return out
}

var sketchBenchValue float64

func BenchmarkSketch(b *testing.B) {
	var exactValue sketchValue
	var approxValue sketchValue
	for i := range 1024 {
		value := math.Exp(float64(i-512) / 64)
		exactValue.addExact(value)
		approxValue.addApprox(value)
	}
	exact := exactValue.encode()
	approx := approxValue.encode()

	b.Run("count", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sketchBenchValue = float64(exact.Count())
		}
	})
	b.Run("quantile_exact", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sketchBenchValue = exact.Quantile(0.99)
		}
	})
	b.Run("quantile_approx", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sketchBenchValue = approx.Quantile(0.99)
		}
	})
	b.Run("encoded_size", func(b *testing.B) {
		b.ReportMetric(float64(len(exact.data)), "exact_bytes")
		b.ReportMetric(float64(len(approx.data)), "approx_bytes")
		for b.Loop() {
		}
	})
}

func BenchmarkSketchRange(b *testing.B) {
	var pending pending
	for i := range uint64(segmentSize) {
		pending.Sketches.Add(i, float64(i), i+1, 1)
	}
	raw, err := pending.marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := series(raw).sketchRange(0, segmentSize-1, 60, func(_ time.Time, value Sketch) bool {
			sketchBenchValue = value.Quantile(0.99)
			return true
		}); err != nil {
			b.Fatal(err)
		}
	}
}
