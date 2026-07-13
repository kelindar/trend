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

func TestHistograms(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithReplica("histograms"))
	require.NoError(t, err)
	histograms := db.Histograms("latency")
	at := time.Unix(60, 0)
	for _, value := range []float64{10, -10, 5, 0} {
		require.NoError(t, histograms.Add(ctx, at, value))
	}
	require.NoError(t, histograms.Add(ctx, at.Add(time.Second), 20))

	values, err := histograms.Values(ctx, at, at.Add(time.Second))
	require.NoError(t, err)
	got := collectHistograms(values)
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

	ranged, err := histograms.Range(ctx, at, at.Add(time.Second), time.Minute)
	require.NoError(t, err)
	merged := collectHistograms(ranged)
	require.Len(t, merged, 1)
	assert.Equal(t, uint64(5), merged[0].Count())
	assert.Equal(t, 25.0, merged[0].Sum())
	assert.Equal(t, 20.0, merged[0].Max())

	t.Run("invalid and empty values", func(t *testing.T) {
		assert.Error(t, histograms.Add(ctx, at, math.NaN()))
		assert.Error(t, histograms.Add(ctx, at, math.Inf(1)))
		assert.Equal(t, uint64(0), (Histogram{}).Count())
		assert.Zero(t, (Histogram{}).Sum())
		assert.True(t, math.IsNaN((Histogram{}).Min()))
		assert.True(t, math.IsNaN((Histogram{}).Max()))
		assert.True(t, math.IsNaN((Histogram{}).Mean()))
		assert.True(t, math.IsNaN((Histogram{}).Quantile(0.5)))
		assert.True(t, math.IsNaN(got[0].Quantile(-0.1)))
		assert.True(t, math.IsNaN(got[0].Quantile(1.1)))
		assert.True(t, math.IsNaN(got[0].Quantile(math.NaN())))
	})
}

func TestHistogramCompaction(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithCompaction(time.Hour, time.Minute))
	require.NoError(t, err)
	histograms := db.Histograms("latency")
	at := time.Now().Add(-2 * time.Hour).Truncate(time.Minute)
	for _, value := range []float64{-100, -1, 0, 1, 100} {
		require.NoError(t, histograms.Add(ctx, at, value))
	}
	require.NoError(t, histograms.Compact(ctx))

	values, err := histograms.Values(ctx, at, at.Add(time.Minute))
	require.NoError(t, err)
	got := collectHistograms(values)
	require.Len(t, got, 1)
	assert.Equal(t, uint64(5), got[0].Count())
	assert.Equal(t, 0.0, got[0].Sum())
	assert.Equal(t, -100.0, got[0].Min())
	assert.Equal(t, 100.0, got[0].Max())
	assert.InDelta(t, -1, got[0].Quantile(0.25), 0.01)
	assert.Equal(t, 0.0, got[0].Quantile(0.5))
	assert.InDelta(t, 1, got[0].Quantile(0.75), 0.01)

	// A late raw observation merges with the compacted bucket on reads.
	require.NoError(t, histograms.Add(ctx, at, 200))
	values, err = histograms.Values(ctx, at, at)
	require.NoError(t, err)
	got = collectHistograms(values)
	require.Len(t, got, 1)
	assert.Equal(t, uint64(6), got[0].Count())
	assert.Equal(t, 200.0, got[0].Max())
}

func TestHistogramBuffer(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithFlushEvery(time.Hour))
	require.NoError(t, err)
	histograms := db.Histograms("x")
	require.NoError(t, histograms.Add(ctx, time.Unix(1, 0), 1))
	require.NoError(t, histograms.Add(ctx, time.Unix(2, 0), 2))
	values, err := histograms.Values(ctx, time.Unix(1, 0), time.Unix(2, 0))
	require.NoError(t, err)
	got := collectHistograms(values)
	require.Len(t, got, 2)
	first := got[0]
	assert.Equal(t, 1.0, first.Quantile(0.5))
	require.NoError(t, db.Flush(ctx))
	assert.Equal(t, 1.0, first.Quantile(0.5))
}

func TestHistogramCodec(t *testing.T) {
	var a, b histogramData
	a.Add(1, 10, 1, 1)
	b.Add(1, 20, 2, 1)
	a.Merge(b)
	a.Merge(b)
	assert.Len(t, a.Time, 2)

	var left, right histogramData
	left.Add(2, 1, 7, 3)
	right.Add(2, 2, 7, 3)
	var leftRight, rightLeft histogramData
	leftRight.Merge(left)
	leftRight.Merge(right)
	rightLeft.Merge(right)
	rightLeft.Merge(left)
	assert.Equal(t, leftRight.Data, rightLeft.Data)
	assert.Equal(t, []float64{2}, leftRight.Data)

	a.Compact(2, 10)
	require.Len(t, a.Buckets, 1)
	decoded, err := decodeHistogram(a.Buckets[0].Data)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), decoded.count)

	var state pending
	state.Histograms.Add(1, -1, 1, 1)
	state.Histograms.Add(1, 1, 2, 1)
	state.Histograms.Buckets = a.Buckets
	raw, err := state.marshal()
	require.NoError(t, err)
	roundTrip, err := decode(raw)
	require.NoError(t, err)
	assert.Equal(t, state.Histograms, roundTrip.Histograms)

	var only pending
	only.Histograms.Add(1, 2, 3, 4)
	onlyRaw, err := only.marshal()
	require.NoError(t, err)
	var histogramSegment segment
	require.NoError(t, series(onlyRaw).scan(func(current segment) bool {
		if current.kind == segmentHistograms {
			histogramSegment = current
		}
		return true
	}))
	histogramRaw, err := histogramSegment.decodeInto(nil)
	require.NoError(t, err)
	sampleRaw := appendSampleRaw(nil, []uint64{1}, []float64{2}, []uint64{3}, []uint64{4})
	assert.Equal(t, sampleRaw, histogramRaw)
}

func TestHistogramBinLimit(t *testing.T) {
	var value histogramValue
	for i := -550; i < 550; i++ {
		value.addApprox(math.Pow(histogramGamma, float64(i)))
	}
	assert.LessOrEqual(t, len(value.negative)+len(value.positive), histogramMaxBins)
	encoded := value.encodeApprox()
	decoded, err := decodeHistogram(encoded)
	require.NoError(t, err)
	assert.Equal(t, uint64(1100), decoded.count)
	assert.Equal(t, value.min, decoded.min)
	assert.Equal(t, value.max, decoded.max)

	var reverse histogramValue
	for i := 549; i >= -550; i-- {
		reverse.addApprox(math.Pow(histogramGamma, float64(i)))
	}
	reverseDecoded, err := decodeHistogram(reverse.encodeApprox())
	require.NoError(t, err)
	assert.Equal(t, decoded.zero, reverseDecoded.zero)
	assert.Equal(t, decoded.positive, reverseDecoded.positive)
}

func TestHistogramErrors(t *testing.T) {
	value := histogramValue{count: 1, min: 1, max: 1, sum: 1}
	_, err := decodeHistogram(appendHistogramHeader(nil, 99, &value))
	assert.ErrorIs(t, err, errVarintCodec)
	_, err = decodeHistogram(appendHistogramExact(nil, &value))
	assert.Error(t, err)
	_, err = decodeHistogram(appendHistogramApprox(nil, &value))
	assert.ErrorIs(t, err, errShapeCodec)

	tooMany := histogramValue{count: histogramMaxBins + 1, min: 1, max: 1, positive: make(map[int]uint64)}
	for i := range histogramMaxBins + 1 {
		tooMany.positive[i] = 1
	}
	_, err = decodeHistogram(appendHistogramApprox(nil, &tooMany))
	assert.ErrorIs(t, err, errLargeCodec)

	buf := encodeBuffers.Get().(*codecBuffer)
	bad := appendSegment(nil, segmentHistograms, 1, 1, 1, []byte{1}, buf)
	putEncodeBuffer(buf)
	var good pending
	good.Histograms.Add(10, 10, 1, 1)
	goodRaw, err := good.marshal()
	require.NoError(t, err)
	mixed := series(append(append([]byte{version}, bad...), goodRaw[1:]...))
	var got []Histogram
	require.NoError(t, mixed.histogramRange(10, 10, 0, func(_ time.Time, value Histogram) bool {
		got = append(got, value)
		return true
	}))
	require.Len(t, got, 1)
	assert.Equal(t, 10.0, got[0].Min())
	assert.Error(t, mixed.histogramRange(1, 1, 0, func(time.Time, Histogram) bool { return true }))
}

func TestHistogramIterator(t *testing.T) {
	var data pending
	data.Histograms.Add(1, 1, 1, 1)
	data.Histograms.Add(2, 2, 2, 1)
	raw, err := data.marshal()
	require.NoError(t, err)
	calls := 0
	require.NoError(t, series(raw).histogramRange(1, 2, 0, func(time.Time, Histogram) bool {
		calls++
		return false
	}))
	assert.Equal(t, 1, calls)

	store := newMemStore()
	store.loadErr = errTest
	db, err := New(store)
	require.NoError(t, err)
	_, err = db.Histograms("x").Values(context.Background(), time.Unix(1, 0), time.Unix(2, 0))
	assert.ErrorIs(t, err, errTest)
}

func FuzzHistogramQuantile(f *testing.F) {
	f.Add(1.0)
	f.Add(-1000.0)
	f.Add(0.0)
	f.Fuzz(func(t *testing.T, value float64) {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			t.Skip()
		}
		var sketch histogramValue
		sketch.addApprox(value)
		got := sketch.encode().Quantile(0.5)
		if value == 0 {
			assert.Zero(t, got)
			return
		}
		if math.Abs(value) < 0x1p-1022 {
			t.Skip()
		}
		assert.LessOrEqual(t, math.Abs(got-value), histogramAlpha*math.Abs(value))
	})
}

func collectHistograms(values func(func(time.Time, Histogram) bool)) []Histogram {
	out := make([]Histogram, 0)
	values(func(_ time.Time, value Histogram) bool {
		out = append(out, value)
		return true
	})
	return out
}

var histogramBenchValue float64

func BenchmarkHistogram(b *testing.B) {
	var exactValue histogramValue
	var approxValue histogramValue
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
			histogramBenchValue = float64(exact.Count())
		}
	})
	b.Run("quantile_exact", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			histogramBenchValue = exact.Quantile(0.99)
		}
	})
	b.Run("quantile_approx", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			histogramBenchValue = approx.Quantile(0.99)
		}
	})
	b.Run("encoded_size", func(b *testing.B) {
		b.ReportMetric(float64(len(exact.data)), "exact_bytes")
		b.ReportMetric(float64(len(approx.data)), "approx_bytes")
		for b.Loop() {
		}
	})
}

func BenchmarkHistogramRange(b *testing.B) {
	var pending pending
	for i := range uint64(segmentSize) {
		pending.Histograms.Add(i, float64(i), i+1, 1)
	}
	raw, err := pending.marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := series(raw).histogramRange(0, segmentSize-1, 60, func(_ time.Time, value Histogram) bool {
			histogramBenchValue = value.Quantile(0.99)
			return true
		}); err != nil {
			b.Fatal(err)
		}
	}
}
