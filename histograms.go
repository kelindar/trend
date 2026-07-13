// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"encoding/binary"
	"errors"
	"iter"
	"math"
	"slices"
	"time"
)

var errInvalidHistogramValue = errors.New("trend: histogram value must be finite")

// Histograms writes and reads distributions of float64 observations.
type Histograms struct {
	db  *DB
	key string
}

// Add stores one observation.
func (h Histograms) Add(ctx context.Context, at time.Time, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return errInvalidHistogramValue
	}
	return h.db.writeHistogram(ctx, h.key, uint64(at.Unix()), value)
}

// Values returns histograms at the finest retained resolution.
func (h Histograms) Values(ctx context.Context, from, to time.Time) (iter.Seq2[time.Time, Histogram], error) {
	data, err := h.db.load(ctx, h.key)
	if err != nil {
		return nil, err
	}
	fromUnix, toUnix := uint64(from.Unix()), uint64(to.Unix())
	return func(yield func(time.Time, Histogram) bool) {
		_ = data.histogramRange(fromUnix, toUnix, 0, yield)
	}, nil
}

// Range returns histograms merged into time buckets.
func (h Histograms) Range(ctx context.Context, from, to time.Time, span time.Duration) (iter.Seq2[time.Time, Histogram], error) {
	data, err := h.db.load(ctx, h.key)
	if err != nil {
		return nil, err
	}
	fromUnix, toUnix := uint64(from.Unix()), uint64(to.Unix())
	return func(yield func(time.Time, Histogram) bool) {
		_ = data.histogramRange(fromUnix, toUnix, uint64(span.Seconds()), yield)
	}, nil
}

// Compact compacts this series.
func (h Histograms) Compact(ctx context.Context) error {
	return h.db.compact(ctx, h.key)
}

// Histogram is an immutable encoded distribution.
type Histogram struct {
	data  []byte
	count uint64
	sum   float64
	min   float64
	max   float64
	kind  byte
}

// Count returns the number of observations.
func (h Histogram) Count() uint64 {
	return h.count
}

// Sum returns the sum of observations.
func (h Histogram) Sum() float64 {
	return h.sum
}

// Min returns the smallest observation, or NaN when empty.
func (h Histogram) Min() float64 {
	if h.count == 0 {
		return math.NaN()
	}
	return h.min
}

// Max returns the largest observation, or NaN when empty.
func (h Histogram) Max() float64 {
	if h.count == 0 {
		return math.NaN()
	}
	return h.max
}

// Mean returns the arithmetic mean, or NaN when empty.
func (h Histogram) Mean() float64 {
	if h.count == 0 {
		return math.NaN()
	}
	return h.sum / float64(h.count)
}

// Quantile returns the nearest-rank quantile, or NaN for an invalid quantile.
func (h Histogram) Quantile(q float64) float64 {
	if h.count == 0 || math.IsNaN(q) || q < 0 || q > 1 {
		return math.NaN()
	}
	if q == 0 {
		return h.min
	}
	if q == 1 {
		return h.max
	}
	r := codecReader{data: h.data}
	rank := uint64(math.Ceil(q * float64(h.count)))
	var value float64
	switch h.kind {
	case histogramExact:
		value = exactQuantile(h.data, rank)
	case histogramExactXOR:
		value = exactXORQuantile(&r, h.count, rank)
	case histogramApprox:
		value = approximateQuantile(&r, rank)
	default:
		return math.NaN()
	}
	if r.err != nil {
		return math.NaN()
	}
	return min(max(value, h.min), h.max)
}

func exactQuantile(data []byte, rank uint64) float64 {
	if rank == 0 || rank > uint64(len(data)/8) {
		return math.NaN()
	}
	offset := int(rank-1) * 8
	return math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
}

func exactXORQuantile(r *codecReader, count, rank uint64) float64 {
	if count > uint64(int(^uint(0)>>1)) {
		r.err = errLargeCodec
		return 0
	}
	var scratch [segmentSize]float64
	values := scratch[:min(int(count), len(scratch))]
	if count > segmentSize {
		values = make([]float64, int(count))
	}
	r.floatXor(values)
	slices.Sort(values)
	return values[rank-1]
}

func appendExactValues(dst []byte, values []float64) []byte {
	for _, value := range values {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(value))
	}
	return dst
}

func approximateQuantile(r *codecReader, rank uint64) float64 {
	zero := r.uvarint()
	negative := *r
	var negativeTotal uint64
	r.histogramBinsEach(func(_ int, count uint64) bool {
		negativeTotal += count
		return true
	})
	if rank <= negativeTotal {
		target := negativeTotal - rank + 1
		var cumulative uint64
		var value float64
		negative.histogramBinsEach(func(key int, count uint64) bool {
			cumulative += count
			value = -histogramBinValue(key)
			return cumulative < target
		})
		r.err = negative.err
		return value
	}
	rank -= negativeTotal
	if rank <= zero {
		return 0
	}
	rank -= zero
	var value float64
	r.histogramBinsEach(func(key int, count uint64) bool {
		value = histogramBinValue(key)
		if rank <= count {
			return false
		}
		rank -= count
		return true
	})
	return value
}

func (s series) histogramRange(from, to, span uint64, yield func(time.Time, Histogram) bool) error {
	var rawCount, bucketBytes int
	var hasRaw, hasBuckets bool
	err := s.scan(func(seg segment) bool {
		if seg.to < from || seg.from > to {
			return true
		}
		switch seg.kind {
		case segmentHistograms:
			hasRaw = true
			rawCount += seg.count
		case segmentHistogramBuckets:
			hasBuckets = true
			bucketBytes += seg.rawLen
		}
		return true
	})
	if err != nil || !hasRaw && !hasBuckets {
		return err
	}
	if !hasBuckets {
		return s.histogramRawRange(from, to, span, rawCount, yield)
	}
	if !hasRaw && span == 0 {
		return s.histogramBucketValues(from, to, bucketBytes, yield)
	}
	return s.histogramMixedRange(from, to, span, yield)
}

func (s series) histogramRawRange(
	from, to, span uint64,
	count int,
	yield func(time.Time, Histogram) bool,
) error {
	arena := make([]byte, 0, count*8)
	var exactScratch [segmentSize]float64
	exact := exactScratch[:0]
	var countValue uint64
	var sum, minValue, maxValue float64
	var start int
	var currentTime uint64
	var stopped bool
	flush := func() bool {
		if countValue == 0 {
			return true
		}
		slices.Sort(exact)
		arena = appendExactValues(arena, exact)
		data := arena[start:len(arena):len(arena)]
		result := Histogram{
			data:  data,
			count: countValue,
			sum:   sum,
			min:   minValue,
			max:   maxValue,
			kind:  histogramExact,
		}
		if !yield(blockTime(currentTime), result) {
			stopped = true
			return false
		}
		exact = exactScratch[:0]
		countValue, sum = 0, 0
		start = len(arena)
		return true
	}

	var times [segmentSize]uint64
	var observations [segmentSize]float64
	var decodeErr error
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	err := s.scan(func(seg segment) bool {
		if seg.kind != segmentHistograms || seg.to < from || seg.from > to {
			return true
		}
		raw, err := seg.decodeInto(buf.raw)
		buf.raw = raw
		if err != nil {
			decodeErr = err
			return false
		}
		decodeErr = decodeSampleValues(raw, times[:seg.count], observations[:seg.count])
		if decodeErr != nil {
			return false
		}
		for i, at := range times[:seg.count] {
			value := observations[i]
			if math.IsNaN(value) || math.IsInf(value, 0) {
				decodeErr = errShapeCodec
				return false
			}
			if at < from || at > to {
				continue
			}
			outputTime := at
			if span > 0 {
				outputTime = bucketOf(at, span)
			}
			if countValue > 0 && outputTime != currentTime && !flush() {
				return false
			}
			currentTime = outputTime
			if countValue == 0 {
				minValue, maxValue = value, value
			} else {
				minValue = min(minValue, value)
				maxValue = max(maxValue, value)
			}
			countValue++
			sum += value
			exact = append(exact, value)
		}
		return true
	})
	if err != nil {
		return err
	}
	if decodeErr != nil {
		return decodeErr
	}
	if !stopped {
		flush()
	}
	return nil
}

func (s series) histogramBucketValues(
	from, to uint64,
	capacity int,
	yield func(time.Time, Histogram) bool,
) error {
	arena := make([]byte, 0, capacity)
	var decodeErr error
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	err := s.scan(func(seg segment) bool {
		if seg.kind != segmentHistogramBuckets || seg.to < from || seg.from > to {
			return true
		}
		raw, err := seg.decodeInto(buf.raw)
		buf.raw = raw
		if err != nil {
			decodeErr = err
			return false
		}
		decodeErr = scanHistogramBuckets(raw, seg.count, func(at uint64, data []byte) error {
			if at < from || at > to {
				return nil
			}
			stored, err := storedHistogram(data)
			if err != nil {
				return err
			}
			start := len(arena)
			arena = append(arena, stored.data...)
			encoded := arena[start:len(arena):len(arena)]
			stored.data = encoded
			if !yield(blockTime(at), stored) {
				return errStopHistogram
			}
			return nil
		})
		return decodeErr == nil
	})
	if err != nil {
		return err
	}
	if decodeErr == errStopHistogram {
		return nil
	}
	return decodeErr
}

func (s series) histogramMixedRange(from, to, span uint64, yield func(time.Time, Histogram) bool) error {
	values := make(map[uint64]*histogramValue)
	get := func(t uint64) *histogramValue {
		if span > 0 {
			t = bucketOf(t, span)
		}
		if values[t] == nil {
			values[t] = new(histogramValue)
		}
		return values[t]
	}
	var times [segmentSize]uint64
	var observations [segmentSize]float64
	var decodeErr error
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	err := s.scan(func(seg segment) bool {
		if seg.kind != segmentHistograms && seg.kind != segmentHistogramBuckets {
			return true
		}
		if seg.to < from || seg.from > to {
			return true
		}
		raw, err := seg.decodeInto(buf.raw)
		buf.raw = raw
		if err != nil {
			decodeErr = err
			return false
		}
		switch seg.kind {
		case segmentHistograms:
			decodeErr = decodeSampleValues(raw, times[:seg.count], observations[:seg.count])
			if decodeErr == nil {
				for i, t := range times[:seg.count] {
					if math.IsNaN(observations[i]) || math.IsInf(observations[i], 0) {
						decodeErr = errShapeCodec
						break
					}
					if t >= from && t <= to {
						get(t).addExact(observations[i])
					}
				}
			}
		case segmentHistogramBuckets:
			decodeErr = scanHistogramBuckets(raw, seg.count, func(t uint64, data []byte) error {
				if t < from || t > to {
					return nil
				}
				return get(t).mergeBytes(data)
			})
		}
		return decodeErr == nil
	})
	if err != nil {
		return err
	}
	if decodeErr != nil {
		return decodeErr
	}
	outputTimes := make([]uint64, 0, len(values))
	for t := range values {
		outputTimes = append(outputTimes, t)
	}
	slices.Sort(outputTimes)
	for _, t := range outputTimes {
		if !yield(blockTime(t), values[t].encode()) {
			break
		}
	}
	return nil
}
