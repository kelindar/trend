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

var errInvalidSketchValue = errors.New("trend: sketch value must be finite")

// Sketches writes and reads distributions of float64 observations.
type Sketches struct {
	db  *DB
	key string
}

// Add stores one observation.
func (h Sketches) Add(ctx context.Context, at time.Time, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return errInvalidSketchValue
	}
	return h.db.writeSketch(ctx, h.key, uint64(at.Unix()), value)
}

// Values returns sketches at the finest retained resolution.
func (h Sketches) Values(ctx context.Context, from, to time.Time) (iter.Seq2[time.Time, Sketch], error) {
	data, err := h.db.load(ctx, h.key)
	if err != nil {
		return nil, err
	}
	fromUnix, toUnix := uint64(from.Unix()), uint64(to.Unix())
	return func(yield func(time.Time, Sketch) bool) {
		_ = data.sketchRange(fromUnix, toUnix, 0, yield)
	}, nil
}

// Range returns sketches merged into time buckets.
func (h Sketches) Range(ctx context.Context, from, to time.Time, span time.Duration) (iter.Seq2[time.Time, Sketch], error) {
	data, err := h.db.load(ctx, h.key)
	if err != nil {
		return nil, err
	}
	fromUnix, toUnix := uint64(from.Unix()), uint64(to.Unix())
	return func(yield func(time.Time, Sketch) bool) {
		_ = data.sketchRange(fromUnix, toUnix, uint64(span.Seconds()), yield)
	}, nil
}

// Compact compacts this series.
func (h Sketches) Compact(ctx context.Context) error {
	return h.db.compact(ctx, h.key)
}

// Sketch is an immutable encoded distribution.
type Sketch struct {
	data  []byte
	count uint64
	sum   float64
	min   float64
	max   float64
	kind  byte
}

// Count returns the number of observations.
func (h Sketch) Count() uint64 {
	return h.count
}

// Sum returns the sum of observations.
func (h Sketch) Sum() float64 {
	return h.sum
}

// Min returns the smallest observation, or NaN when empty.
func (h Sketch) Min() float64 {
	if h.count == 0 {
		return math.NaN()
	}
	return h.min
}

// Max returns the largest observation, or NaN when empty.
func (h Sketch) Max() float64 {
	if h.count == 0 {
		return math.NaN()
	}
	return h.max
}

// Mean returns the arithmetic mean, or NaN when empty.
func (h Sketch) Mean() float64 {
	if h.count == 0 {
		return math.NaN()
	}
	return h.sum / float64(h.count)
}

// Quantile returns the nearest-rank quantile, or NaN for an invalid quantile.
func (h Sketch) Quantile(q float64) float64 {
	switch {
	case h.count == 0 || math.IsNaN(q) || q < 0 || q > 1:
		return math.NaN()
	case q == 0:
		return h.min
	case q == 1:
		return h.max
	}
	r := codecReader{data: h.data}
	rank := uint64(math.Ceil(q * float64(h.count)))
	var value float64
	switch h.kind {
	case sketchExact:
		value = exactQuantile(h.data, rank)
	case sketchExactXOR:
		value = exactXORQuantile(&r, h.count, rank)
	case sketchApprox:
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
	r.sketchBinsEach(func(_ int, count uint64) bool {
		negativeTotal += count
		return true
	})
	if rank <= negativeTotal {
		target := negativeTotal - rank + 1
		var cumulative uint64
		var value float64
		negative.sketchBinsEach(func(key int, count uint64) bool {
			cumulative += count
			value = -sketchBinValue(key)
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
	r.sketchBinsEach(func(key int, count uint64) bool {
		value = sketchBinValue(key)
		if rank <= count {
			return false
		}
		rank -= count
		return true
	})
	return value
}

func (s series) sketchRange(from, to, span uint64, yield func(time.Time, Sketch) bool) error {
	var rawCount, bucketBytes int
	var hasRaw, hasBuckets bool
	err := s.scan(func(seg segment) bool {
		if seg.to < from || seg.from > to {
			return true
		}
		switch seg.kind {
		case segmentSketches:
			hasRaw = true
			rawCount += seg.count
		case segmentSketchBuckets:
			hasBuckets = true
			bucketBytes += seg.rawLen
		}
		return true
	})
	switch {
	case err != nil || !hasRaw && !hasBuckets:
		return err
	case !hasBuckets:
		return s.sketchRawRange(from, to, span, rawCount, yield)
	case !hasRaw && span == 0:
		return s.sketchBucketValues(from, to, bucketBytes, yield)
	default:
		return s.sketchMixedRange(from, to, span, yield)
	}
}

func (s series) sketchRawRange(
	from, to, span uint64,
	count int,
	yield func(time.Time, Sketch) bool,
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
		result := Sketch{
			data:  data,
			count: countValue,
			sum:   sum,
			min:   minValue,
			max:   maxValue,
			kind:  sketchExact,
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
		if seg.kind != segmentSketches || seg.to < from || seg.from > to {
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
	switch {
	case err != nil:
		return err
	case decodeErr != nil:
		return decodeErr
	}
	if !stopped {
		flush()
	}
	return nil
}

func (s series) sketchBucketValues(
	from, to uint64,
	capacity int,
	yield func(time.Time, Sketch) bool,
) error {
	arena := make([]byte, 0, capacity)
	var decodeErr error
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	err := s.scan(func(seg segment) bool {
		if seg.kind != segmentSketchBuckets || seg.to < from || seg.from > to {
			return true
		}
		raw, err := seg.decodeInto(buf.raw)
		buf.raw = raw
		if err != nil {
			decodeErr = err
			return false
		}
		decodeErr = scanSketchBuckets(raw, seg.count, func(at uint64, data []byte) error {
			if at < from || at > to {
				return nil
			}
			stored, err := storedSketch(data)
			if err != nil {
				return err
			}
			start := len(arena)
			arena = append(arena, stored.data...)
			encoded := arena[start:len(arena):len(arena)]
			stored.data = encoded
			if !yield(blockTime(at), stored) {
				return errStopSketch
			}
			return nil
		})
		return decodeErr == nil
	})
	switch {
	case err != nil:
		return err
	case decodeErr == errStopSketch:
		return nil
	default:
		return decodeErr
	}
}

func (s series) sketchMixedRange(from, to, span uint64, yield func(time.Time, Sketch) bool) error {
	values := make(map[uint64]*sketchValue)
	get := func(t uint64) *sketchValue {
		if span > 0 {
			t = bucketOf(t, span)
		}
		if values[t] == nil {
			values[t] = new(sketchValue)
		}
		return values[t]
	}
	var times [segmentSize]uint64
	var observations [segmentSize]float64
	var decodeErr error
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	err := s.scan(func(seg segment) bool {
		switch {
		case seg.kind != segmentSketches && seg.kind != segmentSketchBuckets:
			return true
		case seg.to < from || seg.from > to:
			return true
		}
		raw, err := seg.decodeInto(buf.raw)
		buf.raw = raw
		if err != nil {
			decodeErr = err
			return false
		}
		switch seg.kind {
		case segmentSketches:
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
		case segmentSketchBuckets:
			decodeErr = scanSketchBuckets(raw, seg.count, func(t uint64, data []byte) error {
				if t < from || t > to {
					return nil
				}
				return get(t).mergeBytes(data)
			})
		}
		return decodeErr == nil
	})
	switch {
	case err != nil:
		return err
	case decodeErr != nil:
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
