// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"iter"
	"time"
)

// Counters writes and reads grow-only counters.
type Counters struct {
	db  *DB
	key string
}

// Add stores a positive counter delta.
func (c Counters) Add(ctx context.Context, at time.Time, delta uint64) error {
	return c.db.writeCounter(ctx, c.key, uint64(at.Unix()), delta)
}

// Values returns exact counter values where raw data is still retained.
func (c Counters) Values(ctx context.Context, from, to time.Time) (iter.Seq2[time.Time, float64], error) {
	data, err := c.db.load(ctx, c.key)
	if err != nil {
		return nil, err
	}
	fromUnix, toUnix := uint64(from.Unix()), uint64(to.Unix())
	return func(yield func(time.Time, float64) bool) {
		_ = data.counterValues(fromUnix, toUnix, yield)
	}, nil
}

// Range returns bucketed aggregate values.
func (c Counters) Range(ctx context.Context, from, to time.Time, span time.Duration, agg Agg) (iter.Seq2[time.Time, float64], error) {
	data, err := c.db.load(ctx, c.key)
	if err != nil {
		return nil, err
	}
	fromUnix, toUnix := uint64(from.Unix()), uint64(to.Unix())
	return func(yield func(time.Time, float64) bool) {
		_ = data.counterRange(fromUnix, toUnix, uint64(span.Seconds()), agg, yield)
	}, nil
}

// Compact compacts this series.
func (c Counters) Compact(ctx context.Context) error {
	return c.db.compact(ctx, c.key)
}

func (s series) counterValues(from, to uint64, yield func(time.Time, float64) bool) error {
	var times [segmentSize]uint64
	var values [segmentSize]uint64
	var current uint64
	var sum uint64
	var have bool
	var decodeErr error
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	flush := func() bool {
		if !have {
			return true
		}
		ok := yield(blockTime(current), float64(sum))
		sum, have = 0, false
		return ok
	}
	err := s.scan(func(seg segment) bool {
		switch seg.kind {
		case segmentCounters:
			if seg.to < from || seg.from > to {
				return true
			}
			raw, err := seg.decodeInto(buf.raw)
			buf.raw = raw
			if err != nil {
				decodeErr = err
				return false
			}
			if err := decodeCounterValues(raw, times[:seg.count], values[:seg.count]); err != nil {
				decodeErr = err
				return false
			}
			for i, t := range times[:seg.count] {
				if t < from || t > to {
					continue
				}
				if have && t != current && !flush() {
					return false
				}
				current, have = t, true
				sum += values[i]
			}
		case segmentCounterBuckets:
			if seg.to < from || seg.from > to {
				return true
			}
			var data counterData
			raw, err := seg.decodeInto(buf.raw)
			buf.raw = raw
			if err != nil {
				decodeErr = err
				return false
			}
			if err := decodeCounterBuckets(raw, seg.count, &data); err != nil {
				decodeErr = err
				return false
			}
			for _, b := range data.Buckets {
				if b.Time < from || b.Time > to {
					continue
				}
				if have && b.Time != current && !flush() {
					return false
				}
				current, have = b.Time, true
				sum += b.Sum
			}
		}
		return true
	})
	switch {
	case err != nil:
		return err
	case decodeErr != nil:
		return decodeErr
	}
	flush()
	return nil
}

func (s series) counterRange(from, to, span uint64, agg Agg, yield func(time.Time, float64) bool) error {
	if span == 0 {
		return s.counterValues(from, to, yield)
	}
	var times [segmentSize]uint64
	var values [segmentSize]uint64
	var current uint64
	var f fold
	var decodeErr error
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	err := s.scan(func(seg segment) bool {
		switch seg.kind {
		case segmentCounters:
			if seg.to < from || seg.from > to {
				return true
			}
			raw, err := seg.decodeInto(buf.raw)
			buf.raw = raw
			if err != nil {
				decodeErr = err
				return false
			}
			if err := decodeCounterValues(raw, times[:seg.count], values[:seg.count]); err != nil {
				decodeErr = err
				return false
			}
			for i, t := range times[:seg.count] {
				if t < from || t > to {
					continue
				}
				k := bucketOf(t, span)
				if f.count > 0 && k != current {
					if !yield(blockTime(current), f.Value(agg)) {
						return false
					}
					f = fold{}
				}
				current = k
				f.Add(float64(values[i]))
			}
		case segmentCounterBuckets:
			if seg.to < from || seg.from > to {
				return true
			}
			var data counterData
			raw, err := seg.decodeInto(buf.raw)
			buf.raw = raw
			if err != nil {
				decodeErr = err
				return false
			}
			if err := decodeCounterBuckets(raw, seg.count, &data); err != nil {
				decodeErr = err
				return false
			}
			for _, b := range data.Buckets {
				if b.Time < from || b.Time > to {
					continue
				}
				k := bucketOf(b.Time, span)
				if f.count > 0 && k != current {
					if !yield(blockTime(current), f.Value(agg)) {
						return false
					}
					f = fold{}
				}
				current = k
				f.Add(float64(b.Sum))
			}
		}
		return true
	})
	switch {
	case err != nil:
		return err
	case decodeErr != nil:
		return decodeErr
	}
	if f.count > 0 {
		yield(blockTime(current), f.Value(agg))
	}
	return nil
}

func (d counterData) values(from, to uint64, yield func(time.Time, float64) bool) {
	var current uint64
	var sum uint64
	var have bool
	flush := func() bool {
		if !have {
			return true
		}
		ok := yield(blockTime(current), float64(sum))
		sum, have = 0, false
		return ok
	}

	bucket, item := 0, 0
	for bucket < len(d.Buckets) && d.Buckets[bucket].Time < from {
		bucket++
	}
	for item < len(d.Time) && d.Time[item] < from {
		item++
	}
	for bucket < len(d.Buckets) || item < len(d.Time) {
		if item >= len(d.Time) || bucket < len(d.Buckets) && d.Buckets[bucket].Time <= d.Time[item] {
			t := d.Buckets[bucket].Time
			if t > to {
				break
			}
			if have && t != current && !flush() {
				return
			}
			current, have = t, true
			sum += d.Buckets[bucket].Sum
			bucket++
			continue
		}
		t := d.Time[item]
		if t > to {
			break
		}
		if have && t != current && !flush() {
			return
		}
		current, have = t, true
		sum += d.Value[item]
		item++
	}
	flush()
}

func (d counterData) rangeValues(from, to, span uint64, agg Agg, yield func(time.Time, float64) bool) {
	if span == 0 {
		d.values(from, to, yield)
		return
	}
	var current uint64
	var f fold
	add := func(t uint64, v float64) bool {
		k := bucketOf(t, span)
		if f.count > 0 && k != current {
			if !yield(blockTime(current), f.Value(agg)) {
				return false
			}
			f = fold{}
		}
		current = k
		f.Add(v)
		return true
	}

	bucket, item := 0, 0
	for bucket < len(d.Buckets) && d.Buckets[bucket].Time < from {
		bucket++
	}
	for item < len(d.Time) && d.Time[item] < from {
		item++
	}
	for {
		hasBucket := bucket < len(d.Buckets) && d.Buckets[bucket].Time <= to
		hasItem := item < len(d.Time) && d.Time[item] <= to
		if !hasBucket && !hasItem {
			break
		}
		if !hasItem || hasBucket && d.Buckets[bucket].Time <= d.Time[item] {
			if !add(d.Buckets[bucket].Time, float64(d.Buckets[bucket].Sum)) {
				return
			}
			bucket++
			continue
		}
		if !add(d.Time[item], float64(d.Value[item])) {
			return
		}
		item++
	}
	if f.count > 0 {
		yield(blockTime(current), f.Value(agg))
	}
}
