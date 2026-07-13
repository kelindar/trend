// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"iter"
	"time"
)

// Samples writes and reads float64 samples.
type Samples struct {
	db  *DB
	key string
}

// Set stores a sample using LWW CRDT semantics.
func (s Samples) Set(ctx context.Context, at time.Time, value float64) error {
	return s.db.writeSample(ctx, s.key, uint64(at.Unix()), value)
}

// Values returns exact values where raw data is still retained.
func (s Samples) Values(ctx context.Context, from, to time.Time) (iter.Seq2[time.Time, float64], error) {
	data, err := s.db.load(ctx, s.key)
	if err != nil {
		return nil, err
	}
	fromUnix, toUnix := uint64(from.Unix()), uint64(to.Unix())
	return func(yield func(time.Time, float64) bool) {
		_ = data.sampleValues(fromUnix, toUnix, yield)
	}, nil
}

// Range returns bucketed aggregate values.
func (s Samples) Range(ctx context.Context, from, to time.Time, span time.Duration, agg Agg) (iter.Seq2[time.Time, float64], error) {
	data, err := s.db.load(ctx, s.key)
	if err != nil {
		return nil, err
	}
	fromUnix, toUnix := uint64(from.Unix()), uint64(to.Unix())
	return func(yield func(time.Time, float64) bool) {
		_ = data.sampleRange(fromUnix, toUnix, uint64(span.Seconds()), agg, yield)
	}, nil
}

// Compact compacts this series.
func (s Samples) Compact(ctx context.Context) error {
	return s.db.compact(ctx, s.key)
}

func (s series) sampleValues(from, to uint64, yield func(time.Time, float64) bool) error {
	var times [segmentSize]uint64
	var values [segmentSize]float64
	var decodeErr error
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	err := s.scan(func(seg segment) bool {
		switch seg.kind {
		case segmentSamples:
			if seg.to < from || seg.from > to {
				return true
			}
			raw, err := seg.decodeInto(buf.raw)
			buf.raw = raw
			if err != nil {
				decodeErr = err
				return false
			}
			if err := decodeSampleValues(raw, times[:seg.count], values[:seg.count]); err != nil {
				decodeErr = err
				return false
			}
			for i, t := range times[:seg.count] {
				if t >= from && t <= to && !yield(blockTime(t), values[i]) {
					return false
				}
			}
		case segmentSampleBuckets:
			if seg.to < from || seg.from > to {
				return true
			}
			var data sampleData
			raw, err := seg.decodeInto(buf.raw)
			buf.raw = raw
			if err != nil {
				decodeErr = err
				return false
			}
			if err := decodeSampleBuckets(raw, seg.count, &data); err != nil {
				decodeErr = err
				return false
			}
			for _, b := range data.Buckets {
				if b.Time >= from && b.Time <= to && !yield(blockTime(b.Time), b.Sum/float64(b.Count)) {
					return false
				}
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	return decodeErr
}

func (s series) sampleRange(from, to, span uint64, agg Agg, yield func(time.Time, float64) bool) error {
	if span == 0 {
		return s.sampleValues(from, to, yield)
	}
	var times [segmentSize]uint64
	var values [segmentSize]float64
	var current uint64
	var f fold
	var decodeErr error
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	err := s.scan(func(seg segment) bool {
		switch seg.kind {
		case segmentSamples:
			if seg.to < from || seg.from > to {
				return true
			}
			raw, err := seg.decodeInto(buf.raw)
			buf.raw = raw
			if err != nil {
				decodeErr = err
				return false
			}
			if err := decodeSampleValues(raw, times[:seg.count], values[:seg.count]); err != nil {
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
				f.Add(values[i])
			}
		case segmentSampleBuckets:
			if seg.to < from || seg.from > to {
				return true
			}
			var data sampleData
			raw, err := seg.decodeInto(buf.raw)
			buf.raw = raw
			if err != nil {
				decodeErr = err
				return false
			}
			if err := decodeSampleBuckets(raw, seg.count, &data); err != nil {
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
				f.Merge(b)
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

func (d sampleData) values(from, to uint64, yield func(time.Time, float64) bool) {
	bucket, item := 0, 0
	for bucket < len(d.Buckets) && d.Buckets[bucket].Time < from {
		bucket++
	}
	for item < len(d.Time) && d.Time[item] < from {
		item++
	}
	for bucket < len(d.Buckets) || item < len(d.Time) {
		if item >= len(d.Time) || bucket < len(d.Buckets) && d.Buckets[bucket].Time <= d.Time[item] {
			b := d.Buckets[bucket]
			if b.Time > to || !yield(blockTime(b.Time), b.Sum/float64(b.Count)) {
				return
			}
			bucket++
			continue
		}
		t := d.Time[item]
		if t > to || !yield(blockTime(t), d.Data[item]) {
			return
		}
		item++
	}
}

func (d sampleData) rangeValues(from, to, span uint64, agg Agg, yield func(time.Time, float64) bool) {
	if span == 0 {
		d.values(from, to, yield)
		return
	}
	var current uint64
	var f fold
	addValue := func(t uint64, v float64) bool {
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
			b := d.Buckets[bucket]
			k := bucketOf(b.Time, span)
			if f.count > 0 && k != current {
				if !yield(blockTime(current), f.Value(agg)) {
					return
				}
				f = fold{}
			}
			current = k
			f.Merge(b)
			bucket++
			continue
		}
		if !addValue(d.Time[item], d.Data[item]) {
			return
		}
		item++
	}
	if f.count > 0 {
		yield(blockTime(current), f.Value(agg))
	}
}
