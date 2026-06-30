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
		data.Samples.values(fromUnix, toUnix, yield)
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
		data.Samples.rangeValues(fromUnix, toUnix, uint64(span.Seconds()), agg, yield)
	}, nil
}

// Compact compacts this series.
func (s Samples) Compact(ctx context.Context) error {
	return s.db.compact(ctx, s.key)
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
			if b.Time > to || !yield(time.Unix(int64(b.Time), 0), b.Sum/float64(b.Count)) {
				return
			}
			bucket++
			continue
		}
		t := d.Time[item]
		if t > to || !yield(time.Unix(int64(t), 0), d.Data[item]) {
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
	if len(d.Buckets) == 0 {
		var current uint64
		var f fold
		i := 0
		for i < len(d.Time) && d.Time[i] < from {
			i++
		}
		for ; i < len(d.Time); i++ {
			t := d.Time[i]
			if t > to {
				break
			}
			k := bucketOf(t, span)
			if f.count > 0 && k != current {
				if !yield(time.Unix(int64(current), 0), f.Value(agg)) {
					return
				}
				f = fold{}
			}
			current = k
			f.Add(d.Data[i])
		}
		if f.count > 0 {
			yield(time.Unix(int64(current), 0), f.Value(agg))
		}
		return
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
			return
		}
		var current uint64
		if hasItem {
			current = bucketOf(d.Time[item], span)
		}
		if !hasItem || hasBucket && bucketOf(d.Buckets[bucket].Time, span) < current {
			current = bucketOf(d.Buckets[bucket].Time, span)
		}
		var f fold
		for bucket < len(d.Buckets) && d.Buckets[bucket].Time <= to && bucketOf(d.Buckets[bucket].Time, span) == current {
			f.Merge(d.Buckets[bucket])
			bucket++
		}
		for item < len(d.Time) && d.Time[item] <= to && bucketOf(d.Time[item], span) == current {
			f.Add(d.Data[item])
			item++
		}
		if !yield(time.Unix(int64(current), 0), f.Value(agg)) {
			return
		}
	}
}
