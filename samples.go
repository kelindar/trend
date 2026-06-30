// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"iter"
	"time"

	"github.com/kelindar/binary/sorted"
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
	return data.Samples.values(uint64(from.Unix()), uint64(to.Unix())), nil
}

// Range returns bucketed aggregate values.
func (s Samples) Range(ctx context.Context, from, to time.Time, span time.Duration, agg Agg) (iter.Seq2[time.Time, float64], error) {
	data, err := s.db.load(ctx, s.key)
	if err != nil {
		return nil, err
	}
	return data.Samples.rangeValues(uint64(from.Unix()), uint64(to.Unix()), uint64(span.Seconds()), agg), nil
}

// Compact compacts this series.
func (s Samples) Compact(ctx context.Context) error {
	return s.db.compact(ctx, s.key)
}

func (d sampleData) values(from, to uint64) iter.Seq2[time.Time, float64] {
	return func(yield func(time.Time, float64) bool) {
		raw := sorted.TimeSeries{Time: d.Time, Data: d.Data}
		bucket, item := 0, 0
		for bucket < len(d.Buckets) && d.Buckets[bucket].Time < from {
			bucket++
		}
		for item < len(raw.Time) && raw.Time[item] < from {
			item++
		}
		for bucket < len(d.Buckets) || item < len(raw.Time) {
			if item >= len(raw.Time) || bucket < len(d.Buckets) && d.Buckets[bucket].Time <= raw.Time[item] {
				b := d.Buckets[bucket]
				if b.Time > to || !yield(time.Unix(int64(b.Time), 0), b.Sum/float64(b.Count)) {
					return
				}
				bucket++
				continue
			}
			t := raw.Time[item]
			if t > to || !yield(time.Unix(int64(t), 0), raw.Data[item]) {
				return
			}
			item++
		}
	}
}

func (d sampleData) rangeValues(from, to, span uint64, agg Agg) iter.Seq2[time.Time, float64] {
	if span == 0 {
		return d.values(from, to)
	}
	if len(d.Buckets) == 0 {
		return func(yield func(time.Time, float64) bool) {
			var current uint64
			var f fold
			raw := sorted.TimeSeries{Time: d.Time, Data: d.Data}
			flush := func() bool {
				return f.count == 0 || yield(time.Unix(int64(current), 0), f.Value(agg))
			}
			i := 0
			for i < len(raw.Time) && raw.Time[i] < from {
				i++
			}
			for ; i < len(raw.Time); i++ {
				t := raw.Time[i]
				if t > to {
					break
				}
				k := bucketOf(t, span)
				if f.count > 0 && k != current {
					if !flush() {
						return
					}
					f = fold{}
				}
				current = k
				f.Add(raw.Data[i])
			}
			flush()
		}
	}
	return func(yield func(time.Time, float64) bool) {
		bucket, item := 0, 0
		raw := sorted.TimeSeries{Time: d.Time, Data: d.Data}
		for bucket < len(d.Buckets) && d.Buckets[bucket].Time < from {
			bucket++
		}
		for item < len(raw.Time) && raw.Time[item] < from {
			item++
		}

		for {
			hasBucket := bucket < len(d.Buckets) && d.Buckets[bucket].Time <= to
			hasItem := item < len(raw.Time) && raw.Time[item] <= to
			if !hasBucket && !hasItem {
				return
			}
			var current uint64
			if hasItem {
				current = bucketOf(raw.Time[item], span)
			}
			if !hasItem || hasBucket && bucketOf(d.Buckets[bucket].Time, span) < current {
				current = bucketOf(d.Buckets[bucket].Time, span)
			}
			var f fold
			for bucket < len(d.Buckets) && d.Buckets[bucket].Time <= to && bucketOf(d.Buckets[bucket].Time, span) == current {
				f.Merge(d.Buckets[bucket])
				bucket++
			}
			for item < len(raw.Time) && raw.Time[item] <= to && bucketOf(raw.Time[item], span) == current {
				f.Add(raw.Data[item])
				item++
			}
			if !yield(time.Unix(int64(current), 0), f.Value(agg)) {
				return
			}
		}
	}
}
