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
	return data.Counters.values(uint64(from.Unix()), uint64(to.Unix())), nil
}

// Range returns bucketed aggregate values.
func (c Counters) Range(ctx context.Context, from, to time.Time, span time.Duration, agg Agg) (iter.Seq2[time.Time, float64], error) {
	data, err := c.db.load(ctx, c.key)
	if err != nil {
		return nil, err
	}
	return data.Counters.rangeValues(uint64(from.Unix()), uint64(to.Unix()), uint64(span.Seconds()), agg), nil
}

// Compact compacts this series.
func (c Counters) Compact(ctx context.Context) error {
	return c.db.compact(ctx, c.key)
}

func (d counterData) values(from, to uint64) iter.Seq2[time.Time, float64] {
	return func(yield func(time.Time, float64) bool) {
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
					return
				}
				sum := uint64(0)
				for bucket < len(d.Buckets) && d.Buckets[bucket].Time == t {
					sum += d.Buckets[bucket].Sum
					bucket++
				}
				for item < len(d.Time) && d.Time[item] == t {
					sum += d.Value[item]
					item++
				}
				if !yield(time.Unix(int64(t), 0), float64(sum)) {
					return
				}
				continue
			}
			t := d.Time[item]
			if t > to {
				return
			}
			sum := uint64(0)
			for item < len(d.Time) && d.Time[item] == t {
				sum += d.Value[item]
				item++
			}
			if !yield(time.Unix(int64(t), 0), float64(sum)) {
				return
			}
		}
	}
}

func (d counterData) rangeValues(from, to, span uint64, agg Agg) iter.Seq2[time.Time, float64] {
	if span == 0 {
		return d.values(from, to)
	}
	if len(d.Buckets) == 0 {
		return func(yield func(time.Time, float64) bool) {
			var current uint64
			var f fold
			flush := func() bool {
				return f.count == 0 || yield(time.Unix(int64(current), 0), f.Value(agg))
			}
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
					if !flush() {
						return
					}
					f = fold{}
				}
				current = k
				f.Add(float64(d.Value[i]))
			}
			flush()
		}
	}
	return func(yield func(time.Time, float64) bool) {
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
				f.Add(float64(d.Buckets[bucket].Sum))
				bucket++
			}
			for item < len(d.Time) && d.Time[item] <= to && bucketOf(d.Time[item], span) == current {
				f.Add(float64(d.Value[item]))
				item++
			}
			if !yield(time.Unix(int64(current), 0), f.Value(agg)) {
				return
			}
		}
	}
}
