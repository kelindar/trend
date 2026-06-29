// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"iter"
	"sort"
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
	items := data.Counters.values(uint64(from.Unix()), uint64(to.Unix()))
	return points(items), nil
}

// Range returns bucketed aggregate values.
func (c Counters) Range(ctx context.Context, from, to time.Time, span time.Duration, agg Agg) (iter.Seq2[time.Time, float64], error) {
	data, err := c.db.load(ctx, c.key)
	if err != nil {
		return nil, err
	}
	items := data.Counters.rangeValues(uint64(from.Unix()), uint64(to.Unix()), uint64(span.Seconds()), agg)
	return points(items), nil
}

// Compact compacts this series.
func (c Counters) Compact(ctx context.Context) error {
	return c.db.compact(ctx, c.key)
}

func (d counterData) values(from, to uint64) []point {
	sums := make(map[uint64]uint64, len(d.Time)+len(d.Buckets))
	for _, b := range d.Buckets {
		if inWindow(b.Time, from, to) {
			sums[b.Time] += b.Sum
		}
	}
	for i, t := range d.Time {
		if inWindow(t, from, to) {
			sums[t] += d.Value[i]
		}
	}
	times := sortedTimes(sums)
	out := make([]point, 0, len(times))
	for _, t := range times {
		out = append(out, point{
			at:    time.Unix(int64(t), 0),
			value: float64(sums[t]),
		})
	}
	return out
}

func (d counterData) rangeValues(from, to, span uint64, agg Agg) []point {
	if span == 0 {
		return d.values(from, to)
	}
	if len(d.Buckets) == 0 {
		var out []point
		var current uint64
		var f fold
		flush := func() {
			if f.count == 0 {
				return
			}
			out = append(out, point{
				at:    time.Unix(int64(current), 0),
				value: f.value(agg),
			})
		}
		for i, t := range d.Time {
			if !inWindow(t, from, to) {
				continue
			}
			k := bucketOf(t, span)
			if f.count > 0 && k != current {
				flush()
				f = fold{}
			}
			current = k
			f.add(float64(d.Value[i]))
		}
		flush()
		return out
	}
	folds := make(map[uint64]*fold)
	add := func(t uint64, v float64) {
		k := bucketOf(t, span)
		if folds[k] == nil {
			folds[k] = &fold{}
		}
		folds[k].add(v)
	}
	for _, b := range d.Buckets {
		if inWindow(b.Time, from, to) {
			add(b.Time, float64(b.Sum))
		}
	}
	raw := d.values(from, to)
	for _, item := range raw {
		add(uint64(item.at.Unix()), item.value)
	}
	times := sortedTimes(folds)
	out := make([]point, 0, len(times))
	for _, t := range times {
		out = append(out, point{
			at:    time.Unix(int64(t), 0),
			value: folds[t].value(agg),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].at.Before(out[j].at) })
	return out
}
