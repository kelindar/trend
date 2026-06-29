// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"iter"
	"sort"
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
	items := data.Samples.values(uint64(from.Unix()), uint64(to.Unix()))
	return points(items), nil
}

// Range returns bucketed aggregate values.
func (s Samples) Range(ctx context.Context, from, to time.Time, span time.Duration, agg Agg) (iter.Seq2[time.Time, float64], error) {
	data, err := s.db.load(ctx, s.key)
	if err != nil {
		return nil, err
	}
	items := data.Samples.rangeValues(uint64(from.Unix()), uint64(to.Unix()), uint64(span.Seconds()), agg)
	return points(items), nil
}

// Compact compacts this series.
func (s Samples) Compact(ctx context.Context) error {
	return s.db.compact(ctx, s.key)
}

func (d sampleData) values(from, to uint64) []point {
	out := make([]point, 0, len(d.Time)+len(d.Buckets))
	for _, b := range d.Buckets {
		if inWindow(b.Time, from, to) {
			out = append(out, point{
				at:    time.Unix(int64(b.Time), 0),
				value: b.Sum / float64(b.Count),
			})
		}
	}
	raw := sorted.TimeSeries{Time: d.Time, Data: d.Data}
	for i, t := range raw.Time {
		if inWindow(t, from, to) {
			out = append(out, point{
				at:    time.Unix(int64(t), 0),
				value: raw.Data[i],
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].at.Before(out[j].at) })
	return out
}

func (d sampleData) rangeValues(from, to, span uint64, agg Agg) []point {
	if span == 0 {
		return d.values(from, to)
	}
	if len(d.Buckets) == 0 {
		var out []point
		var current uint64
		var f fold
		raw := sorted.TimeSeries{Time: d.Time, Data: d.Data}
		flush := func() {
			if f.count == 0 {
				return
			}
			out = append(out, point{
				at:    time.Unix(int64(current), 0),
				value: f.value(agg),
			})
		}
		for i, t := range raw.Time {
			if !inWindow(t, from, to) {
				continue
			}
			k := bucketOf(t, span)
			if f.count > 0 && k != current {
				flush()
				f = fold{}
			}
			current = k
			f.add(raw.Data[i])
		}
		flush()
		return out
	}
	folds := make(map[uint64]*fold)
	for _, b := range d.Buckets {
		if inWindow(b.Time, from, to) {
			k := bucketOf(b.Time, span)
			if folds[k] == nil {
				folds[k] = &fold{}
			}
			folds[k].merge(b)
		}
	}
	raw := sorted.TimeSeries{Time: d.Time, Data: d.Data}
	for i, t := range raw.Time {
		if inWindow(t, from, to) {
			k := bucketOf(t, span)
			if folds[k] == nil {
				folds[k] = &fold{}
			}
			folds[k].add(raw.Data[i])
		}
	}
	times := sortedTimes(folds)
	out := make([]point, 0, len(times))
	for _, t := range times {
		out = append(out, point{
			at:    time.Unix(int64(t), 0),
			value: folds[t].value(agg),
		})
	}
	return out
}

func inWindow(t, from, to uint64) bool {
	return t >= from && t <= to
}
