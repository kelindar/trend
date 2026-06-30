// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

type counterOp struct {
	replica uint64
	clock   uint64
	value   uint64
}

func (d *counterData) Add(t, replica, clock, value uint64) {
	d.Time = append(d.Time, t)
	d.Replica = append(d.Replica, replica)
	d.Clock = append(d.Clock, clock)
	d.Value = append(d.Value, value)
}

func (d *counterData) Append(delta counterData) {
	d.Time = append(d.Time, delta.Time...)
	d.Replica = append(d.Replica, delta.Replica...)
	d.Clock = append(d.Clock, delta.Clock...)
	d.Value = append(d.Value, delta.Value...)
	d.Buckets = append(d.Buckets, delta.Buckets...)
}

func (d *counterData) Merge(delta counterData) {
	ops := make(map[uint64]map[counterOp]struct{}, len(d.Time)+len(delta.Time))
	add := func(t, replica, clock, value uint64) {
		if ops[t] == nil {
			ops[t] = make(map[counterOp]struct{})
		}
		ops[t][counterOp{replica: replica, clock: clock, value: value}] = struct{}{}
	}
	for i, t := range d.Time {
		add(t, d.Replica[i], d.Clock[i], d.Value[i])
	}
	for i, t := range delta.Time {
		add(t, delta.Replica[i], delta.Clock[i], delta.Value[i])
	}
	times := sortedTimes(ops)
	d.Time = d.Time[:0]
	d.Replica = d.Replica[:0]
	d.Clock = d.Clock[:0]
	d.Value = d.Value[:0]
	for _, t := range times {
		for op := range ops[t] {
			d.Add(t, op.replica, op.clock, op.value)
		}
	}
	d.Buckets = mergeCounterBuckets(d.Buckets, delta.Buckets)
}

func mergeCounterBuckets(a, b []counterBucket) []counterBucket {
	buckets := make(map[uint64]uint64, len(a)+len(b))
	for _, x := range a {
		buckets[x.Time] += x.Sum
	}
	for _, x := range b {
		buckets[x.Time] += x.Sum
	}
	times := sortedTimes(buckets)
	out := make([]counterBucket, 0, len(times))
	for _, t := range times {
		out = append(out, counterBucket{Time: t, Sum: buckets[t]})
	}
	return out
}

func (d *counterData) Compact(cutoff, span uint64) {
	buckets := make(map[uint64]uint64, len(d.Buckets))
	for _, b := range d.Buckets {
		buckets[b.Time] += b.Sum
	}
	keep := 0
	for i, t := range d.Time {
		if t >= cutoff {
			d.Time[keep] = d.Time[i]
			d.Replica[keep] = d.Replica[i]
			d.Clock[keep] = d.Clock[i]
			d.Value[keep] = d.Value[i]
			keep++
			continue
		}
		buckets[bucketOf(t, span)] += d.Value[i]
	}
	d.Time = d.Time[:keep]
	d.Replica = d.Replica[:keep]
	d.Clock = d.Clock[:keep]
	d.Value = d.Value[:keep]
	times := sortedTimes(buckets)
	d.Buckets = d.Buckets[:0]
	for _, t := range times {
		d.Buckets = append(d.Buckets, counterBucket{Time: t, Sum: buckets[t]})
	}
}
