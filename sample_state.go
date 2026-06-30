// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

type samplePoint struct {
	value   float64
	clock   uint64
	replica uint64
}

func (d *sampleData) Add(t uint64, value float64, clock, replica uint64) {
	d.Time = append(d.Time, t)
	d.Data = append(d.Data, value)
	d.Clock = append(d.Clock, clock)
	d.Replica = append(d.Replica, replica)
}

func (d *sampleData) Reset() {
	d.Time = d.Time[:0]
	d.Data = d.Data[:0]
	d.Clock = d.Clock[:0]
	d.Replica = d.Replica[:0]
	d.Buckets = d.Buckets[:0]
}

func (d sampleData) count() int {
	return len(d.Time) + len(d.Buckets)
}

func (d sampleData) appendable() bool {
	return strictlyIncreasing(d.Time) && sampleBucketsIncreasing(d.Buckets)
}

func (d sampleData) minTime() (uint64, bool) {
	var out uint64
	var ok bool
	for _, t := range d.Time {
		if !ok || t < out {
			out = t
		}
		ok = true
	}
	for _, b := range d.Buckets {
		if !ok || b.Time < out {
			out = b.Time
		}
		ok = true
	}
	return out, ok
}

func sampleBucketsIncreasing(buckets []sampleBucket) bool {
	for i := 1; i < len(buckets); i++ {
		if buckets[i].Time <= buckets[i-1].Time {
			return false
		}
	}
	return true
}

func (d *sampleData) Merge(delta sampleData) {
	points := make(map[uint64]samplePoint, len(d.Time)+len(delta.Time))
	for i, t := range d.Time {
		points[t] = samplePoint{value: d.Data[i], clock: d.Clock[i], replica: d.Replica[i]}
	}
	for i, t := range delta.Time {
		next := samplePoint{value: delta.Data[i], clock: delta.Clock[i], replica: delta.Replica[i]}
		if old, ok := points[t]; !ok || newerSample(next, old) {
			points[t] = next
		}
	}
	times := sortedTimes(points)
	d.Time = d.Time[:0]
	d.Data = d.Data[:0]
	d.Clock = d.Clock[:0]
	d.Replica = d.Replica[:0]
	for _, t := range times {
		p := points[t]
		d.Add(t, p.value, p.clock, p.replica)
	}
	d.Buckets = mergeSampleBuckets(d.Buckets, delta.Buckets)
}

func newerSample(a, b samplePoint) bool {
	return a.clock > b.clock || a.clock == b.clock && a.replica > b.replica
}

func mergeSampleBuckets(a, b []sampleBucket) []sampleBucket {
	buckets := make(map[uint64]sampleBucket, len(a)+len(b))
	for _, x := range a {
		buckets[x.Time] = x
	}
	for _, x := range b {
		if old, ok := buckets[x.Time]; ok {
			buckets[x.Time] = combineSampleBucket(old, x)
		} else {
			buckets[x.Time] = x
		}
	}
	times := sortedTimes(buckets)
	out := make([]sampleBucket, 0, len(times))
	for _, t := range times {
		out = append(out, buckets[t])
	}
	return out
}

func combineSampleBucket(a, b sampleBucket) sampleBucket {
	if a.Count == 0 {
		return b
	}
	if b.Count == 0 {
		return a
	}
	a.Count += b.Count
	a.Sum += b.Sum
	if b.Min < a.Min {
		a.Min = b.Min
	}
	if b.Max > a.Max {
		a.Max = b.Max
	}
	a.Last = b.Last
	return a
}

func (d *sampleData) Compact(cutoff, span uint64) {
	if len(d.Time) == 0 {
		return
	}
	buckets := make(map[uint64]sampleBucket, len(d.Buckets))
	for _, b := range d.Buckets {
		buckets[b.Time] = b
	}
	keep := 0
	for i, t := range d.Time {
		if t >= cutoff {
			d.Time[keep] = d.Time[i]
			d.Data[keep] = d.Data[i]
			d.Clock[keep] = d.Clock[i]
			d.Replica[keep] = d.Replica[i]
			keep++
			continue
		}
		bt := bucketOf(t, span)
		b := buckets[bt]
		v := d.Data[i]
		if b.Count == 0 {
			b.Time, b.Min, b.Max, b.First = bt, v, v, v
		}
		b.Count++
		b.Sum += v
		if v < b.Min {
			b.Min = v
		}
		if v > b.Max {
			b.Max = v
		}
		b.Last = v
		buckets[bt] = b
	}
	d.Time = d.Time[:keep]
	d.Data = d.Data[:keep]
	d.Clock = d.Clock[:keep]
	d.Replica = d.Replica[:keep]
	times := sortedTimes(buckets)
	d.Buckets = d.Buckets[:0]
	for _, t := range times {
		d.Buckets = append(d.Buckets, buckets[t])
	}
}
