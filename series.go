// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"slices"
	"time"
)

type series []byte

type pending struct {
	Samples    sampleData
	Counters   counterData
	Histograms histogramData
}

type sampleData struct {
	Time    []uint64
	Data    []float64
	Clock   []uint64
	Replica []uint64
	Buckets []sampleBucket
}

type sampleBucket struct {
	Time  uint64
	Count uint64
	Sum   float64
	Min   float64
	Max   float64
	First float64
	Last  float64
}

type counterData struct {
	Time    []uint64
	Replica []uint64
	Clock   []uint64
	Value   []uint64
	Buckets []counterBucket
}

type counterBucket struct {
	Time uint64
	Sum  uint64
}

func (p *pending) Merge(delta *pending) {
	if delta == nil {
		return
	}
	p.Samples.Merge(delta.Samples)
	p.Counters.Merge(delta.Counters)
	p.Histograms.Merge(delta.Histograms)
}

func (p *pending) Reset() {
	p.Samples.Reset()
	p.Counters.Reset()
	p.Histograms.Reset()
}

func (p *pending) Count() int {
	if p == nil {
		return 0
	}
	return p.Samples.count() + p.Counters.count() + p.Histograms.count()
}

func (p *pending) appendable() bool {
	if p == nil {
		return true
	}
	return p.Samples.appendable() && p.Counters.appendable() && p.Histograms.appendable()
}

func (p *pending) minTime() (uint64, bool) {
	out, ok := p.Samples.minTime()
	if v, has := p.Counters.minTime(); has && (!ok || v < out) {
		out, ok = v, true
	}
	if v, has := p.Histograms.minTime(); has && (!ok || v < out) {
		out, ok = v, true
	}
	return out, ok
}

func (p *pending) Compact(cutoff time.Time, span time.Duration) {
	if span <= 0 {
		return
	}
	p.Samples.Compact(uint64(cutoff.Unix()), uint64(span.Seconds()))
	p.Counters.Compact(uint64(cutoff.Unix()), uint64(span.Seconds()))
	p.Histograms.Compact(uint64(cutoff.Unix()), uint64(span.Seconds()))
}

func bucketOf(t, span uint64) uint64 {
	if span == 0 {
		return t
	}
	return (t / span) * span
}

func sortedTimes[V any](m map[uint64]V) []uint64 {
	times := make([]uint64, 0, len(m))
	for t := range m {
		times = append(times, t)
	}
	slices.Sort(times)
	return times
}

func strictlyIncreasing(data []uint64) bool {
	for i := 1; i < len(data); i++ {
		if data[i] <= data[i-1] {
			return false
		}
	}
	return true
}

func nondecreasing(data []uint64) bool {
	for i := 1; i < len(data); i++ {
		if data[i] < data[i-1] {
			return false
		}
	}
	return true
}
