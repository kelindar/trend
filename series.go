// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"sort"
	"time"
)

type series struct {
	Samples  sampleData
	Counters counterData
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

func (s *series) merge(delta *series) {
	if delta == nil {
		return
	}
	s.Samples.merge(delta.Samples)
	s.Counters.merge(delta.Counters)
}

func (s *series) append(delta *series) {
	if delta == nil {
		return
	}
	s.Samples.append(delta.Samples)
	s.Counters.append(delta.Counters)
}

func (s *series) compact(cutoff time.Time, span time.Duration) {
	if span <= 0 {
		return
	}
	s.Samples.compact(uint64(cutoff.Unix()), uint64(span.Seconds()))
	s.Counters.compact(uint64(cutoff.Unix()), uint64(span.Seconds()))
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
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	return times
}
