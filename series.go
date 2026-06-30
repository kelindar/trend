// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"slices"
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

func (s *series) Merge(delta *series) {
	if delta == nil {
		return
	}
	s.Samples.Merge(delta.Samples)
	s.Counters.Merge(delta.Counters)
}

func (s *series) Append(delta *series) {
	if delta == nil {
		return
	}
	s.Samples.Append(delta.Samples)
	s.Counters.Append(delta.Counters)
}

func (s *series) Reset() {
	s.Samples.Reset()
	s.Counters.Reset()
}

func (s *series) Compact(cutoff time.Time, span time.Duration) {
	if span <= 0 {
		return
	}
	s.Samples.Compact(uint64(cutoff.Unix()), uint64(span.Seconds()))
	s.Counters.Compact(uint64(cutoff.Unix()), uint64(span.Seconds()))
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
