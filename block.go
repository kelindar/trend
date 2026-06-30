// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"math"
	"math/bits"
	"time"
)

func appendSampleSegments(dst []byte, data sampleData, buf *codecBuffer) []byte {
	times, values, clocks, replicas := data.Time, data.Data, data.Clock, data.Replica
	for len(times) > 0 {
		n := min(len(times), segmentSize)
		buf.raw = appendSampleRaw(buf.raw[:0], times[:n], values[:n], clocks[:n], replicas[:n])
		from, to := times[0], times[n-1]
		dst = appendSegment(dst, segmentSamples, from, to, n, buf.raw, buf)
		times, values = times[n:], values[n:]
		clocks, replicas = clocks[n:], replicas[n:]
	}
	return dst
}

func appendCounterSegments(dst []byte, data counterData, buf *codecBuffer) []byte {
	times, values, replicas, clocks := data.Time, data.Value, data.Replica, data.Clock
	for len(times) > 0 {
		n := min(len(times), segmentSize)
		buf.raw = appendCounterRaw(buf.raw[:0], times[:n], replicas[:n], clocks[:n], values[:n])
		from, to := times[0], times[n-1]
		dst = appendSegment(dst, segmentCounters, from, to, n, buf.raw, buf)
		times, values = times[n:], values[n:]
		replicas, clocks = replicas[n:], clocks[n:]
	}
	return dst
}

func appendSampleBucketSegments(dst []byte, buckets []sampleBucket, buf *codecBuffer) []byte {
	for len(buckets) > 0 {
		n := min(len(buckets), segmentSize)
		buf.raw = appendSampleBuckets(buf.raw[:0], buckets[:n])
		from, to := buckets[0].Time, buckets[n-1].Time
		dst = appendSegment(dst, segmentSampleBuckets, from, to, n, buf.raw, buf)
		buckets = buckets[n:]
	}
	return dst
}

func appendCounterBucketSegments(dst []byte, buckets []counterBucket, buf *codecBuffer) []byte {
	for len(buckets) > 0 {
		n := min(len(buckets), segmentSize)
		buf.raw = appendCounterBuckets(buf.raw[:0], buckets[:n])
		from, to := buckets[0].Time, buckets[n-1].Time
		dst = appendSegment(dst, segmentCounterBuckets, from, to, n, buf.raw, buf)
		buckets = buckets[n:]
	}
	return dst
}

func appendSampleRaw(dst []byte, timeData []uint64, data []float64, clock, replica []uint64) []byte {
	dst = appendDelta(dst, timeData)
	dst = appendFloatXor(dst, data)
	dst = appendDelta(dst, clock)
	return appendDelta(dst, replica)
}

func appendCounterRaw(dst []byte, timeData, replica, clock, value []uint64) []byte {
	dst = appendDelta(dst, timeData)
	dst = appendDelta(dst, value)
	dst = appendDelta(dst, replica)
	return appendDelta(dst, clock)
}

func appendDelta(dst []byte, data []uint64) []byte {
	var prev uint64
	for _, v := range data {
		dst = appendUvarint(dst, v-prev)
		prev = v
	}
	return dst
}

func appendFloatXor(dst []byte, data []float64) []byte {
	var prev uint64
	for _, v := range data {
		var current uint64
		dst, current = appendFloat(dst, v, prev)
		prev = current
	}
	return dst
}

func appendSampleBuckets(dst []byte, buckets []sampleBucket) []byte {
	var prevTime, prevCount uint64
	var prevSum, prevMin, prevMax, prevFirst, prevLast uint64
	for _, b := range buckets {
		dst = appendUvarint(dst, b.Time-prevTime)
		dst = appendUvarint(dst, b.Count-prevCount)
		dst, prevSum = appendFloat(dst, b.Sum, prevSum)
		dst, prevMin = appendFloat(dst, b.Min, prevMin)
		dst, prevMax = appendFloat(dst, b.Max, prevMax)
		dst, prevFirst = appendFloat(dst, b.First, prevFirst)
		dst, prevLast = appendFloat(dst, b.Last, prevLast)
		prevTime, prevCount = b.Time, b.Count
	}
	return dst
}

func appendCounterBuckets(dst []byte, buckets []counterBucket) []byte {
	var prevTime, prevSum uint64
	for _, b := range buckets {
		dst = appendUvarint(dst, b.Time-prevTime)
		dst = appendUvarint(dst, b.Sum-prevSum)
		prevTime, prevSum = b.Time, b.Sum
	}
	return dst
}

func decodeSamples(raw []byte, n int, out *sampleData) error {
	var data []float64
	timeData := make([]uint64, n)
	data = make([]float64, n)
	clock := make([]uint64, n)
	replica := make([]uint64, n)
	if err := decodeSampleRaw(raw, timeData, data, clock, replica); err != nil {
		return err
	}
	out.Time = append(out.Time, timeData...)
	out.Data = append(out.Data, data...)
	out.Clock = append(out.Clock, clock...)
	out.Replica = append(out.Replica, replica...)
	return nil
}

func decodeCounters(raw []byte, n int, out *counterData) error {
	timeData := make([]uint64, n)
	replica := make([]uint64, n)
	clock := make([]uint64, n)
	value := make([]uint64, n)
	if err := decodeCounterRaw(raw, timeData, replica, clock, value); err != nil {
		return err
	}
	out.Time = append(out.Time, timeData...)
	out.Replica = append(out.Replica, replica...)
	out.Clock = append(out.Clock, clock...)
	out.Value = append(out.Value, value...)
	return nil
}

func decodeSampleRaw(raw []byte, timeData []uint64, data []float64, clock, replica []uint64) error {
	r := codecReader{data: raw}
	r.delta(timeData)
	r.floatXor(data)
	r.delta(clock)
	r.delta(replica)
	return r.done()
}

func decodeSampleValues(raw []byte, timeData []uint64, data []float64) error {
	r := codecReader{data: raw}
	r.delta(timeData)
	r.floatXor(data)
	return r.err
}

func decodeCounterRaw(raw []byte, timeData, replica, clock, value []uint64) error {
	r := codecReader{data: raw}
	r.delta(timeData)
	r.delta(value)
	r.delta(replica)
	r.delta(clock)
	return r.done()
}

func decodeCounterValues(raw []byte, timeData, value []uint64) error {
	r := codecReader{data: raw}
	r.delta(timeData)
	r.delta(value)
	return r.err
}

func decodeSampleBuckets(raw []byte, n int, out *sampleData) error {
	var prevTime, prevCount uint64
	var prevSum, prevMin, prevMax, prevFirst, prevLast uint64
	r := codecReader{data: raw}
	for range n {
		prevTime += r.uvarint()
		prevCount += r.uvarint()
		prevSum ^= r.uvarint()
		prevMin ^= r.uvarint()
		prevMax ^= r.uvarint()
		prevFirst ^= r.uvarint()
		prevLast ^= r.uvarint()
		out.Buckets = append(out.Buckets, sampleBucket{
			Time:  prevTime,
			Count: prevCount,
			Sum:   floatFromReversed(prevSum),
			Min:   floatFromReversed(prevMin),
			Max:   floatFromReversed(prevMax),
			First: floatFromReversed(prevFirst),
			Last:  floatFromReversed(prevLast),
		})
	}
	return r.done()
}

func decodeCounterBuckets(raw []byte, n int, out *counterData) error {
	var prevTime, prevSum uint64
	r := codecReader{data: raw}
	for range n {
		prevTime += r.uvarint()
		prevSum += r.uvarint()
		out.Buckets = append(out.Buckets, counterBucket{Time: prevTime, Sum: prevSum})
	}
	return r.done()
}

func floatFromReversed(v uint64) float64 {
	return math.Float64frombits(bits.Reverse64(v))
}

func (r *codecReader) done() error {
	switch {
	case r.err != nil:
		return r.err
	case len(r.data) != 0:
		return errLongCodec
	default:
		return nil
	}
}

func blockTime(t uint64) time.Time {
	return time.Unix(int64(t), 0)
}
