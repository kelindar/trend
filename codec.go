// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sync"

	"github.com/klauspost/compress/s2"
)

const version = 2

var (
	encodeBuffers = sync.Pool{New: func() any { return new(codecBuffer) }}
	decodeBuffers = sync.Pool{New: func() any { return new(codecBuffer) }}
)

var (
	errShortCodec  = errors.New("trend: short codec data")
	errLongCodec   = errors.New("trend: trailing codec data")
	errLargeCodec  = errors.New("trend: codec count too large")
	errVarintCodec = errors.New("trend: invalid codec varint")
	errShapeCodec  = errors.New("trend: invalid series shape")
)

type codecBuffer struct {
	data []byte
}

func decode(b []byte) (*series, error) {
	switch {
	case len(b) == 0:
		return &series{}, nil
	case b[0] != version:
		return nil, fmt.Errorf("trend: unsupported version %d", b[0])
	}

	size, err := s2.DecodedLen(b[1:])
	if err != nil {
		return nil, err
	}
	scratch := getDecodeBuffer(size)
	defer putDecodeBuffer(scratch)

	out, err := s2.Decode(scratch.data[:0], b[1:])
	if err != nil {
		return nil, err
	}
	return decodeSeries(out)
}

func (s *series) Marshal() ([]byte, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}

	buffer := encodeBuffers.Get().(*codecBuffer)
	buffer.data = appendSeries(buffer.data[:0], s)
	encoded := buffer.data
	dst := make([]byte, s2.MaxEncodedLen(len(encoded))+1)
	dst[0] = version
	out := s2.Encode(dst[1:], encoded)
	putEncodeBuffer(buffer)
	return dst[:len(out)+1], nil
}

func (s *series) valid() error {
	if len(s.Samples.Time) != len(s.Samples.Data) ||
		len(s.Samples.Time) != len(s.Samples.Clock) ||
		len(s.Samples.Time) != len(s.Samples.Replica) ||
		len(s.Counters.Time) != len(s.Counters.Replica) ||
		len(s.Counters.Time) != len(s.Counters.Clock) ||
		len(s.Counters.Time) != len(s.Counters.Value) {
		return errShapeCodec
	}
	return nil
}

func (s *series) alloc(samples, sampleBuckets, counters, counterBuckets int) {
	if samples+counters > 0 {
		values := make([]uint64, samples*3+counters*4)
		offset := 0
		if samples > 0 {
			s.Samples.Time = values[:samples:samples]
			s.Samples.Clock = values[samples : 2*samples : 2*samples]
			s.Samples.Replica = values[2*samples : 3*samples : 3*samples]
			offset = 3 * samples
		}
		if counters > 0 {
			s.Counters.Time = values[offset : offset+counters : offset+counters]
			offset += counters
			s.Counters.Replica = values[offset : offset+counters : offset+counters]
			offset += counters
			s.Counters.Clock = values[offset : offset+counters : offset+counters]
			offset += counters
			s.Counters.Value = values[offset : offset+counters : offset+counters]
		}
	}
	if samples > 0 {
		s.Samples.Data = make([]float64, samples)
	}
	if sampleBuckets > 0 {
		s.Samples.Buckets = make([]sampleBucket, sampleBuckets)
	}
	if counterBuckets > 0 {
		s.Counters.Buckets = make([]counterBucket, counterBuckets)
	}
}

func appendSeries(dst []byte, s *series) []byte {
	dst = appendUvarint(dst, uint64(len(s.Samples.Time)))
	dst = appendUvarint(dst, uint64(len(s.Samples.Buckets)))
	dst = appendUvarint(dst, uint64(len(s.Counters.Time)))
	dst = appendUvarint(dst, uint64(len(s.Counters.Buckets)))
	dst = appendDelta(dst, s.Samples.Time)
	dst = appendFloatXor(dst, s.Samples.Data)
	dst = appendDelta(dst, s.Samples.Clock)
	dst = appendDelta(dst, s.Samples.Replica)
	dst = appendSampleBuckets(dst, s.Samples.Buckets)
	dst = appendDelta(dst, s.Counters.Time)
	dst = appendDelta(dst, s.Counters.Replica)
	dst = appendDelta(dst, s.Counters.Clock)
	dst = appendDelta(dst, s.Counters.Value)
	return appendCounterBuckets(dst, s.Counters.Buckets)
}

func decodeSeries(data []byte) (*series, error) {
	r := codecReader{data: data}
	samples := r.count()
	sampleBuckets := r.count()
	counters := r.count()
	counterBuckets := r.count()
	if r.err != nil {
		return nil, r.err
	}
	fields, ok := minFields(samples, sampleBuckets, counters, counterBuckets)
	if !ok {
		return nil, errLargeCodec
	}
	if !r.hasFields(fields) {
		return nil, r.err
	}

	out := new(series)
	out.alloc(samples, sampleBuckets, counters, counterBuckets)
	r.delta(out.Samples.Time)
	r.floatXor(out.Samples.Data)
	r.delta(out.Samples.Clock)
	r.delta(out.Samples.Replica)
	r.sampleBuckets(out.Samples.Buckets)
	r.delta(out.Counters.Time)
	r.delta(out.Counters.Replica)
	r.delta(out.Counters.Clock)
	r.delta(out.Counters.Value)
	r.counterBuckets(out.Counters.Buckets)
	if r.err != nil {
		return nil, r.err
	}
	if len(r.data) != 0 {
		return nil, errLongCodec
	}
	return out, nil
}

func minFields(samples, sampleBuckets, counters, counterBuckets int) (int, bool) {
	total := 0
	for _, v := range [...]struct {
		count  int
		fields int
	}{
		{samples, 4},
		{sampleBuckets, 7},
		{counters, 4},
		{counterBuckets, 2},
	} {
		if v.count > (int(^uint(0)>>1)-total)/v.fields {
			return 0, false
		}
		total += v.count * v.fields
	}
	return total, true
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

func appendFloat(dst []byte, v float64, prev uint64) ([]byte, uint64) {
	current := bits.Reverse64(math.Float64bits(v))
	return appendUvarint(dst, current^prev), current
}

func appendUvarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

type codecReader struct {
	data []byte
	err  error
}

func (r *codecReader) hasFields(n int) bool {
	if n < 0 || n > len(r.data) {
		r.err = errShortCodec
		return false
	}
	return true
}

func (r *codecReader) count() int {
	v := r.uvarint()
	if r.err != nil {
		return 0
	}
	if v > uint64(int(^uint(0)>>1)) {
		r.err = errLargeCodec
		return 0
	}
	return int(v)
}

func (r *codecReader) delta(dst []uint64) {
	var prev uint64
	for i := range dst {
		prev += r.uvarint()
		dst[i] = prev
	}
}

func (r *codecReader) floatXor(dst []float64) {
	var prev uint64
	for i := range dst {
		prev ^= r.uvarint()
		dst[i] = math.Float64frombits(bits.Reverse64(prev))
	}
}

func (r *codecReader) sampleBuckets(dst []sampleBucket) {
	var prevTime, prevCount uint64
	var prevSum, prevMin, prevMax, prevFirst, prevLast uint64
	for i := range dst {
		b := &dst[i]
		prevTime += r.uvarint()
		prevCount += r.uvarint()
		prevSum ^= r.uvarint()
		prevMin ^= r.uvarint()
		prevMax ^= r.uvarint()
		prevFirst ^= r.uvarint()
		prevLast ^= r.uvarint()
		b.Time = prevTime
		b.Count = prevCount
		b.Sum = math.Float64frombits(bits.Reverse64(prevSum))
		b.Min = math.Float64frombits(bits.Reverse64(prevMin))
		b.Max = math.Float64frombits(bits.Reverse64(prevMax))
		b.First = math.Float64frombits(bits.Reverse64(prevFirst))
		b.Last = math.Float64frombits(bits.Reverse64(prevLast))
	}
}

func (r *codecReader) counterBuckets(dst []counterBucket) {
	var prevTime, prevSum uint64
	for i := range dst {
		prevTime += r.uvarint()
		prevSum += r.uvarint()
		dst[i] = counterBucket{Time: prevTime, Sum: prevSum}
	}
}

func (r *codecReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	var x uint64
	for shift := uint(0); shift < 64; shift += 7 {
		if len(r.data) == 0 {
			r.err = errShortCodec
			return 0
		}
		b := r.data[0]
		r.data = r.data[1:]
		if b < 0x80 {
			if shift == 63 && b > 1 {
				r.err = errVarintCodec
				return 0
			}
			return x | uint64(b)<<shift
		}
		x |= uint64(b&0x7f) << shift
	}
	r.err = errVarintCodec
	return 0
}

func putEncodeBuffer(buf *codecBuffer) {
	encodeBuffers.Put(buf)
}

func getDecodeBuffer(size int) *codecBuffer {
	buf := decodeBuffers.Get().(*codecBuffer)
	if cap(buf.data) < size {
		buf.data = make([]byte, size)
	}
	buf.data = buf.data[:size]
	return buf
}

func putDecodeBuffer(buf *codecBuffer) {
	decodeBuffers.Put(buf)
}
