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

const (
	version     = 4
	segmentSize = 1024
)

const (
	segmentSamples byte = iota + 1
	segmentCounters
	segmentSampleBuckets
	segmentCounterBuckets
	segmentHistograms
	segmentHistogramBuckets
)

var (
	encodeBuffers = sync.Pool{New: func() any { return new(codecBuffer) }}
)

var (
	errShortCodec  = errors.New("trend: short codec data")
	errLongCodec   = errors.New("trend: trailing codec data")
	errLargeCodec  = errors.New("trend: codec count too large")
	errVarintCodec = errors.New("trend: invalid codec varint")
	errShapeCodec  = errors.New("trend: invalid series shape")
)

type codecBuffer struct {
	raw []byte
	zip []byte
}

type segment struct {
	kind   byte
	from   uint64
	to     uint64
	count  int
	rawLen int
	data   []byte
}

func decode(b []byte) (*pending, error) {
	return series(b).pending()
}

func (p *pending) marshal() ([]byte, error) {
	return series(nil).append(p)
}

func (s series) append(delta *pending) ([]byte, error) {
	if delta == nil || delta.Count() == 0 {
		return append([]byte(nil), s...), s.valid()
	}
	if err := delta.valid(); err != nil {
		return nil, err
	}
	appendable := delta.appendable()
	if len(s) == 0 {
		if !appendable {
			current := pending{}
			current.Merge(delta)
			return current.marshal()
		}
		return delta.appendTo(nil)
	}
	if err := s.valid(); err != nil {
		return nil, err
	}
	if delta.Count() >= segmentSize && appendable && s.canAppend(delta) {
		out := append([]byte(nil), s...)
		return delta.appendTo(out)
	}
	current, err := s.pending()
	if err != nil {
		return nil, err
	}
	current.Merge(delta)
	return current.marshal()
}

func (s series) canAppend(delta *pending) bool {
	next, ok := delta.minTime()
	if !ok {
		return true
	}
	last, ok := s.maxTime()
	return !ok || next > last
}

func (s series) valid() error {
	return s.scan(func(segment) bool { return true })
}

func (s series) versionOK() error {
	switch {
	case len(s) == 0:
		return nil
	case s[0] != version:
		return fmt.Errorf("trend: unsupported version %d", s[0])
	}
	return nil
}

func (s series) maxTime() (uint64, bool) {
	var out uint64
	var ok bool
	_ = s.scan(func(seg segment) bool {
		if !ok || seg.to > out {
			out = seg.to
		}
		ok = true
		return true
	})
	return out, ok
}

func (s series) pending() (*pending, error) {
	var out pending
	var decodeErr error
	err := s.scan(func(seg segment) bool {
		raw, err := seg.decodeInto(nil)
		if err != nil {
			decodeErr = err
			return false
		}
		decodeErr = seg.applyRaw(&out, raw)
		if decodeErr != nil {
			out = pending{}
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if decodeErr != nil {
		return nil, decodeErr
	}
	return &out, nil
}

func (p *pending) appendTo(dst []byte) ([]byte, error) {
	if len(dst) == 0 {
		dst = append(dst, version)
	} else if dst[0] != version {
		return nil, fmt.Errorf("trend: unsupported version %d", dst[0])
	}

	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)

	dst = appendSampleBucketSegments(dst, p.Samples.Buckets, buf)
	dst = appendSampleSegments(dst, p.Samples, buf)
	dst = appendCounterBucketSegments(dst, p.Counters.Buckets, buf)
	dst = appendCounterSegments(dst, p.Counters, buf)
	dst = appendHistogramBucketSegments(dst, p.Histograms.Buckets, buf)
	return appendHistogramSegments(dst, p.Histograms, buf), nil
}

func (p *pending) valid() error {
	if p == nil {
		return nil
	}
	if len(p.Samples.Time) != len(p.Samples.Data) ||
		len(p.Samples.Time) != len(p.Samples.Clock) ||
		len(p.Samples.Time) != len(p.Samples.Replica) ||
		len(p.Counters.Time) != len(p.Counters.Replica) ||
		len(p.Counters.Time) != len(p.Counters.Clock) ||
		len(p.Counters.Time) != len(p.Counters.Value) ||
		len(p.Histograms.Time) != len(p.Histograms.Data) ||
		len(p.Histograms.Time) != len(p.Histograms.Clock) ||
		len(p.Histograms.Time) != len(p.Histograms.Replica) {
		return errShapeCodec
	}
	return nil
}

func (s series) scan(yield func(segment) bool) error {
	switch {
	case len(s) == 0:
		return nil
	case s[0] != version:
		return fmt.Errorf("trend: unsupported version %d", s[0])
	}
	r := codecReader{data: s[1:]}
	for len(r.data) > 0 && r.err == nil {
		seg := segment{
			kind:   r.byte(),
			from:   r.uvarint(),
			to:     r.uvarint(),
			count:  r.count(),
			rawLen: r.count(),
		}
		zipLen := r.count()
		seg.data = r.bytes(zipLen)
		if r.err != nil {
			break
		}
		if seg.count <= 0 || seg.count > segmentSize || seg.from > seg.to {
			return errLargeCodec
		}
		switch seg.kind {
		case segmentSamples, segmentCounters, segmentSampleBuckets, segmentCounterBuckets,
			segmentHistograms, segmentHistogramBuckets:
		default:
			return errVarintCodec
		}
		if !yield(seg) {
			return nil
		}
	}
	return r.err
}

func (seg segment) applyRaw(out *pending, raw []byte) error {
	switch seg.kind {
	case segmentSamples:
		return decodeSamples(raw, seg.count, &out.Samples)
	case segmentCounters:
		return decodeCounters(raw, seg.count, &out.Counters)
	case segmentSampleBuckets:
		return decodeSampleBuckets(raw, seg.count, &out.Samples)
	case segmentCounterBuckets:
		return decodeCounterBuckets(raw, seg.count, &out.Counters)
	case segmentHistograms:
		return decodeHistograms(raw, seg.count, &out.Histograms)
	case segmentHistogramBuckets:
		return decodeHistogramBuckets(raw, seg.count, &out.Histograms)
	default:
		return errVarintCodec
	}
}

func (seg segment) decodeInto(dst []byte) ([]byte, error) {
	raw, err := s2.Decode(dst[:0], seg.data)
	if err != nil {
		return nil, err
	}
	if len(raw) != seg.rawLen {
		return nil, errShortCodec
	}
	return raw, nil
}

func appendSegment(dst []byte, kind byte, from, to uint64, count int, raw []byte, buf *codecBuffer) []byte {
	buf.zip = s2.Encode(buf.zip[:0], raw)
	dst = append(dst, kind)
	dst = appendUvarint(dst, from)
	dst = appendUvarint(dst, to)
	dst = appendUvarint(dst, uint64(count))
	dst = appendUvarint(dst, uint64(len(raw)))
	dst = appendUvarint(dst, uint64(len(buf.zip)))
	return append(dst, buf.zip...)
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

func (r *codecReader) byte() byte {
	if r.err != nil {
		return 0
	}
	if len(r.data) == 0 {
		r.err = errShortCodec
		return 0
	}
	out := r.data[0]
	r.data = r.data[1:]
	return out
}

func (r *codecReader) bytes(n int) []byte {
	if n < 0 || n > len(r.data) {
		r.err = errShortCodec
		return nil
	}
	out := r.data[:n:n]
	r.data = r.data[n:]
	return out
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
