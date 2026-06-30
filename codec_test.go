// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"math"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodec(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		empty, err := decode(nil)
		require.NoError(t, err)
		assert.Equal(t, &pending{}, empty)
	})
	t.Run("errors", func(t *testing.T) {
		_, err := decode([]byte{version + 1})
		assert.Error(t, err)
		_, err = decode([]byte{version, 0xff})
		assert.Error(t, err)
	})

	in := codecSeries(128)
	raw, err := in.marshal()
	require.NoError(t, err)
	assert.Equal(t, byte(version), raw[0])
	out, err := decode(raw)
	require.NoError(t, err)
	assertSeriesEqual(t, in, out)
	raw[0]++
	_, err = decode(raw)
	assert.Error(t, err)

	t.Run("rejects corrupt blocks", func(t *testing.T) {
		in := codecSeries(segmentSize)
		raw, err := in.marshal()
		require.NoError(t, err)
		require.NotEmpty(t, raw)
		raw = raw[:len(raw)-1]
		_, err = decode(raw)
		assert.Error(t, err)
	})

	t.Run("append edge cases", func(t *testing.T) {
		out, err := series(nil).append(nil)
		require.NoError(t, err)
		assert.Empty(t, out)

		var empty pending
		out, err = series(nil).append(&empty)
		require.NoError(t, err)
		assert.Empty(t, out)

		var delta pending
		delta.Samples.Add(2, 1, 1, 1)
		delta.Samples.Add(1, 2, 2, 1)
		_, err = series(nil).append(&delta)
		require.NoError(t, err)

		_, err = series([]byte{version, 0xff}).append(codecSeries(1))
		assert.Error(t, err)

		var p pending
		p.Samples.Add(1, 1, 1, 1)
		_, err = p.appendTo([]byte{version + 1})
		assert.Error(t, err)
	})

	t.Run("pending roundtrip", func(t *testing.T) {
		var p pending
		p.Samples.Add(1, 1, 1, 1)
		p.Counters.Add(1, 1, 1, 1)
		p.Samples.Buckets = []sampleBucket{{Time: 10, Count: 1, Sum: 1, Min: 1, Max: 1, First: 1, Last: 1}}
		p.Counters.Buckets = []counterBucket{{Time: 10, Sum: 1}}
		raw := marshaled(t, &p)
		got, err := raw.pending()
		require.NoError(t, err)
		assertSeriesEqual(t, &p, got)
	})

	t.Run("scan rejects", func(t *testing.T) {
		assert.Error(t, series([]byte{version + 1}).valid())
		assert.Error(t, series([]byte{version, 99, 1, 1, 1, 1, 1, 1}).valid())
		assert.Error(t, series([]byte{version, segmentSamples, 2, 1, 1, 1, 1, 1, 1}).valid())
	})

	t.Run("decode into", func(t *testing.T) {
		var p pending
		p.Samples.Add(1, 1, 1, 1)
		raw := marshaled(t, &p)
		var seg segment
		require.NoError(t, raw.scan(func(s segment) bool {
			seg = s
			return false
		}))
		seg.data = []byte{0xff}
		_, err := seg.decodeInto(nil)
		assert.Error(t, err)
	})

	t.Run("reader", func(t *testing.T) {
		r := codecReader{data: []byte{}}
		assert.Equal(t, byte(0), r.byte())
		assert.Error(t, r.err)

		r = codecReader{data: []byte{1}}
		assert.Nil(t, r.bytes(2))
		assert.Error(t, r.err)

		r = codecReader{data: []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02}}
		_ = r.uvarint()
		assert.ErrorIs(t, r.err, errVarintCodec)

		r = codecReader{data: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}}
		assert.Equal(t, 0, r.count())
		assert.ErrorIs(t, r.err, errLargeCodec)

		r = codecReader{data: append(bytesRepeat(0x80, 9), 0x02)}
		_ = r.uvarint()
		assert.ErrorIs(t, r.err, errVarintCodec)

		_ = r.byte()
		assert.Equal(t, byte(0), r.byte())
		assert.Error(t, r.err)

		r = codecReader{err: errShortCodec}
		assert.Equal(t, uint64(0), r.uvarint())
	})

	t.Run("append fast path", func(t *testing.T) {
		var base pending
		for i := range uint64(segmentSize) {
			base.Samples.Add(i+1, float64(i), i+1, 1)
		}
		raw, err := base.marshal()
		require.NoError(t, err)

		var delta pending
		for i := range uint64(segmentSize) {
			delta.Samples.Add(segmentSize+i+1, float64(i), segmentSize+i+1, 1)
		}
		out, err := series(raw).append(&delta)
		require.NoError(t, err)
		got, err := decode(out)
		require.NoError(t, err)
		assert.Len(t, got.Samples.Time, segmentSize*2)
	})

	t.Run("append blocked", func(t *testing.T) {
		var base pending
		base.Samples.Add(10, 1, 1, 1)
		raw, err := base.marshal()
		require.NoError(t, err)

		var delta pending
		delta.Samples.Add(5, 2, 2, 1)
		out, err := series(raw).append(&delta)
		require.NoError(t, err)
		got, err := decode(out)
		require.NoError(t, err)
		assert.Equal(t, 2.0, got.Samples.Data[0])
	})

	t.Run("pending valid", func(t *testing.T) {
		assert.NoError(t, (*pending)(nil).valid())
		assert.Error(t, (&pending{Samples: sampleData{Time: []uint64{1}}}).valid())
	})

	t.Run("pending decode errors", func(t *testing.T) {
		s := sampleSeriesSegment(t, 1, 1, 1, []byte{1})
		_, err := s.pending()
		assert.Error(t, err)

		s = counterSeriesSegment(t, 1, 1, 1, []byte{1})
		_, err = s.pending()
		assert.Error(t, err)

		s = sampleBucketSeriesSegment(t, 1, 1, 1, []byte{1})
		_, err = s.pending()
		assert.Error(t, err)

		s = counterBucketSeriesSegment(t, 1, 1, 1, []byte{1})
		_, err = s.pending()
		assert.Error(t, err)
	})

	t.Run("decode into length", func(t *testing.T) {
		payload := appendSampleRaw(nil, []uint64{1}, []float64{1}, []uint64{1}, []uint64{1})
		zip := s2Encode(t, payload)
		seg := segment{data: zip, rawLen: len(payload) + 1}
		_, err := seg.decodeInto(nil)
		assert.ErrorIs(t, err, errShortCodec)
	})

	t.Run("append pending error", func(t *testing.T) {
		_, err := series(sampleSeriesSegment(t, 1, 1, 1, []byte{1})).append(codecSeries(1))
		assert.Error(t, err)
	})

	t.Run("can append empty delta", func(t *testing.T) {
		assert.True(t, series(nil).canAppend(&pending{}))
	})

	t.Run("can append empty base", func(t *testing.T) {
		var delta pending
		delta.Samples.Add(1, 1, 1, 1)
		assert.True(t, series(nil).canAppend(&delta))
	})

	t.Run("apply raw unknown kind", func(t *testing.T) {
		assert.ErrorIs(t, (segment{kind: 99}).applyRaw(&pending{}, nil), errVarintCodec)
	})

	t.Run("pending decode into error", func(t *testing.T) {
		_, err := garbageZipSeriesSegment(t, segmentSamples, 1, 1, 1, 1, 4).pending()
		assert.Error(t, err)
	})
}

func TestCodecErrors(t *testing.T) {
	raw := []byte{version, segmentSamples, 1, 1}
	_, err := decode(raw)
	assert.Error(t, err)
}

func TestCodecPreservesFloat64(t *testing.T) {
	var in pending
	values := []float64{
		math.SmallestNonzeroFloat64,
		math.Pi,
		-math.MaxFloat64,
		1.0 / 3.0,
	}
	for i, v := range values {
		in.Samples.Add(uint64(10+i), v, uint64(i+1), 9)
	}
	in.Samples.Buckets = []sampleBucket{{
		Time:  1,
		Count: 4,
		Sum:   math.Pi,
		Min:   math.SmallestNonzeroFloat64,
		Max:   math.MaxFloat64,
		First: values[0],
		Last:  values[len(values)-1],
	}}
	in.Counters.Add(10, 9, 1, 3)
	in.Counters.Buckets = []counterBucket{{Time: 1, Sum: 3}}

	raw, err := in.marshal()
	require.NoError(t, err)
	out, err := decode(raw)
	require.NoError(t, err)
	assertSeriesEqual(t, &in, out)
	for i, want := range values {
		assert.Equal(t, math.Float64bits(want), math.Float64bits(out.Samples.Data[i]))
	}
	assert.Equal(t, math.Float64bits(math.SmallestNonzeroFloat64), math.Float64bits(out.Samples.Buckets[0].Min))
}

func TestCodecSparseSides(t *testing.T) {
	tests := []*pending{
		{},
		{Samples: sampleData{
			Time:    []uint64{1},
			Data:    []float64{1},
			Clock:   []uint64{1},
			Replica: []uint64{1},
		}},
		{Counters: counterData{
			Time:    []uint64{1},
			Replica: []uint64{1},
			Clock:   []uint64{1},
			Value:   []uint64{1},
		}},
	}
	for _, in := range tests {
		raw, err := in.marshal()
		require.NoError(t, err)
		out, err := decode(raw)
		require.NoError(t, err)
		assertSeriesEqual(t, in, out)
	}
}

func TestCodecShape(t *testing.T) {
	_, err := (&pending{
		Samples: sampleData{
			Time: []uint64{1},
		},
	}).marshal()
	assert.Error(t, err)
}

func codecSeries(n int) *pending {
	var s pending
	for i := range uint64(n) {
		s.Samples.Add(i, float64(i), i+1, 1)
		s.Counters.Add(i, 1, i+1, i+1)
	}
	return &s
}

func assertSeriesEqual(t *testing.T, want, got *pending) {
	t.Helper()
	assert.Equal(t, want, got)
}

func marshaled(t *testing.T, p *pending) series {
	t.Helper()
	raw, err := p.marshal()
	require.NoError(t, err)
	return series(raw)
}

func invalidZipSeriesSegment(t *testing.T, kind byte, from, to uint64, count int, payload []byte) series {
	t.Helper()
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	header := appendSegment(nil, kind, from, to, count, payload, buf)
	for i := len(header) - 4; i < len(header); i++ {
		header[i] = 0xff
	}
	return series(append([]byte{version}, header...))
}

func garbageZipSeriesSegment(t *testing.T, kind byte, from, to uint64, count, rawLen, zipLen int) series {
	t.Helper()
	dst := []byte{kind}
	dst = appendUvarint(dst, from)
	dst = appendUvarint(dst, to)
	dst = appendUvarint(dst, uint64(count))
	dst = appendUvarint(dst, uint64(rawLen))
	dst = appendUvarint(dst, uint64(zipLen))
	dst = append(dst, bytesRepeat(0xff, zipLen)...)
	return series(append([]byte{version}, dst...))
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func sampleSeriesSegment(t *testing.T, from, to uint64, count int, payload []byte) series {
	t.Helper()
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	return series(append([]byte{version}, appendSegment(nil, segmentSamples, from, to, count, payload, buf)...))
}

func counterSeriesSegment(t *testing.T, from, to uint64, count int, payload []byte) series {
	t.Helper()
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	return series(append([]byte{version}, appendSegment(nil, segmentCounters, from, to, count, payload, buf)...))
}

func sampleBucketSeriesSegment(t *testing.T, from, to uint64, count int, payload []byte) series {
	t.Helper()
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	return series(append([]byte{version}, appendSegment(nil, segmentSampleBuckets, from, to, count, payload, buf)...))
}

func counterBucketSeriesSegment(t *testing.T, from, to uint64, count int, payload []byte) series {
	t.Helper()
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	return series(append([]byte{version}, appendSegment(nil, segmentCounterBuckets, from, to, count, payload, buf)...))
}

func s2Encode(t *testing.T, raw []byte) []byte {
	t.Helper()
	buf := encodeBuffers.Get().(*codecBuffer)
	defer putEncodeBuffer(buf)
	return s2.Encode(buf.zip[:0], raw)
}
