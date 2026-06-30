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
		assert.Equal(t, &series{}, empty)
	})
	t.Run("errors", func(t *testing.T) {
		_, err := decode([]byte{version + 1})
		assert.Error(t, err)
		_, err = decode([]byte{version, 0xff})
		assert.Error(t, err)
	})

	in := codecSeries(128)
	raw, err := in.Marshal()
	require.NoError(t, err)
	assert.Equal(t, byte(version), raw[0])
	out, err := decode(raw)
	require.NoError(t, err)
	assert.Equal(t, in, out)
	raw[0]++
	_, err = decode(raw)
	assert.Error(t, err)
}

func TestCodecErrors(t *testing.T) {
	raw := append([]byte{version}, s2.Encode(nil, []byte{1, 2, 3})...)
	_, err := decode(raw)
	assert.Error(t, err)
}

func TestCodecPreservesFloat64(t *testing.T) {
	var in series
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

	raw, err := in.Marshal()
	require.NoError(t, err)
	out, err := decode(raw)
	require.NoError(t, err)
	assert.Equal(t, &in, out)
	for i, want := range values {
		assert.Equal(t, math.Float64bits(want), math.Float64bits(out.Samples.Data[i]))
	}
	assert.Equal(t, math.Float64bits(math.SmallestNonzeroFloat64), math.Float64bits(out.Samples.Buckets[0].Min))
}

func TestCodecSparseSides(t *testing.T) {
	tests := []*series{
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
		raw, err := in.Marshal()
		require.NoError(t, err)
		out, err := decode(raw)
		require.NoError(t, err)
		assert.Equal(t, in, out)
	}
}

func TestCodecShape(t *testing.T) {
	_, err := (&series{
		Samples: sampleData{
			Time: []uint64{1},
		},
	}).Marshal()
	assert.Error(t, err)
}

func codecSeries(n int) *series {
	var s series
	for i := range uint64(n) {
		s.Samples.Add(i, float64(i), i+1, 1)
		s.Counters.Add(i, 1, i+1, i+1)
	}
	return &s
}
