// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"math"
	"reflect"
	"testing"

	"github.com/klauspost/compress/s2"
)

func TestCodec(t *testing.T) {
	empty, err := decode(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(empty, &series{}) {
		t.Fatalf("empty decode: %+v", empty)
	}
	if _, err := decode([]byte{version + 1}); err == nil {
		t.Fatal("expected version error")
	}
	if _, err := decode([]byte{version, 0xff}); err == nil {
		t.Fatal("expected s2 decode error")
	}

	in := codecSeries(128)
	raw, err := in.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if raw[0] != version {
		t.Fatalf("version byte: %d", raw[0])
	}
	out, err := decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatal("round trip mismatch")
	}
	raw[0]++
	if _, err := decode(raw); err == nil {
		t.Fatal("expected mutated version error")
	}
}

func TestCodecErrors(t *testing.T) {
	raw := append([]byte{version}, s2.Encode(nil, []byte{1, 2, 3})...)
	if _, err := decode(raw); err == nil {
		t.Fatalf("expected decode error, got %v", err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	out, err := decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, &in) {
		t.Fatal("round trip mismatch")
	}
	for i, want := range values {
		if got := out.Samples.Data[i]; math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("sample %d bits: got %x want %x", i, math.Float64bits(got), math.Float64bits(want))
		}
	}
	if got := out.Samples.Buckets[0].Min; math.Float64bits(got) != math.Float64bits(math.SmallestNonzeroFloat64) {
		t.Fatalf("bucket min bits: got %x", math.Float64bits(got))
	}
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
		if err != nil {
			t.Fatal(err)
		}
		out, err := decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(out, in) {
			t.Fatalf("round trip mismatch: %#v", out)
		}
	}
}

func TestCodecRejectsInvalidShape(t *testing.T) {
	_, err := (&series{
		Samples: sampleData{
			Time: []uint64{1},
		},
	}).Marshal()
	if err == nil {
		t.Fatal("expected invalid shape error")
	}
}

func BenchmarkCodec(b *testing.B) {
	input := codecSeries(10_000)
	encoded, err := input.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	b.Run("marshal_10k", func(b *testing.B) {
		b.ReportAllocs()
		var out []byte
		for b.Loop() {
			var err error
			out, err = input.Marshal()
			if err != nil {
				b.Fatal(err)
			}
			if len(out) == 0 {
				b.Fatal("empty marshal")
			}
		}
		b.ReportMetric(float64(len(out)), "encoded_bytes/op")
	})
	b.Run("decode_10k", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			out, err := decode(encoded)
			if err != nil {
				b.Fatal(err)
			}
			if len(out.Samples.Time) != 10_000 || len(out.Counters.Time) != 10_000 {
				b.Fatal("bad decode")
			}
		}
		b.ReportMetric(float64(len(encoded)), "encoded_bytes/op")
	})
}

func codecSeries(n int) *series {
	var s series
	for i := range uint64(n) {
		s.Samples.Add(i, float64(i), i+1, 1)
		s.Counters.Add(i, 1, i+1, i+1)
	}
	return &s
}
