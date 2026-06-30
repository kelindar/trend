// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCounters(t *testing.T) {
	var s series
	s.Counters.Add(1, 1, 1, 2)
	s.Counters.Buckets = []counterBucket{{Time: 10, Sum: 3}}
	if got := s.Counters.rangeValues(1, 10, 0, Sum); len(got) != 2 {
		t.Fatalf("zero counter span: %v", got)
	}
	if got := s.Counters.rangeValues(1, 10, 10, Count); len(got) != 2 {
		t.Fatalf("counter range: %v", got)
	}

	var raw series
	raw.Counters.Add(1, 1, 1, 1)
	raw.Counters.Add(61, 1, 2, 2)
	if got := raw.Counters.rangeValues(2, 120, 60, Sum); len(got) != 1 || got[0].value != 2 {
		t.Fatalf("raw counter range: %v", got)
	}
	if got := raw.Counters.rangeValues(200, 300, 60, Sum); len(got) != 0 {
		t.Fatalf("empty raw counter range: %v", got)
	}
	if got := raw.Counters.rangeValues(1, 120, 60, Sum); len(got) != 2 {
		t.Fatalf("raw counter buckets: %v", got)
	}
}

func TestCounterState(t *testing.T) {
	counters := counterData{
		Time:    []uint64{10, 20},
		Replica: []uint64{1, 1},
		Clock:   []uint64{1, 2},
		Value:   []uint64{1, 2},
		Buckets: []counterBucket{{Time: 1, Sum: 1}},
	}
	counters.Compact(15, 10)
	if len(counters.Time) != 1 || len(counters.Buckets) != 2 {
		t.Fatalf("counter compact: %+v", counters)
	}
	if got := mergeCounterBuckets([]counterBucket{{Time: 1, Sum: 1}}, []counterBucket{{Time: 2, Sum: 2}}); len(got) != 2 {
		t.Fatalf("counter bucket merge: %v", got)
	}
}

func TestCounterErrors(t *testing.T) {
	store := newMemStore()
	store.loadErr = errTest
	db, _ := New(store)
	now := time.Now()
	if _, err := db.Counters("x").Values(context.Background(), now, now); !errors.Is(err, errTest) {
		t.Fatal("expected counter values error")
	}
	if _, err := db.Counters("x").Range(context.Background(), now, now, time.Second, Sum); !errors.Is(err, errTest) {
		t.Fatal("expected counter range error")
	}
}
