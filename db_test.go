// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestDB(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithReplica("a"), WithCache(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t0 := time.Unix(10, 0)
	if err := db.Samples("cpu").Set(ctx, t0, 1.5); err != nil {
		t.Fatal(err)
	}
	if err := db.Samples("cpu").Set(ctx, t0.Add(time.Second), 2.5); err != nil {
		t.Fatal(err)
	}
	if err := db.Counters("req").Add(ctx, t0, 2); err != nil {
		t.Fatal(err)
	}
	if err := db.Counters("req").Add(ctx, t0, 3); err != nil {
		t.Fatal(err)
	}

	got := collect(t, must(db.Samples("cpu").Values(ctx, t0, t0.Add(time.Second))))
	if !reflect.DeepEqual(got, []float64{1.5, 2.5}) {
		t.Fatalf("samples: %v", got)
	}
	got = collect(t, must(db.Samples("cpu").Range(ctx, t0, t0.Add(time.Second), 10*time.Second, Mean)))
	if !reflect.DeepEqual(got, []float64{2}) {
		t.Fatalf("sample range: %v", got)
	}
	got = collect(t, must(db.Counters("req").Values(ctx, t0, t0)))
	if !reflect.DeepEqual(got, []float64{5}) {
		t.Fatalf("counter values: %v", got)
	}
	got = collect(t, must(db.Counters("req").Range(ctx, t0, t0, time.Second, Sum)))
	if !reflect.DeepEqual(got, []float64{5}) {
		t.Fatalf("counter range: %v", got)
	}
}

func TestBuffer(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithFlushEvery(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	at := time.Unix(1, 0)
	if err := db.Samples("x").Set(ctx, at, 7); err != nil {
		t.Fatal(err)
	}
	if len(store.data) != 0 {
		t.Fatal("write should be buffered")
	}
	if got := collect(t, must(db.Samples("x").Values(ctx, at, at))); !reflect.DeepEqual(got, []float64{7}) {
		t.Fatalf("pending read: %v", got)
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(store.data) == 0 {
		t.Fatal("flush did not write")
	}

	var delta series
	delta.Samples.add(1, 1, 1, 1)
	if err := db.write(ctx, "s:y", &delta); err != nil {
		t.Fatal(err)
	}
	if err := db.write(ctx, "s:y", &delta); err != nil {
		t.Fatal(err)
	}
	if err := db.writeSample(ctx, "s:z", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.writeSample(ctx, "s:z", 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := db.writeCounter(ctx, "c:z", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.writeCounter(ctx, "c:z", 2, 2); err != nil {
		t.Fatal(err)
	}
}

func TestLoops(t *testing.T) {
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithFlushEvery(time.Millisecond), WithCompactor(time.Millisecond, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Samples("x").Set(context.Background(), time.Now(), 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLoad(t *testing.T) {
	store := newMemStore()
	store.loadErr = errTest
	db, _ := New(store)
	if _, err := db.Samples("x").Values(context.Background(), time.Now(), time.Now()); !errors.Is(err, errTest) {
		t.Fatal("expected load error")
	}
	store.loadErr = nil
	store.data[""] = []byte{99}
	if _, err := db.Samples("x").Values(context.Background(), time.Now(), time.Now()); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("flush", func(t *testing.T) {
		store := keyedMemStore{newMemStore()}
		db, _ := New(store, WithFlushEvery(time.Hour))
		store.data["s:x"] = []byte{99}
		if err := db.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if err := db.Samples("x").Set(ctx, time.Now(), 1); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(ctx); err == nil {
			t.Fatal("expected flush merge error")
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		db, _ = New(store, WithFlushEvery(time.Hour))
		store.data["s:x"] = []byte{99}
		if err := db.Samples("x").Set(ctx, time.Now(), 1); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err == nil {
			t.Fatal("expected close flush error")
		}
	})

	t.Run("merge", func(t *testing.T) {
		store := keyedMemStore{newMemStore()}
		db, _ := New(store)
		store.data["s:x"] = []byte{99}
		if err := db.Samples("x").Set(ctx, time.Now(), 1); err == nil {
			t.Fatal("expected write merge error")
		}
		if err := db.merge(ctx, "s:x", &series{}); err == nil {
			t.Fatal("expected merge error")
		}
	})
}
