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

func TestCompact(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithCompaction(time.Hour, time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	old := time.Now().Add(-2 * time.Hour).Truncate(time.Minute)
	if err := db.Samples("x").Set(ctx, old, 2); err != nil {
		t.Fatal(err)
	}
	if err := db.Samples("x").Set(ctx, old.Add(time.Second), 4); err != nil {
		t.Fatal(err)
	}
	if err := db.Counters("x").Add(ctx, old, 3); err != nil {
		t.Fatal(err)
	}
	if err := db.Samples("x").Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Counters("x").Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if got := collect(t, must(db.Samples("x").Values(ctx, old, old.Add(time.Minute)))); !reflect.DeepEqual(got, []float64{3}) {
		t.Fatalf("compacted samples: %v", got)
	}
	if got := collect(t, must(db.Counters("x").Values(ctx, old, old.Add(time.Minute)))); !reflect.DeepEqual(got, []float64{3}) {
		t.Fatalf("compacted counters: %v", got)
	}
}

func TestLease(t *testing.T) {
	db, _ := New(keyedMemStore{&memStore{data: make(map[string][]byte), leaseOK: false}})
	if err := db.compact(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	store := keyedMemStore{&memStore{data: make(map[string][]byte), leaseOK: true, leaseErr: errTest}}
	db, _ = New(store)
	if !errors.Is(db.compact(context.Background(), "x"), errTest) {
		t.Fatal("expected lease error")
	}
}

func TestCompactErrors(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, _ := New(store)
	store.data["s:x"] = []byte{99}
	db.compactor.after = 0
	if err := db.compact(ctx, "s:x"); err != nil {
		t.Fatal(err)
	}
	db.compactor.after = time.Hour
	if err := db.compact(ctx, "s:x"); err == nil {
		t.Fatal("expected compact decode error")
	}
}
