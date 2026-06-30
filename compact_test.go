// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompact(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithCompaction(time.Hour, time.Minute))
	require.NoError(t, err)
	defer db.Close()

	old := time.Now().Add(-2 * time.Hour).Truncate(time.Minute)
	require.NoError(t, db.Samples("x").Set(ctx, old, 2))
	require.NoError(t, db.Samples("x").Set(ctx, old.Add(time.Second), 4))
	require.NoError(t, db.Counters("x").Add(ctx, old, 3))
	require.NoError(t, db.Samples("x").Compact(ctx))
	require.NoError(t, db.Counters("x").Compact(ctx))
	assert.Equal(t, []float64{3}, collect(t, must(db.Samples("x").Values(ctx, old, old.Add(time.Minute)))))
	assert.Equal(t, []float64{3}, collect(t, must(db.Counters("x").Values(ctx, old, old.Add(time.Minute)))))
}

func TestLease(t *testing.T) {
	db, _ := New(keyedMemStore{&memStore{data: make(map[string][]byte), leaseOK: false}})
	assert.NoError(t, db.compact(context.Background(), "x"))

	store := keyedMemStore{&memStore{data: make(map[string][]byte), leaseOK: true, leaseErr: errTest}}
	db, _ = New(store)
	assert.ErrorIs(t, db.compact(context.Background(), "x"), errTest)
}

func TestCompactErrors(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, _ := New(store)
	store.data["s:x"] = []byte{99}
	db.compactor.after = 0
	assert.NoError(t, db.compact(ctx, "s:x"))
	db.compactor.after = time.Hour
	assert.Error(t, db.compact(ctx, "s:x"))
}
