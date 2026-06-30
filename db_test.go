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

func TestDB(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithReplica("a"), WithCache(time.Second))
	require.NoError(t, err)
	defer db.Close()

	t0 := time.Unix(10, 0)
	require.NoError(t, db.Samples("cpu").Set(ctx, t0, 1.5))
	require.NoError(t, db.Samples("cpu").Set(ctx, t0.Add(time.Second), 2.5))
	require.NoError(t, db.Counters("req").Add(ctx, t0, 2))
	require.NoError(t, db.Counters("req").Add(ctx, t0, 3))

	got := collect(t, must(db.Samples("cpu").Values(ctx, t0, t0.Add(time.Second))))
	assert.Equal(t, []float64{1.5, 2.5}, got)
	got = collect(t, must(db.Samples("cpu").Range(ctx, t0, t0.Add(time.Second), 10*time.Second, Mean)))
	assert.Equal(t, []float64{2}, got)
	got = collect(t, must(db.Counters("req").Values(ctx, t0, t0)))
	assert.Equal(t, []float64{5}, got)
	got = collect(t, must(db.Counters("req").Range(ctx, t0, t0, time.Second, Sum)))
	assert.Equal(t, []float64{5}, got)
}

func TestBuffer(t *testing.T) {
	ctx := context.Background()
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithFlushEvery(time.Hour))
	require.NoError(t, err)
	defer db.Close()

	at := time.Unix(1, 0)
	require.NoError(t, db.Samples("x").Set(ctx, at, 7))
	assert.Empty(t, store.data)
	assert.Equal(t, []float64{7}, collect(t, must(db.Samples("x").Values(ctx, at, at))))
	require.NoError(t, db.Flush(ctx))
	assert.NotEmpty(t, store.data)

	require.NoError(t, db.writeSample(ctx, "s:z", 1, 1))
	require.NoError(t, db.writeSample(ctx, "s:z", 2, 2))
	require.NoError(t, db.writeCounter(ctx, "c:z", 1, 1))
	require.NoError(t, db.writeCounter(ctx, "c:z", 2, 2))

	t.Run("cache hit", func(t *testing.T) {
		store := keyedMemStore{newMemStore()}
		db, err := New(store, WithCache(time.Second), WithFlushEvery(time.Hour))
		require.NoError(t, err)
		defer db.Close()

		at := time.Unix(5, 0)
		require.NoError(t, db.Counters("c").Add(ctx, at, 2))
		require.NoError(t, db.Samples("s").Set(ctx, at, 1.5))

		got := collect(t, must(db.Counters("c").Values(ctx, at, at)))
		assert.Equal(t, []float64{2}, got)
		got = collect(t, must(db.Samples("s").Values(ctx, at, at)))
		assert.Equal(t, []float64{1.5}, got)

		require.NoError(t, db.Samples("s").Set(ctx, at.Add(time.Second), 2.5))
		got = collect(t, must(db.Samples("s").Values(ctx, at, at.Add(time.Second))))
		assert.Equal(t, []float64{1.5, 2.5}, got)

		require.NoError(t, db.Samples("s").Set(ctx, at.Add(2*time.Second), 3.5))
		got = collect(t, must(db.Samples("s").Values(ctx, at, at.Add(2*time.Second))))
		assert.Equal(t, []float64{1.5, 2.5, 3.5}, got)

		_, err = db.load(ctx, "s")
		require.NoError(t, err)
	})
}

func TestLoops(t *testing.T) {
	store := keyedMemStore{newMemStore()}
	db, err := New(store, WithFlushEvery(time.Millisecond), WithCompactor(time.Millisecond, time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, db.Samples("x").Set(context.Background(), time.Now(), 1))
	time.Sleep(5 * time.Millisecond)
	require.NoError(t, db.Close())
}

func TestLoad(t *testing.T) {
	store := newMemStore()
	store.loadErr = errTest
	db, _ := New(store)
	_, err := db.Samples("x").Values(context.Background(), time.Now(), time.Now())
	assert.ErrorIs(t, err, errTest)
	store.loadErr = nil
	store.data[""] = []byte{99}
	_, err = db.Samples("x").Values(context.Background(), time.Now(), time.Now())
	assert.Error(t, err)

	t.Run("buffer errors", func(t *testing.T) {
		ctx := context.Background()
		store := keyedMemStore{newMemStore()}
		db, err := New(store, WithFlushEvery(time.Hour))
		require.NoError(t, err)
		defer db.Close()

		store.data["s:x"] = sampleSeriesSegment(t, 1, 1, 1, []byte{1})
		require.NoError(t, db.Samples("x").Set(ctx, time.Unix(2, 0), 1))
		_, err = db.load(ctx, "s:x")
		assert.Error(t, err)

		store.data["s:y"] = marshaled(t, codecSeries(1))
		require.NoError(t, db.Samples("y").Set(ctx, time.Unix(2, 0), 1))
		old := loadMarshal
		loadMarshal = func(*pending) ([]byte, error) { return nil, errTest }
		defer func() { loadMarshal = old }()
		_, err = db.load(ctx, "s:y")
		assert.ErrorIs(t, err, errTest)
	})
}

func TestErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("flush", func(t *testing.T) {
		store := keyedMemStore{newMemStore()}
		db, _ := New(store, WithFlushEvery(time.Hour))
		store.data["s:x"] = []byte{99}
		require.NoError(t, db.Flush(ctx))
		require.NoError(t, db.Samples("x").Set(ctx, time.Now(), 1))
		assert.Error(t, db.Flush(ctx))
		require.NoError(t, db.Close())

		db, _ = New(store, WithFlushEvery(time.Hour))
		store.data["s:x"] = []byte{99}
		require.NoError(t, db.Samples("x").Set(ctx, time.Now(), 1))
		assert.Error(t, db.Close())
	})

	t.Run("merge", func(t *testing.T) {
		store := keyedMemStore{newMemStore()}
		db, _ := New(store)
		store.data["s:x"] = []byte{99}
		assert.Error(t, db.Samples("x").Set(ctx, time.Now(), 1))
		assert.Error(t, db.merge(ctx, "s:x", &pending{}))
	})

	t.Run("counter merge", func(t *testing.T) {
		store := keyedMemStore{newMemStore()}
		db, err := New(store)
		require.NoError(t, err)
		store.data["c:x"] = []byte{99}
		assert.Error(t, db.Counters("x").Add(ctx, time.Unix(1, 0), 1))
	})
}
