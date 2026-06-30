// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/kelindar/trend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	store, err := New(db, "p:")
	require.NoError(t, err)
	require.NoError(t, store.Update(ctx, "k", func(old []byte) ([]byte, error) {
		assert.Nil(t, old)
		return []byte("v"), nil
	}))
	got, err := store.Load(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", string(got))
	require.NoError(t, store.Update(ctx, "k", func(old []byte) ([]byte, error) {
		assert.Equal(t, "v", string(old))
		return []byte("v2"), nil
	}))
	assert.NoError(t, store.Delete(ctx, "missing"))
	assert.NoError(t, store.Delete(ctx, "k"))
	got, err = store.Load(ctx, "k")
	require.NoError(t, err)
	assert.Nil(t, got)
	assert.NoError(t, store.Close())
	assert.NoError(t, db.Close())
}

func TestLease(t *testing.T) {
	ctx := context.Background()
	opened, err := Open(&url.URL{Path: ":memory:", RawQuery: "prefix=p:"})
	require.NoError(t, err)
	s := opened.(*store)
	release, ok, err := s.Lease(ctx, "lock", time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)
	_, ok, err = s.Lease(ctx, "lock", time.Minute)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.NoError(t, release(ctx))
	assert.NoError(t, release(ctx))
	_, ok, err = s.Lease(ctx, "lock", time.Nanosecond)
	require.NoError(t, err)
	assert.True(t, ok)
	time.Sleep(time.Millisecond)
	_, ok, err = s.Lease(ctx, "lock", time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.NoError(t, s.Close())
}

func TestOpen(t *testing.T) {
	tests := []*url.URL{
		{},
		{Path: ":memory:"},
		{Path: "/:memory:"},
		{Scheme: "sqlite", Opaque: filepath.Join(t.TempDir(), "opaque.db")},
		{Path: filepath.Join(t.TempDir(), "trend.db")},
	}
	for _, u := range tests {
		t.Run(u.String(), func(t *testing.T) {
			store, err := Open(u)
			require.NoError(t, err)
			assert.NoError(t, store.Close())
		})
	}
	_, err := Open(&url.URL{Path: t.TempDir()})
	assert.Error(t, err)
}

func TestRegister(t *testing.T) {
	db, err := trend.Open("sqlite:///:memory:?prefix=p:")
	require.NoError(t, err)
	assert.NoError(t, db.Close())
}

func TestBackendErrors(t *testing.T) {
	ctx := context.Background()
	raw, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	s, err := New(raw, "")
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	_, err = s.Load(ctx, "x")
	assert.Error(t, err)
	assert.Error(t, s.Update(ctx, "x", func([]byte) ([]byte, error) { return nil, nil }))
	assert.Error(t, s.Delete(ctx, "x"))
	_, _, err = s.(interface {
		Lease(context.Context, string, time.Duration) (func(context.Context) error, bool, error)
	}).Lease(ctx, "x", time.Second)
	assert.Error(t, err)

	raw, err = sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	s, err = New(raw, "")
	require.NoError(t, err)
	errTest := errors.New("merge")
	assert.ErrorIs(t, s.Update(ctx, "x", func([]byte) ([]byte, error) { return nil, errTest }), errTest)
	_ = raw.Close()
}
