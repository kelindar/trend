// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package cached

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memStore struct {
	data    map[string][]byte
	loads   int
	closed  bool
	loadErr error
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string][]byte)}
}

func (m *memStore) Load(context.Context, string) ([]byte, error) {
	m.loads++
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	return clone(m.data["k"]), nil
}

func (m *memStore) Update(_ context.Context, _ string, merge func([]byte) ([]byte, error)) error {
	next, err := merge(clone(m.data["k"]))
	if err != nil {
		return err
	}
	m.data["k"] = clone(next)
	return nil
}

func (m *memStore) Delete(context.Context, string) error {
	delete(m.data, "k")
	return nil
}

func (m *memStore) Close() error {
	m.closed = true
	return nil
}

func TestStore(t *testing.T) {
	ctx := context.Background()
	primary := newMemStore()
	cache := newMemStore()
	primary.data["k"] = []byte("v")
	store := New(primary, cache)

	got, err := store.Load(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", string(got))
	got, err = store.Load(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", string(got))
	assert.Equal(t, 1, primary.loads)

	require.NoError(t, store.Update(ctx, "k", func([]byte) ([]byte, error) {
		return []byte("v2"), nil
	}))
	got, err = store.Load(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v2", string(got))

	require.NoError(t, store.Delete(ctx, "k"))
	got, err = store.Load(ctx, "k")
	require.NoError(t, err)
	assert.Nil(t, got)
	require.NoError(t, store.Close())
	assert.True(t, primary.closed)
	assert.True(t, cache.closed)
}

func TestErrors(t *testing.T) {
	ctx := context.Background()
	errTest := errors.New("test")
	primary := newMemStore()
	primary.loadErr = errTest
	store := New(primary, newMemStore())
	_, err := store.Load(ctx, "k")
	assert.ErrorIs(t, err, errTest)
	assert.ErrorIs(t, store.Update(ctx, "k", func([]byte) ([]byte, error) {
		return nil, errTest
	}), errTest)
}

func TestLease(t *testing.T) {
	ctx := context.Background()
	wrapped := New(newMemStore(), newMemStore())
	s, ok := wrapped.(*store)
	require.True(t, ok)
	release, locked, err := s.Lease(ctx, "k", 0)
	require.NoError(t, err)
	assert.False(t, locked)
	require.NoError(t, release(ctx))

	primary := &leaseStore{memStore: *newMemStore()}
	wrapped = New(primary, newMemStore())
	s, ok = wrapped.(*store)
	require.True(t, ok)
	release, locked, err = s.Lease(ctx, "k", time.Second)
	require.NoError(t, err)
	assert.True(t, locked)
	require.NoError(t, release(ctx))
}

type leaseStore struct {
	memStore
}

func (m *leaseStore) Lease(context.Context, string, time.Duration) (func(context.Context) error, bool, error) {
	return func(context.Context) error { return nil }, true, nil
}

func clone(v []byte) []byte {
	if v == nil {
		return nil
	}
	return append([]byte(nil), v...)
}
