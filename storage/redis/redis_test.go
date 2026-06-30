// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package redis

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	store := New(client, "p:")
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
	assert.NoError(t, store.Delete(ctx, "k"))
	got, err = store.Load(ctx, "k")
	require.NoError(t, err)
	assert.Nil(t, got)
	assert.NoError(t, store.Close())
	assert.NoError(t, client.Close())
}

func TestLease(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	opened, err := Open(&url.URL{Scheme: "redis", Host: server.Addr(), RawQuery: "prefix=p:"})
	require.NoError(t, err)
	s := opened.(*store)
	release, ok, err := s.Lease(ctx, "lock", time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)
	_, ok, err = s.Lease(ctx, "lock", time.Minute)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.NoError(t, release(ctx))
	_, ok, err = s.Lease(ctx, "lock", time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.NoError(t, s.Close())
}

func TestErrors(t *testing.T) {
	_, err := Open(&url.URL{Scheme: "http", Host: "localhost"})
	assert.Error(t, err)

	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	store := New(client, "")
	errTest := errors.New("merge")
	assert.ErrorIs(t, store.Update(ctx, "x", func([]byte) ([]byte, error) { return nil, errTest }), errTest)
	require.NoError(t, client.LPush(ctx, "bad", "x").Err())
	assert.Error(t, store.Update(ctx, "bad", func([]byte) ([]byte, error) { return []byte("x"), nil }))
	server.Close()
	assert.Error(t, store.Update(ctx, "x", func([]byte) ([]byte, error) { return []byte("x"), nil }))
	_ = client.Close()
}
