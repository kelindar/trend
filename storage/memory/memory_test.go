// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package memory

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/kelindar/trend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	ctx := context.Background()
	store, err := New(time.Minute)
	require.NoError(t, err)
	require.NoError(t, store.Update(ctx, "k", func(old []byte) ([]byte, error) {
		assert.Nil(t, old)
		return []byte("v"), nil
	}))
	got, err := store.Load(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", string(got))
	got[0] = 'x'
	got, err = store.Load(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", string(got))
	require.NoError(t, store.Delete(ctx, "k"))
	got, err = store.Load(ctx, "k")
	require.NoError(t, err)
	assert.Nil(t, got)
	assert.NoError(t, store.Close())
}

func TestOpen(t *testing.T) {
	store, err := Open(&url.URL{RawQuery: "ttl=1m"})
	require.NoError(t, err)
	assert.NoError(t, store.Close())
	_, err = Open(&url.URL{RawQuery: "ttl=nope"})
	assert.Error(t, err)
}

func TestRegister(t *testing.T) {
	db, err := trend.Open("memory://")
	require.NoError(t, err)
	assert.NoError(t, db.Close())
}

func TestUpdateError(t *testing.T) {
	store, err := New(0)
	require.NoError(t, err)
	errTest := errors.New("merge")
	assert.ErrorIs(t, store.Update(context.Background(), "x", func([]byte) ([]byte, error) {
		return nil, errTest
	}), errTest)
}
