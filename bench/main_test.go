// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kelindar/bench"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCases(t *testing.T) {
	cases := cases()
	names := make([]string, 0, len(cases))
	for _, tc := range cases {
		names = append(names, tc.name)
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()(1)
			assert.Equal(t, tc.name, tc.String())
		})
	}
	assert.Equal(t, []string{
		"samples/append",
		"samples/range",
		"samples/values",
		"counters/append",
		"counters/range",
		"counters/values",
	}, names)
}

func TestRun(t *testing.T) {
	run(bench.WithDryRun(), bench.WithSamples(1), bench.WithDuration(time.Millisecond))
}

func TestMain(t *testing.T) {
	old := mainOptions
	mainOptions = []bench.Option{bench.WithDryRun(), bench.WithSamples(1), bench.WithDuration(time.Millisecond)}
	defer func() { mainOptions = old }()
	main()
}

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore()
	require.NoError(t, store.Update(ctx, "x", func([]byte) ([]byte, error) {
		return []byte("ok"), nil
	}))
	got, err := store.Load(ctx, "x")
	require.NoError(t, err)
	assert.Equal(t, "ok", string(got))
	require.NoError(t, store.Delete(ctx, "x"))
	got, err = store.Load(ctx, "x")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestMustPanics(t *testing.T) {
	defer func() {
		assert.NotNil(t, recover())
	}()
	must(errors.New("boom"))
}
