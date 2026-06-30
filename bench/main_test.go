// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kelindar/bench"
)

func TestCases(t *testing.T) {
	cases := cases()
	if len(cases) != 6 {
		t.Fatalf("cases: %d", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()(1)
			if tc.String() != tc.name {
				t.Fatalf("string: %q", tc.String())
			}
		})
	}
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
	if err := store.Update(ctx, "x", func([]byte) ([]byte, error) {
		return []byte("ok"), nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "ok" {
		t.Fatalf("load: %q", got)
	}
	if err := store.Delete(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	got, err = store.Load(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("delete: %q", got)
	}
}

func TestMustPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	must(errors.New("boom"))
}
