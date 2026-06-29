// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package buntdb

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	bunt "github.com/tidwall/buntdb"
)

func TestStore(t *testing.T) {
	ctx := context.Background()
	raw, err := bunt.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	store := New(raw, "p:")
	if err := store.Update(ctx, "k", func(old []byte) ([]byte, error) {
		if old != nil {
			t.Fatal("expected empty old value")
		}
		return []byte("v"), nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Load(ctx, "k"); err != nil || string(got) != "v" {
		t.Fatalf("load: %q %v", got, err)
	}
	if err := store.Update(ctx, "k", func(old []byte) ([]byte, error) {
		if string(old) != "v" {
			t.Fatalf("old: %q", old)
		}
		return []byte("v2"), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "missing"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Load(ctx, "k"); err != nil || got != nil {
		t.Fatalf("missing: %q %v", got, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLease(t *testing.T) {
	ctx := context.Background()
	opened, err := Open(&url.URL{Path: ":memory:", RawQuery: "prefix=p:"})
	if err != nil {
		t.Fatal(err)
	}
	s := opened.(*store)
	release, ok, err := s.Lease(ctx, "lock", time.Minute)
	if err != nil || !ok {
		t.Fatalf("lease: %v %v", ok, err)
	}
	if _, ok, err = s.Lease(ctx, "lock", time.Minute); err != nil || ok {
		t.Fatalf("second lease: %v %v", ok, err)
	}
	if err := release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, err = s.Lease(ctx, "lock", time.Nanosecond); err != nil || !ok {
		t.Fatalf("third lease: %v %v", ok, err)
	}
	time.Sleep(time.Millisecond)
	if _, ok, err = s.Lease(ctx, "lock", time.Minute); err != nil || !ok {
		t.Fatalf("expired lease: %v %v", ok, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	if store, err := Open(&url.URL{}); err != nil {
		t.Fatal(err)
	} else {
		_ = store.Close()
	}
	if store, err := Open(&url.URL{Path: t.TempDir() + "/trend.db"}); err != nil {
		t.Fatal(err)
	} else {
		_ = store.Close()
	}
	if _, err := Open(&url.URL{Path: t.TempDir()}); err == nil {
		t.Fatal("expected open directory error")
	}
}

func TestBackendErrors(t *testing.T) {
	ctx := context.Background()
	raw, err := bunt.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s := New(raw, "").(*store)
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, "x"); err == nil {
		t.Fatal("expected load error")
	}
	if err := s.Update(ctx, "x", func([]byte) ([]byte, error) { return nil, nil }); err == nil {
		t.Fatal("expected update error")
	}
	if err := s.Delete(ctx, "x"); err == nil {
		t.Fatal("expected delete error")
	}
	if _, _, err := s.Lease(ctx, "x", time.Second); err == nil {
		t.Fatal("expected lease error")
	}

	raw, _ = bunt.Open(":memory:")
	s = New(raw, "").(*store)
	errTest := errors.New("merge")
	if err := s.Update(ctx, "x", func([]byte) ([]byte, error) { return nil, errTest }); !errors.Is(err, errTest) {
		t.Fatal(err)
	}
	_ = raw.Close()
}
