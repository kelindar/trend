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
)

func TestStore(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	store := New(client, "p:")
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
	if err := store.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Load(ctx, "k"); err != nil || got != nil {
		t.Fatalf("missing: %q %v", got, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLease(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	opened, err := Open(&url.URL{Scheme: "redis", Host: server.Addr(), RawQuery: "prefix=p:"})
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
	if _, ok, err = s.Lease(ctx, "lock", time.Minute); err != nil || !ok {
		t.Fatalf("third lease: %v %v", ok, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestErrors(t *testing.T) {
	if _, err := Open(&url.URL{Scheme: "http", Host: "localhost"}); err == nil {
		t.Fatal("expected open error")
	}

	ctx := context.Background()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	store := New(client, "")
	errTest := errors.New("merge")
	if err := store.Update(ctx, "x", func([]byte) ([]byte, error) { return nil, errTest }); !errors.Is(err, errTest) {
		t.Fatal(err)
	}
	if err := client.LPush(ctx, "bad", "x").Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(ctx, "bad", func([]byte) ([]byte, error) { return []byte("x"), nil }); err == nil {
		t.Fatal("expected wrong type get error")
	}
	server.Close()
	if err := store.Update(ctx, "x", func([]byte) ([]byte, error) { return []byte("x"), nil }); err == nil {
		t.Fatal("expected update get error")
	}
	_ = client.Close()
}
