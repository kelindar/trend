// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"errors"
	"iter"
	"net/url"
	"sync"
	"testing"
	"time"
)

type memStore struct {
	mu       sync.Mutex
	data     map[string][]byte
	closed   bool
	loadErr  error
	leaseErr error
	leaseOK  bool
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string][]byte), leaseOK: true}
}

func (m *memStore) Load(context.Context, string) ([]byte, error) {
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[""], nil
}

func (m *memStore) Update(_ context.Context, key string, merge func([]byte) ([]byte, error)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next, err := merge(m.data[key])
	if err != nil {
		return err
	}
	m.data[key] = append([]byte(nil), next...)
	return nil
}

func (m *memStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *memStore) Close() error {
	m.closed = true
	return nil
}

func (m *memStore) Lease(context.Context, string, time.Duration) (func(context.Context) error, bool, error) {
	return func(context.Context) error { return nil }, m.leaseOK, m.leaseErr
}

type keyedMemStore struct{ *memStore }

func (m keyedMemStore) Load(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[key], nil
}

func must(it iter.Seq2[time.Time, float64], err error) iter.Seq2[time.Time, float64] {
	if err != nil {
		panic(err)
	}
	return it
}

func collect(t *testing.T, it iter.Seq2[time.Time, float64]) []float64 {
	t.Helper()
	var out []float64
	it(func(_ time.Time, value float64) bool {
		out = append(out, value)
		return true
	})
	return out
}

func collectCall(t *testing.T, call func(func(time.Time, float64) bool)) []float64 {
	t.Helper()
	var out []float64
	call(func(_ time.Time, value float64) bool {
		out = append(out, value)
		return true
	})
	return out
}

func TestStore(t *testing.T) {
	Register("mem", func(*url.URL) (Store, error) {
		return keyedMemStore{newMemStore()}, nil
	})
	if _, err := Open("%zz"); err == nil {
		t.Fatal("expected bad URI error")
	}
	if _, err := Open("missing://x"); err == nil {
		t.Fatal("expected missing store error")
	}
	db, err := Open("mem://x", WithReplica("r1"), WithCache(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if db.replica != hashReplica("r1") {
		t.Fatal("replica not set")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

var errTest = errors.New("test")
