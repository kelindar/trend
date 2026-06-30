// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

// Package memory stores trend data in an in-memory cache.
package memory

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/kelindar/trend"
)

const defaultTTL = 24 * time.Hour

type store struct {
	mu sync.Mutex
	db *bigcache.BigCache
}

func init() {
	trend.Register("memory", Open)
}

// Open opens an in-memory store. The optional ttl query sets entry lifetime.
func Open(u *url.URL) (trend.Store, error) {
	ttl := defaultTTL
	if v := u.Query().Get("ttl"); v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return nil, err
		}
		ttl = parsed
	}
	return New(ttl)
}

// New creates an in-memory store.
func New(ttl time.Duration) (trend.Store, error) {
	if ttl <= 0 {
		ttl = defaultTTL
	}
	db, err := bigcache.New(context.Background(), bigcache.DefaultConfig(ttl))
	if err != nil {
		return nil, err
	}
	return &store{db: db}, nil
}

func (s *store) Load(_ context.Context, key string) ([]byte, error) {
	out, err := s.db.Get(key)
	if err == bigcache.ErrEntryNotFound {
		return nil, nil
	}
	return clone(out), err
}

func (s *store) Update(_ context.Context, key string, merge func([]byte) ([]byte, error)) error {
	// ponytail: global lock keeps Update atomic; shard it if write throughput matters.
	s.mu.Lock()
	defer s.mu.Unlock()

	old, err := s.Load(context.Background(), key)
	if err != nil {
		return err
	}
	next, err := merge(old)
	if err != nil {
		return err
	}
	return s.db.Set(key, clone(next))
}

func (s *store) Delete(_ context.Context, key string) error {
	_ = s.db.Delete(key)
	return nil
}

func (s *store) Close() error {
	return s.db.Close()
}

func clone(v []byte) []byte {
	if v == nil {
		return nil
	}
	return append([]byte(nil), v...)
}
