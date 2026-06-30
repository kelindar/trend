// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

// Package cached adds a read-through cache to a store.
package cached

import (
	"context"
	"time"

	"github.com/kelindar/trend"
)

type store struct {
	primary trend.Store
	cache   trend.Store
}

// New wraps primary with cache.
func New(primary, cache trend.Store) trend.Store {
	return &store{primary: primary, cache: cache}
}

func (s *store) Load(ctx context.Context, key string) ([]byte, error) {
	if s.cache != nil {
		if out, err := s.cache.Load(ctx, key); err != nil || out != nil {
			return out, err
		}
	}
	out, err := s.primary.Load(ctx, key)
	if err == nil && out != nil && s.cache != nil {
		_ = s.cache.Update(ctx, key, func([]byte) ([]byte, error) { return out, nil })
	}
	return out, err
}

func (s *store) Update(ctx context.Context, key string, merge func([]byte) ([]byte, error)) error {
	err := s.primary.Update(ctx, key, merge)
	if err == nil && s.cache != nil {
		_ = s.cache.Delete(ctx, key)
	}
	return err
}

func (s *store) Delete(ctx context.Context, key string) error {
	if s.cache != nil {
		_ = s.cache.Delete(ctx, key)
	}
	return s.primary.Delete(ctx, key)
}

func (s *store) Lease(ctx context.Context, key string, ttl time.Duration) (func(context.Context) error, bool, error) {
	leaser, ok := s.primary.(interface {
		Lease(context.Context, string, time.Duration) (func(context.Context) error, bool, error)
	})
	if !ok {
		return func(context.Context) error { return nil }, false, nil
	}
	return leaser.Lease(ctx, key, ttl)
}

func (s *store) Close() error {
	if s.cache != nil {
		_ = s.cache.Close()
	}
	return s.primary.Close()
}
