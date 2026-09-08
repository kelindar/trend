// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

// Package cached adds a read-through cache to a store.
package cached

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/kelindar/trend"
)

// DefaultTTL is the default lifetime for cached entries.
const DefaultTTL = time.Hour

// Config controls a local read-through cache.
type Config struct {
	Enabled bool          // Whether reads use the cache.
	TTL     time.Duration // How long entries remain cached.
	Size    int           // Maximum cache size in megabytes; zero is unlimited.
}

// Option configures a cache.
type Option func(*Config)

// WithCache enables or disables caching.
func WithCache(enabled bool) Option {
	return func(cfg *Config) {
		cfg.Enabled = enabled
	}
}

// WithCacheTTL sets the cache entry lifetime.
func WithCacheTTL(ttl time.Duration) Option {
	return func(cfg *Config) {
		cfg.TTL = ttl
	}
}

// WithCacheSize sets the cache size limit in megabytes. Zero means unlimited.
func WithCacheSize(size int) Option {
	return func(cfg *Config) {
		cfg.Size = size
	}
}

// NewConfig creates a cache configuration.
func NewConfig(opts ...Option) Config {
	cfg := Config{Enabled: true, TTL: DefaultTTL}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return normalize(cfg)
}

func normalize(cfg Config) Config {
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultTTL
	}
	if cfg.Size < 0 {
		cfg.Size = 0
	}
	return cfg
}

// ParseConfig reads cache, cache_ttl, and cache_size query parameters.
func ParseConfig(q url.Values) (Config, error) {
	cfg := NewConfig()
	if value := q.Get("cache"); value != "" {
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return cfg, fmt.Errorf("cached: parse cache setting %q: %w", value, err)
		}
		cfg.Enabled = enabled
	}
	if value := q.Get("cache_ttl"); value != "" {
		ttl, err := time.ParseDuration(value)
		if err != nil {
			return cfg, fmt.Errorf("cached: parse cache_ttl %q: %w", value, err)
		}
		cfg.TTL = ttl
	}
	if value := q.Get("cache_size"); value != "" {
		size, err := strconv.Atoi(value)
		if err != nil {
			return cfg, fmt.Errorf("cached: parse cache_size %q: %w", value, err)
		}
		if size < 0 {
			return cfg, fmt.Errorf("cached: cache_size must be >= 0")
		}
		cfg.Size = size
	}
	return normalize(cfg), nil
}

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
