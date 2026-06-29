// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"hash/fnv"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/rs/xid"
)

// Option configures a DB.
type Option func(*DB) error

// WithReplica sets a stable replica identifier.
func WithReplica(id string) Option {
	return func(db *DB) error {
		db.replica = hashReplica(id)
		return nil
	}
}

// WithCache enables a local read-through cache.
func WithCache(ttl time.Duration) Option {
	return func(db *DB) error {
		cache, err := bigcache.NewBigCache(bigcache.DefaultConfig(ttl))
		db.cache = cache
		return err
	}
}

// WithFlushEvery buffers writes in memory and flushes them periodically.
func WithFlushEvery(every time.Duration) Option {
	return func(db *DB) error {
		db.flushEvery = every
		return nil
	}
}

// WithCompaction enables lossy compaction of raw values older than after.
func WithCompaction(after, span time.Duration) Option {
	return func(db *DB) error {
		db.compactAfter = after
		db.compactSpan = span
		return nil
	}
}

// WithCompactor starts a jittered background compactor for keys seen locally.
func WithCompactor(every, jitter time.Duration) Option {
	return func(db *DB) error {
		db.compactEvery = every
		db.compactJitter = jitter
		return nil
	}
}

func defaultReplica() uint64 {
	return hashReplica(xid.New().String())
}

func hashReplica(id string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return h.Sum64()
}
