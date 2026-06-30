// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"hash/fnv"
	"time"

	"github.com/kelindar/trend/machine"
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
		db.compactor.after = after
		db.compactor.span = span
		return nil
	}
}

// WithCompactor starts a jittered background compactor for keys seen locally.
func WithCompactor(every, jitter time.Duration) Option {
	return func(db *DB) error {
		db.compactor.every = every
		db.compactor.jitter = jitter
		return nil
	}
}

func defaultReplica() uint64 {
	return uint64(machine.ID())
}

func hashReplica(id string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return h.Sum64()
}
