// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/allegro/bigcache/v3"
)

// DB stores time-series samples and counters.
type DB struct {
	store      Store
	replica    uint64
	clock      atomic.Uint64
	cache      *bigcache.BigCache
	flushEvery time.Duration
	seen       sync.Map
	buffer     *buffer
	done       context.CancelFunc
	compactor  compactor
}

// Open opens a registered store URI.
func Open(uri string, opts ...Option) (*DB, error) {
	store, err := openStore(uri)
	if err != nil {
		return nil, err
	}
	return New(store, opts...)
}

// New creates a DB over an existing store.
func New(store Store, opts ...Option) (*DB, error) {
	db := &DB{
		store:     store,
		replica:   defaultReplica(),
		compactor: compactor{after: 24 * time.Hour, span: time.Hour},
	}
	for _, opt := range opts {
		if err := opt(db); err != nil {
			_ = store.Close()
			return nil, err
		}
	}
	if db.flushEvery > 0 || db.compactor.every > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		db.done = cancel
		if db.flushEvery > 0 {
			db.buffer = &buffer{items: make(map[string]*series)}
			go db.flushLoop(ctx)
		}
		if db.compactor.every > 0 {
			go db.compactLoop(ctx)
		}
	}
	return db, nil
}

// Samples returns a sample series handle.
func (db *DB) Samples(key string) Samples {
	db.seen.Store(sampleKey(key), struct{}{})
	return Samples{db: db, key: sampleKey(key)}
}

// Counters returns a counter series handle.
func (db *DB) Counters(key string) Counters {
	db.seen.Store(counterKey(key), struct{}{})
	return Counters{db: db, key: counterKey(key)}
}

// Close flushes pending writes and closes resources.
func (db *DB) Close() error {
	if db.done != nil {
		db.done()
	}
	if err := db.Flush(context.Background()); err != nil {
		return err
	}
	if db.cache != nil {
		_ = db.cache.Close()
	}
	return db.store.Close()
}

// Flush writes buffered deltas.
func (db *DB) Flush(ctx context.Context) error {
	if db.buffer == nil {
		return nil
	}

	for key, delta := range db.buffer.flush() {
		if err := db.merge(ctx, key, delta); err != nil {
			return err
		}
		db.buffer.recycle(delta)
	}
	return nil
}

func (db *DB) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(db.flushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = db.Flush(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (db *DB) merge(ctx context.Context, key string, delta *series) error {
	return db.store.Update(ctx, key, func(old []byte) ([]byte, error) {
		if len(old) == 0 {
			return delta.Marshal()
		}
		current, err := decode(old)
		if err != nil {
			return nil, err
		}
		current.Merge(delta)
		return current.Marshal()
	})
}

func (db *DB) write(ctx context.Context, key string, delta *series) error {
	db.seen.Store(key, struct{}{})
	if db.buffer != nil {
		db.buffer.append(key, delta)
		db.dropCache(key)
		return nil
	}
	if err := db.merge(ctx, key, delta); err != nil {
		return err
	}
	db.dropCache(key)
	return nil
}

func (db *DB) writeSample(ctx context.Context, key string, at uint64, value float64) error {
	clock := db.clock.Add(1)
	if db.buffer != nil {
		db.buffer.addSample(key, at, value, clock, db.replica)
		db.dropCache(key)
		return nil
	}
	var delta series
	delta.Samples.Add(at, value, clock, db.replica)
	return db.write(ctx, key, &delta)
}

func (db *DB) writeCounter(ctx context.Context, key string, at, value uint64) error {
	clock := db.clock.Add(1)
	if db.buffer != nil {
		db.buffer.addCounter(key, at, db.replica, clock, value)
		db.dropCache(key)
		return nil
	}
	var delta series
	delta.Counters.Add(at, db.replica, clock, value)
	return db.write(ctx, key, &delta)
}

func (db *DB) load(ctx context.Context, key string) (*series, error) {
	var raw []byte
	if db.cache != nil {
		if v, err := db.cache.Get(key); err == nil {
			raw = v
		}
	}
	if raw == nil {
		var err error
		if raw, err = db.store.Load(ctx, key); err != nil {
			return nil, err
		}
		if db.cache != nil && raw != nil {
			_ = db.cache.Set(key, raw)
		}
	}
	out, err := decode(raw)
	if err != nil {
		return nil, err
	}
	if db.buffer != nil {
		db.buffer.mergeInto(key, out)
	}
	return out, nil
}

func (db *DB) dropCache(key string) {
	if db.cache != nil {
		_ = db.cache.Delete(key)
	}
}

func sampleKey(key string) string  { return "s:" + key }
func counterKey(key string) string { return "c:" + key }

// -----------------------------------------------------------------------------

type buffer struct {
	mu    sync.Mutex
	items map[string]*series
	spare []*series
}

func (b *buffer) flush() map[string]*series {
	b.mu.Lock()
	defer b.mu.Unlock()
	items := b.items
	b.items = make(map[string]*series, len(items))
	return items
}

func (b *buffer) append(key string, delta *series) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.series(key).Append(delta)
}

func (b *buffer) addSample(key string, at uint64, value float64, clock, replica uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.series(key).Samples.Add(at, value, clock, replica)
}

func (b *buffer) addCounter(key string, at, replica, clock, value uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.series(key).Counters.Add(at, replica, clock, value)
}

func (b *buffer) mergeInto(key string, out *series) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out.Merge(b.items[key])
}

func (b *buffer) series(key string) *series {
	if b.items[key] == nil {
		b.items[key] = b.get()
	}
	return b.items[key]
}

func (b *buffer) get() *series {
	n := len(b.spare)
	if n == 0 {
		return &series{}
	}
	out := b.spare[n-1]
	b.spare = b.spare[:n-1]
	return out
}

func (b *buffer) recycle(s *series) {
	s.Reset()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.spare = append(b.spare, s)
}
