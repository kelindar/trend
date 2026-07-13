// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// DB stores time-series samples, counters, and histograms.
type DB struct {
	store      Store
	replica    uint64
	clock      atomic.Uint64
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
			db.buffer = &buffer{items: make(map[string]*pending)}
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

// Histograms returns a histogram series handle.
func (db *DB) Histograms(key string) Histograms {
	db.seen.Store(histogramKey(key), struct{}{})
	return Histograms{db: db, key: histogramKey(key)}
}

// Close flushes pending writes and closes resources.
func (db *DB) Close() error {
	if db.done != nil {
		db.done()
	}
	if err := db.Flush(context.Background()); err != nil {
		return err
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

func (db *DB) merge(ctx context.Context, key string, delta *pending) error {
	return db.store.Update(ctx, key, func(old []byte) ([]byte, error) {
		return series(old).append(delta)
	})
}

func (db *DB) writeSample(ctx context.Context, key string, at uint64, value float64) error {
	clock := db.clock.Add(1)
	if db.buffer != nil {
		db.buffer.addSample(key, at, value, clock, db.replica)
		return nil
	}
	var delta pending
	delta.Samples.Add(at, value, clock, db.replica)
	if err := db.merge(ctx, key, &delta); err != nil {
		return err
	}
	return nil
}

func (db *DB) writeCounter(ctx context.Context, key string, at, value uint64) error {
	clock := db.clock.Add(1)
	if db.buffer != nil {
		db.buffer.addCounter(key, at, db.replica, clock, value)
		return nil
	}
	var delta pending
	delta.Counters.Add(at, db.replica, clock, value)
	if err := db.merge(ctx, key, &delta); err != nil {
		return err
	}
	return nil
}

func (db *DB) writeHistogram(ctx context.Context, key string, at uint64, value float64) error {
	clock := db.clock.Add(1)
	if db.buffer != nil {
		db.buffer.addHistogram(key, at, value, clock, db.replica)
		return nil
	}
	var delta pending
	delta.Histograms.Add(at, value, clock, db.replica)
	return db.merge(ctx, key, &delta)
}

func (db *DB) loadRaw(ctx context.Context, key string) (series, error) {
	raw, err := db.store.Load(ctx, key)
	if err != nil {
		return nil, err
	}
	return series(raw), nil
}

func (db *DB) load(ctx context.Context, key string) (series, error) {
	raw, err := db.loadRaw(ctx, key)
	if err != nil {
		return nil, err
	}
	if err := raw.versionOK(); err != nil {
		return nil, err
	}
	if db.buffer != nil {
		if tail := db.buffer.clone(key); tail != nil {
			current, err := raw.pending()
			if err != nil {
				return nil, err
			}
			current.Merge(tail)
			marshaled, err := loadMarshal(current)
			if err != nil {
				return nil, err
			}
			raw = series(marshaled)
		}
	}
	return raw, nil
}

func sampleKey(key string) string    { return "s:" + key }
func counterKey(key string) string   { return "c:" + key }
func histogramKey(key string) string { return "h:" + key }

var loadMarshal = func(p *pending) ([]byte, error) {
	return p.marshal()
}

// -----------------------------------------------------------------------------

type buffer struct {
	mu    sync.Mutex
	items map[string]*pending
	spare []*pending
}

func (b *buffer) flush() map[string]*pending {
	b.mu.Lock()
	defer b.mu.Unlock()
	items := b.items
	b.items = make(map[string]*pending, len(items))
	return items
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

func (b *buffer) addHistogram(key string, at uint64, value float64, clock, replica uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.series(key).Histograms.Add(at, value, clock, replica)
}

func (b *buffer) clone(key string) *pending {
	b.mu.Lock()
	defer b.mu.Unlock()
	return clonePending(b.items[key])
}

func (b *buffer) series(key string) *pending {
	if b.items[key] == nil {
		b.items[key] = b.get()
	}
	return b.items[key]
}

func (b *buffer) get() *pending {
	n := len(b.spare)
	if n == 0 {
		return &pending{}
	}
	out := b.spare[n-1]
	b.spare = b.spare[:n-1]
	return out
}

func (b *buffer) recycle(s *pending) {
	s.Reset()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.spare = append(b.spare, s)
}

func clonePending(p *pending) *pending {
	if p == nil {
		return nil
	}
	out := &pending{}
	out.Samples.Time = append(out.Samples.Time, p.Samples.Time...)
	out.Samples.Data = append(out.Samples.Data, p.Samples.Data...)
	out.Samples.Clock = append(out.Samples.Clock, p.Samples.Clock...)
	out.Samples.Replica = append(out.Samples.Replica, p.Samples.Replica...)
	out.Samples.Buckets = append(out.Samples.Buckets, p.Samples.Buckets...)
	out.Counters.Time = append(out.Counters.Time, p.Counters.Time...)
	out.Counters.Replica = append(out.Counters.Replica, p.Counters.Replica...)
	out.Counters.Clock = append(out.Counters.Clock, p.Counters.Clock...)
	out.Counters.Value = append(out.Counters.Value, p.Counters.Value...)
	out.Counters.Buckets = append(out.Counters.Buckets, p.Counters.Buckets...)
	out.Histograms.Time = append(out.Histograms.Time, p.Histograms.Time...)
	out.Histograms.Data = append(out.Histograms.Data, p.Histograms.Data...)
	out.Histograms.Clock = append(out.Histograms.Clock, p.Histograms.Clock...)
	out.Histograms.Replica = append(out.Histograms.Replica, p.Histograms.Replica...)
	out.Histograms.Buckets = cloneHistogramBuckets(p.Histograms.Buckets)
	return out
}

func cloneHistogramBuckets(buckets []histogramBucket) []histogramBucket {
	out := make([]histogramBucket, len(buckets))
	for i, b := range buckets {
		out[i] = histogramBucket{Time: b.Time, Data: append([]byte(nil), b.Data...)}
	}
	return out
}
