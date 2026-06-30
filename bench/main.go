// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package main

import (
	"context"
	"sync"
	"time"

	"github.com/kelindar/bench"
	"github.com/kelindar/trend"
	trendbuntdb "github.com/kelindar/trend/storage/buntdb"
	"github.com/tidwall/buntdb"
)

func main() {
	run(mainOptions...)
}

var mainOptions = []bench.Option{bench.WithSamples(20), bench.WithDuration(200 * time.Millisecond)}

func run(opts ...bench.Option) {
	bench.Run(func(b *bench.B) {
		for _, tc := range cases() {
			run := tc.setup()
			b.Run(tc.name, func(i int) {
				run(i)
			})
		}
	}, opts...)
}

type benchCase struct {
	name  string
	setup func() func(int)
}

func cases() []benchCase {
	ctx := context.Background()
	return []benchCase{
		{
			name: "samples/set",
			setup: func() func(int) {
				db := newBufferedDB("samples-set")
				samples := db.Samples("cpu")
				return func(i int) {
					must(samples.Set(ctx, time.Unix(int64(i), 0), float64(i)))
				}
			},
		},
		{
			name: "samples/range",
			setup: func() func(int) {
				db := seededDB("samples-range", 512)
				samples := db.Samples("cpu")
				from := time.Unix(0, 0)
				to := time.Unix(512, 0)
				_, err := samples.Range(ctx, from, to, time.Minute, trend.Mean)
				must(err)
				return func(int) {
					_, err := samples.Range(ctx, from, to, time.Minute, trend.Mean)
					must(err)
				}
			},
		},
		{
			name: "counters/add",
			setup: func() func(int) {
				db := newBufferedDB("counters-add")
				counters := db.Counters("requests")
				return func(i int) {
					must(counters.Add(ctx, time.Unix(int64(i), 0), uint64(i+1)))
				}
			},
		},
		{
			name: "counters/range",
			setup: func() func(int) {
				db := seededDB("counters-range", 512)
				counters := db.Counters("requests")
				from := time.Unix(0, 0)
				to := time.Unix(512, 0)
				_, err := counters.Range(ctx, from, to, time.Minute, trend.Sum)
				must(err)
				return func(int) {
					_, err := counters.Range(ctx, from, to, time.Minute, trend.Sum)
					must(err)
				}
			},
		},
		{
			name: "codec/marshal_10k",
			setup: func() func(int) {
				store := newMemoryStore()
				db := newMemoryDB(store, trend.WithReplica("codec-marshal"), trend.WithFlushEvery(time.Hour))
				samples := db.Samples("codec")
				return func(int) {
					for i := range 10_000 {
						must(samples.Set(ctx, time.Unix(int64(i), 0), float64(i)))
					}
					must(db.Flush(ctx))
					must(store.Delete(ctx, "s:codec"))
				}
			},
		},
		{
			name: "codec/decode_10k",
			setup: func() func(int) {
				store := newMemoryStore()
				seedSamples(ctx, newMemoryDB(store, trend.WithReplica("codec-seed"), trend.WithFlushEvery(time.Hour)), "codec", 10_000)
				db := newMemoryDB(store)
				samples := db.Samples("codec")
				from := time.Unix(0, 0)
				to := time.Unix(10_000, 0)
				return func(int) {
					values, err := samples.Values(ctx, from, to)
					must(err)
					for range values {
					}
				}
			},
		},
	}
}

func seededDB(name string, count int) *trend.DB {
	ctx := context.Background()
	db := newDB(name)
	seedSamples(ctx, db, "cpu", count)
	seedCounters(ctx, db, "requests", count)
	return db
}

func seedSamples(ctx context.Context, db *trend.DB, key string, count int) {
	for i := range count {
		at := time.Unix(int64(i), 0)
		must(db.Samples(key).Set(ctx, at, float64(i)))
	}
	must(db.Flush(ctx))
}

func seedCounters(ctx context.Context, db *trend.DB, key string, count int) {
	for i := range count {
		must(db.Counters(key).Add(ctx, time.Unix(int64(i), 0), uint64(i+1)))
	}
	must(db.Flush(ctx))
}

func newDB(name string) *trend.DB {
	db, err := buntdb.Open(":memory:")
	must(err)
	out, err := trend.New(trendbuntdb.New(db, ""), trend.WithReplica(name), trend.WithCache(time.Minute))
	must(err)
	return out
}

func newBufferedDB(name string) *trend.DB {
	db, err := buntdb.Open(":memory:")
	must(err)
	out, err := trend.New(trendbuntdb.New(db, ""), trend.WithReplica(name), trend.WithFlushEvery(time.Hour))
	must(err)
	return out
}

func newMemoryDB(store *memoryStore, opts ...trend.Option) *trend.DB {
	out, err := trend.New(store, opts...)
	must(err)
	return out
}

type memoryStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemoryStore() *memoryStore {
	return &memoryStore{data: make(map[string][]byte)}
}

func (s *memoryStore) Load(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clone(s.data[key]), nil
}

func (s *memoryStore) Update(_ context.Context, key string, fn func([]byte) ([]byte, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next, err := fn(clone(s.data[key]))
	if err != nil {
		return err
	}
	s.data[key] = clone(next)
	return nil
}

func (s *memoryStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}

func (s *memoryStore) Close() error {
	return nil
}

func clone(v []byte) []byte {
	return append([]byte(nil), v...)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func (c benchCase) String() string {
	return c.name
}
