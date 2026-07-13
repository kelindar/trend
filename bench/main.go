// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package main

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/kelindar/bench"
	"github.com/kelindar/trend"
	"github.com/kelindar/trend/storage/buntdb"
	"github.com/kelindar/trend/storage/memory"
	trendredis "github.com/kelindar/trend/storage/redis"
	"github.com/kelindar/trend/storage/sqlite"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	run(mainOptions...)
}

const benchCount = 10_000

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
	from := time.Unix(0, 0)
	to := time.Unix(benchCount-1, 0)
	return []benchCase{
		{
			name: "samples/append",
			setup: func() func(int) {
				store, seed := seedSampleStore(ctx, "samples")
				db := newMemoryDB(store, trend.WithReplica("samples-append"), trend.WithFlushEvery(time.Hour))
				samples := db.Samples("samples")
				return func(int) {
					store.Set("s:samples", seed)
					for i := range benchCount {
						at := time.Unix(int64(benchCount+i), 0)
						must(samples.Set(ctx, at, float64(i)))
					}
					must(db.Flush(ctx))
				}
			},
		},
		{
			name: "samples/range",
			setup: func() func(int) {
				store, _ := seedSampleStore(ctx, "samples")
				samples := newMemoryDB(store).Samples("samples")
				return func(int) {
					values, err := samples.Range(ctx, from, to, time.Second, trend.Mean)
					must(err)
					for range values {
					}
				}
			},
		},
		{
			name: "samples/values",
			setup: func() func(int) {
				store, _ := seedSampleStore(ctx, "samples")
				samples := newMemoryDB(store).Samples("samples")
				return func(int) {
					values, err := samples.Values(ctx, from, to)
					must(err)
					for range values {
					}
				}
			},
		},
		{
			name: "counters/append",
			setup: func() func(int) {
				store, seed := seedCounterStore(ctx, "counters")
				db := newMemoryDB(store, trend.WithReplica("counters-append"), trend.WithFlushEvery(time.Hour))
				counters := db.Counters("counters")
				return func(int) {
					store.Set("c:counters", seed)
					for i := range benchCount {
						at := time.Unix(int64(benchCount+i), 0)
						must(counters.Add(ctx, at, uint64(i+1)))
					}
					must(db.Flush(ctx))
				}
			},
		},
		{
			name: "counters/range",
			setup: func() func(int) {
				store, _ := seedCounterStore(ctx, "counters")
				counters := newMemoryDB(store).Counters("counters")
				return func(int) {
					values, err := counters.Range(ctx, from, to, time.Second, trend.Sum)
					must(err)
					for range values {
					}
				}
			},
		},
		{
			name: "counters/values",
			setup: func() func(int) {
				store, _ := seedCounterStore(ctx, "counters")
				counters := newMemoryDB(store).Counters("counters")
				return func(int) {
					values, err := counters.Values(ctx, from, to)
					must(err)
					for range values {
					}
				}
			},
		},
		{
			name: "sketches/append",
			setup: func() func(int) {
				store, seed := seedSketchStore(ctx, "sketches")
				db := newMemoryDB(store, trend.WithReplica("sketches-append"), trend.WithFlushEvery(time.Hour))
				sketches := db.Sketches("sketches")
				return func(int) {
					store.Set("h:sketches", seed)
					for i := range benchCount {
						at := time.Unix(int64(benchCount+i), 0)
						must(sketches.Add(ctx, at, float64(i)))
					}
					must(db.Flush(ctx))
				}
			},
		},
		{
			name: "sketches/range",
			setup: func() func(int) {
				store, _ := seedSketchStore(ctx, "sketches")
				sketches := newMemoryDB(store).Sketches("sketches")
				return func(int) {
					values, err := sketches.Range(ctx, from, to, time.Minute)
					must(err)
					for _, value := range values {
						_ = value.Quantile(0.99)
					}
				}
			},
		},
		{
			name: "sketches/values",
			setup: func() func(int) {
				store, _ := seedSketchStore(ctx, "sketches")
				sketches := newMemoryDB(store).Sketches("sketches")
				return func(int) {
					values, err := sketches.Values(ctx, from, to)
					must(err)
					for _, value := range values {
						_ = value.Count()
					}
				}
			},
		},
		loadCase(ctx, "load/memory", openMemoryStore),
		loadCase(ctx, "load/buntdb", openBuntDBStore),
		loadCase(ctx, "load/sqlite", openSQLiteStore),
		loadCase(ctx, "load/redis", openRedisStore),
	}
}

func loadCase(ctx context.Context, name string, open func() trend.Store) benchCase {
	const key = "s:samples"
	_, raw := seedSampleStore(ctx, "samples")
	return benchCase{
		name: name,
		setup: func() func(int) {
			store := open()
			must(store.Update(ctx, key, func([]byte) ([]byte, error) {
				return raw, nil
			}))
			return func(int) {
				got, err := store.Load(ctx, key)
				must(err)
				if len(got) != len(raw) {
					panic("short load")
				}
			}
		},
	}
}

func openMemoryStore() trend.Store {
	store, err := memory.New(time.Hour)
	must(err)
	return store
}

func openBuntDBStore() trend.Store {
	store, err := buntdb.Open(&url.URL{Path: ":memory:"})
	must(err)
	return store
}

func openSQLiteStore() trend.Store {
	store, err := sqlite.Open(&url.URL{Path: ":memory:"})
	must(err)
	return store
}

func openRedisStore() trend.Store {
	server, err := miniredis.Run()
	must(err)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	return trendredis.New(client, "")
}

func seedSampleStore(ctx context.Context, key string) (*memoryStore, []byte) {
	store := newMemoryStore()
	db := newMemoryDB(store, trend.WithReplica(key+"-seed"), trend.WithFlushEvery(time.Hour))
	seedSamples(ctx, db, key, benchCount)
	must(db.Close())
	raw, err := store.Load(ctx, "s:"+key)
	must(err)
	return store, raw
}

func seedCounterStore(ctx context.Context, key string) (*memoryStore, []byte) {
	store := newMemoryStore()
	db := newMemoryDB(store, trend.WithReplica(key+"-seed"), trend.WithFlushEvery(time.Hour))
	seedCounters(ctx, db, key, benchCount)
	must(db.Close())
	raw, err := store.Load(ctx, "c:"+key)
	must(err)
	return store, raw
}

func seedSketchStore(ctx context.Context, key string) (*memoryStore, []byte) {
	store := newMemoryStore()
	db := newMemoryDB(store, trend.WithReplica(key+"-seed"), trend.WithFlushEvery(time.Hour))
	seedSketches(ctx, db, key, benchCount)
	must(db.Close())
	raw, err := store.Load(ctx, "h:"+key)
	must(err)
	return store, raw
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

func seedSketches(ctx context.Context, db *trend.DB, key string, count int) {
	for i := range count {
		must(db.Sketches(key).Add(ctx, time.Unix(int64(i), 0), float64(i)))
	}
	must(db.Flush(ctx))
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

func (s *memoryStore) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = clone(value)
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
