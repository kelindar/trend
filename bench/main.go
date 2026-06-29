// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package main

import (
	"context"
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
	}
}

func seededDB(name string, count int) *trend.DB {
	ctx := context.Background()
	db := newDB(name)
	for i := range count {
		at := time.Unix(int64(i), 0)
		must(db.Samples("cpu").Set(ctx, at, float64(i)))
		must(db.Counters("requests").Add(ctx, at, uint64(i+1)))
	}
	return db
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

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func (c benchCase) String() string {
	return c.name
}
