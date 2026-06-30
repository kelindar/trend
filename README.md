<p align="center">
<img width="300" height="100" src=".github/logo.png" border="0" alt="kelindar/trend">
<br>
<img src="https://img.shields.io/github/go-mod/go-version/kelindar/trend" alt="Go Version">
<a href="https://pkg.go.dev/github.com/kelindar/trend"><img src="https://pkg.go.dev/badge/github.com/kelindar/trend" alt="PkgGoDev"></a>
<a href="https://goreportcard.com/report/github.com/kelindar/trend"><img src="https://goreportcard.com/badge/github.com/kelindar/trend" alt="Go Report Card"></a>
<a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
<a href="https://coveralls.io/github/kelindar/trend"><img src="https://coveralls.io/repos/github/kelindar/trend/badge.svg" alt="Coverage"></a>
</p>

## Trend: Small Time-Series Store for Go

Trend stores float64 samples and unsigned counters behind a small byte-store interface. It is meant for embedded or service-local time-series data where simple writes, mergeable state, iterator reads, and compact storage are enough.

It keeps recent raw points, can compact older points into buckets, and serializes state with a compact binary format compressed with S2.

- **Samples:** Last-write-wins float64 values.
- **Counters:** Grow-only unsigned counter deltas.
- **Reads:** `Values` and `Range` return Go iterators.
- **Storage:** Pluggable stores registered by URI scheme.

**Use When**

- You need simple local time-series storage inside a Go service.
- You want separate APIs for samples and counters.
- You want compact serialized state without running a metrics database.

**Not For**

- High-cardinality metrics ingestion at Prometheus/TSDB scale.
- Distributed query engines, retention policies, or alerting.
- Interoperable on-disk formats.

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/kelindar/trend"
	_ "github.com/kelindar/trend/storage/buntdb"
)

func main() {
	ctx := context.Background()

	db, err := trend.Open("buntdb:///:memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	now := time.Now()
	_ = db.Samples("cpu").Set(ctx, now, 0.42)
	_ = db.Counters("requests").Add(ctx, now, 1)

	values, _ := db.Samples("cpu").Values(ctx, now.Add(-time.Minute), now)
	for at, value := range values {
		fmt.Println(at, value)
	}
}
```

## API Highlights

- `Open(uri string, opts ...Option)`: Open a registered storage adapter.
- `New(store Store, opts ...Option)`: Use an existing store implementation.
- `Samples(key).Set(ctx, at, value)`: Store a float64 sample.
- `Samples(key).Values(ctx, from, to)`: Iterate raw sample values.
- `Samples(key).Range(ctx, from, to, span, agg)`: Iterate bucketed sample aggregates.
- `Counters(key).Add(ctx, at, delta)`: Store an unsigned counter delta.
- `Counters(key).Values(ctx, from, to)`: Iterate counter values.
- `Counters(key).Range(ctx, from, to, span, agg)`: Iterate bucketed counter aggregates.
- `Compact(ctx)`: Compact old points using the configured window.

## Storage

Stores implement a byte-oriented update interface:

```go
type Store interface {
	Load(context.Context, string) ([]byte, error)
	Update(context.Context, string, func([]byte) ([]byte, error)) error
	Delete(context.Context, string) error
	Close() error
}
```

Adapters register by URI scheme. Import the adapter package for registration:

```go
import (
	_ "github.com/kelindar/trend/storage/buntdb"
	_ "github.com/kelindar/trend/storage/redis"
	_ "github.com/kelindar/trend/storage/sqlite"
)
```

## Benchmarks

```text
name             time/op   ops/s    allocs/op
samples/append   327.9 us  3.0K     10
samples/range    177.9 us  5.6K     4
samples/values   95.0 us   10.5K    4
counters/append  286.4 us  3.5K     9
counters/range   100.7 us  9.9K     4
counters/values  78.6 us   12.7K    4
```

Numbers are from local 10K element DB-backed benchmarks on an Intel i7-13700K.

## About

Trend is MIT licensed and maintained by [@kelindar](https://github.com/kelindar).

PRs and issues welcome.
