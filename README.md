<p align="center">
<img width="300" height="100" src=".github/logo.png" border="0" alt="kelindar/trend">
<br>
<img src="https://img.shields.io/github/go-mod/go-version/kelindar/trend" alt="Go Version">
<a href="https://pkg.go.dev/github.com/kelindar/trend"><img src="https://pkg.go.dev/badge/github.com/kelindar/trend" alt="PkgGoDev"></a>
<a href="https://goreportcard.com/report/github.com/kelindar/trend"><img src="https://goreportcard.com/badge/github.com/kelindar/trend" alt="Go Report Card"></a>
<a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
<a href="https://coveralls.io/github/kelindar/trend"><img src="https://coveralls.io/repos/github/kelindar/trend/badge.svg" alt="Coverage"></a>
</p>

# Trend

Trend is a small CRDT-backed time-series library for Go. It stores float samples and unsigned counters, keeps reads cheap with a cache, and keeps storage replaceable behind a tiny byte-store interface.

## Install

```sh
go get github.com/kelindar/trend
```

Pick a storage adapter by importing it for registration:

```go
import _ "github.com/kelindar/trend/storage/buntdb"
```

## Example

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
	db, err := trend.Open("buntdb:///:memory:", trend.WithCache(time.Minute))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	now := time.Now()
	_ = db.Samples("cpu").Set(ctx, now, 0.42)
	_ = db.Counters("requests").Add(ctx, now, 1)

	values, _ := db.Samples("cpu").Values(ctx, now.Add(-time.Minute), now)
	values(func(at time.Time, value float64) bool {
		fmt.Println(at, value)
		return true
	})
}
```

## Features

- `trend.Samples` for float64 samples with last-write-wins merge semantics.
- `trend.Counters` for unsigned grow-only counter deltas.
- `Range` and `Values` APIs using Go iterators.
- Optional buffered writes to reduce storage pressure.
- Optional compaction for old windows.
- BigCache read-through caching.
- Storage adapters in separate modules, starting with BuntDB and Redis.

## Storage

Trend stores opaque bytes through this interface:

```go
type Store interface {
	Load(context.Context, string) ([]byte, error)
	Update(context.Context, string, func([]byte) ([]byte, error)) error
	Delete(context.Context, string) error
	Close() error
}
```

Adapters register themselves by scheme, so swapping storage is a URI and import change.

## License

Trend is distributed under the MIT license. See [LICENSE](LICENSE).
