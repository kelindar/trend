module github.com/kelindar/trend/bench

go 1.25.0

require (
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/kelindar/bench v0.3.2
	github.com/kelindar/trend v0.0.0
	github.com/kelindar/trend/storage/buntdb v0.0.0
	github.com/kelindar/trend/storage/memory v0.0.0
	github.com/kelindar/trend/storage/redis v0.0.0
	github.com/kelindar/trend/storage/sqlite v0.0.0
	github.com/redis/go-redis/v9 v9.6.0
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/allegro/bigcache/v3 v3.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/ncruces/go-sqlite3 v0.35.1 // indirect
	github.com/ncruces/go-sqlite3-wasm/v3 v3.1.35302 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/tidwall/btree v1.4.2 // indirect
	github.com/tidwall/buntdb v1.3.2 // indirect
	github.com/tidwall/gjson v1.14.3 // indirect
	github.com/tidwall/grect v0.1.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tidwall/rtred v0.1.2 // indirect
	github.com/tidwall/tinyqueue v0.1.1 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	golang.org/x/sys v0.46.0 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/kelindar/trend => ..

replace github.com/kelindar/trend/storage/buntdb => ../storage/buntdb

replace github.com/kelindar/trend/storage/memory => ../storage/memory

replace github.com/kelindar/trend/storage/redis => ../storage/redis

replace github.com/kelindar/trend/storage/sqlite => ../storage/sqlite
