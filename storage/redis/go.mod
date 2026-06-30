module github.com/kelindar/trend/storage/redis

go 1.25.0

require (
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/kelindar/trend v0.0.0
	github.com/redis/go-redis/v9 v9.6.0
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/allegro/bigcache/v3 v3.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/kelindar/trend => ../..
