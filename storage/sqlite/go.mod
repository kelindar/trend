module github.com/kelindar/trend/storage/sqlite

go 1.25.0

require (
	github.com/kelindar/trend v0.2.0
	github.com/kelindar/trend/storage/memory v0.1.0
	github.com/ncruces/go-sqlite3 v0.35.1
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/allegro/bigcache/v3 v3.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/ncruces/go-sqlite3-wasm/v3 v3.1.35302 // indirect
	github.com/ncruces/julianday v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/kelindar/trend => ../..

replace github.com/kelindar/trend/storage/memory => ../memory
