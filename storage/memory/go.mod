module github.com/kelindar/trend/storage/memory

go 1.25.0

require (
	github.com/allegro/bigcache/v3 v3.1.0
	github.com/kelindar/trend v0.0.0
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/kelindar/trend => ../..
