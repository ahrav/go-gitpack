module github.com/ahrav/go-gitpack

go 1.24.4

require (
	github.com/hashicorp/golang-lru/arc/v2 v2.0.7
	golang.org/x/exp v0.0.0-20250606033433-dcc06ee1d476
)

require (
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/klauspost/compress v1.19.0
)

require (
	github.com/kr/pretty v0.1.0 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-farm v0.0.0-20240924180020-3414d57e47da
	github.com/hexops/gotextdiff v1.0.3
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.10.0
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Pinned fork containing the bytes.Reader flate fast path.
//
// NOTE FOR CONSUMERS: replace directives only apply when this repo is the
// main module. Downstream modules importing this library resolve the
// upstream klauspost/compress version above — behavior is identical, but the
// bytes.Reader decode fast path (and its throughput gain) is absent. To opt
// in, copy this replace line into your own go.mod.
replace github.com/klauspost/compress => github.com/ahrav/compress v0.0.0-20260708010904-f4b8e874a3f0
