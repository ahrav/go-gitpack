# go-gitpack

A minimal, memory-mapped Git object store that resolves objects directly from `*.pack` files without shelling out to the Git executable.

## Overview

The `objstore` package provides fast, read-only access to Git objects stored in packfiles. It's designed for scenarios where you need low-latency lookups, such as secret scanning, indexing, etc.

Note: This is very much experimental and a learning repo.

## Usage

### Scan every unique blob (recommended)

Blob mode visits every unique blob exactly once in pack-offset order — no diff
computation, sequential I/O, and each blob is seen only once via deduplication.

```go
type myScanner struct{}

func (s *myScanner) ScanBlob(r io.Reader, meta objstore.ScanMeta) error {
    // meta.Blob, meta.Commit, meta.Path available
    _, err := io.Copy(io.Discard, r)
    return err
}

scanner, err := objstore.NewHistoryScanner("/path/to/.git")
if err != nil {
    log.Fatal(err)
}
defer scanner.Close()

if err := scanner.Scan(nil, &myScanner{}); err != nil {
    log.Fatal(err)
}
```

## Memory characteristics

- **Per-scanner offset cache** — each `HistoryScanner` keeps a cache of
  materialized pack objects (default budget 256 MiB) that accelerates
  delta-chain resolution. Processes that open many repositories concurrently
  should lower it with `objstore.WithOffsetCacheBudget(bytes)`; a budget
  `<= 0` disables the cache. The memory is released on `Close`.
- **Process-global delta arenas** — delta resolution reuses 32 MiB scratch
  arenas from a bounded free-list sized from `GOMAXPROCS` (at most 8 arenas,
  256 MiB). The reserve is retained for the process lifetime after peak
  concurrency; bounding scan concurrency bounds it proportionally.

## Environment variables

Runtime overrides read once at process start — no rebuild or code change
required:

- `GOGITPACK_OFFSET_CACHE_BUDGET` — per-store offset-cache budget in bytes;
  `<= 0` disables the cache. Overrides the compiled 256 MiB default (code
  can still call `WithOffsetCacheBudget` per scanner).
- `GOGITPACK_DELTA_ARENA_RETAIN` — maximum idle 32 MiB delta arenas retained
  process-wide; `0` disables retention so arenas are released to the GC
  after use.
- `GOGITPACK_NOASM_INFLATE` — set to `1` to disable the amd64/arm64 assembly
  inflate kernels and use the portable Go decoder (same effect as building
  with the `purego` tag, without rebuilding).

## Build flags for maximum throughput

On ARM (Graviton3+) hosts, building with LSE atomics and shipping the bundled
PGO profile is measurably faster (~4-8% combined on history scans):

```bash
GOARM64=v8.4 go build -pgo=default.pgo ./...
```

`default.pgo` is a CPU profile captured from a full-history `DiffHistoryHunks`
scan; `go build` picks it up automatically when building this package as the
main module.

### Optional libdeflate backend (cgo)

For another ~2x on decompression-bound scans, build with the `gitpack_libdeflate`
tag against a static [libdeflate](https://github.com/ebiggers/libdeflate):

```bash
git clone --depth 1 -b v1.24 https://github.com/ebiggers/libdeflate /tmp/libdeflate
cmake -S /tmp/libdeflate -B /tmp/libdeflate/build -DCMAKE_BUILD_TYPE=Release \
  -DLIBDEFLATE_BUILD_SHARED_LIB=OFF -DLIBDEFLATE_BUILD_GZIP=OFF
cmake --build /tmp/libdeflate/build -j

CGO_CFLAGS="-I/tmp/libdeflate" \
CGO_LDFLAGS="/tmp/libdeflate/build/libdeflate.a" \
GOARM64=v8.4 go build -tags gitpack_libdeflate -pgo=default.pgo ./...
```

Pack objects are always inflated to a size known in advance from the object
header, which matches libdeflate's one-shot whole-buffer model exactly. On a
full trufflehog history scan this halves wall time again (300ms → 150ms).
The default build remains pure Go.

### High-throughput consumers: use DiffHistoryHunksFunc

`DiffHistoryHunks` delivers every hunk through one channel, so hunk processing
runs on a single consumer goroutine. If your per-hunk work is CPU-bound
(regex/secret scanning, hashing), use the concurrent-callback API instead —
the callback runs on every internal worker in parallel:

```go
err := scanner.DiffHistoryHunksFunc(func(h objstore.HunkAddition) error {
    // called concurrently from up to runtime.NumCPU() workers;
    // must be safe for concurrent use.
    return scan(h)
})
```

On a full trufflehog history scan with a hashing consumer this is ~2.2x
faster end-to-end than draining the channel (917ms → 410ms).
