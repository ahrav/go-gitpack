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
