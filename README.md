# go-gitpack

A minimal, memory-mapped Git object store that resolves objects directly from `*.pack` files without shelling out to the Git executable.

## Overview

The `objstore` package provides fast, read-only access to Git objects stored in packfiles. It's designed for scenarios where you need low-latency lookups, such as secret scanning, indexing, etc.

Note: This is an experimental learning repo.

## Usage

### Scan every unique blob (recommended)

Blob mode visits every unique blob exactly once in pack-offset order: no diff
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

- **Per-scanner offset cache**: each `HistoryScanner` keeps a cache of
  materialized pack objects (default budget 256 MiB) that accelerates
  delta-chain resolution. Processes that open many repositories concurrently
  should lower it with `objstore.WithOffsetCacheBudget(bytes)`; a budget
  `<= 0` disables the cache. The memory is released on `Close`.
- **Per-scanner pair cache**: each `HistoryScanner` memoizes computed diff
  hunks by blob pair (default budget 128 MiB) because merges and long-lived
  branches replay the same transitions. Entries are stored pointer-free (one
  text buffer plus offset tables), so a full cache costs the garbage
  collector little to mark. Lower it with `objstore.WithPairCacheBudget`.
- **Delta reconstruction buffers**: each hop of a multi-hop delta chain
  writes into a buffer sized to that hop's target, recycled through
  size-class pools that the garbage collector trims when idle. An in-flight
  reconstruction holds its two largest consecutive hops and nothing more, so
  resident memory no longer scales with worker count (a 192-CPU host scanned
  trufflehog's history in ~1 GB where fixed 32 MiB arenas took ~10 GB).
- **Large-blob prefetch**: a hunk scan inflates pack entries above 2 MiB
  compressed at scan start, biggest first, within a 256 MiB budget, so the
  slowest single inflations overlap the rest of the walk instead of forming
  its tail. A worker that needs one of these blobs before the prefetch
  reaches it inflates the blob itself. The prefetched bytes are released
  when the scan ends, and a scanner with the offset cache disabled
  (`WithOffsetCacheBudget(0)` or `GOGITPACK_OFFSET_CACHE_BUDGET<=0`) skips
  the prefetch.

## Environment variables

Runtime overrides read once at process start: no rebuild or code change
required:

- `GOGITPACK_OFFSET_CACHE_BUDGET`: per-store offset-cache budget in bytes;
  `<= 0` disables the cache. Overrides the compiled 256 MiB default (code
  can still call `WithOffsetCacheBudget` per scanner).
- `GOGITPACK_NOASM_INFLATE`: set to `1` to disable the amd64/arm64 assembly
  inflate kernels and use the portable Go decoder (same effect as building
  with the `purego` tag, without rebuilding).

## Build flags for maximum throughput

On ARM (Graviton3+) hosts, building with LSE atomics and shipping the bundled
PGO profile is measurably faster (~4-8% combined on history scans):

```bash
GOARM64=v8.4 go build -pgo=default.pgo ./...
```

`default.pgo` is a CPU profile captured from a full-history `DiffHistoryHunks`
scan. Pass `-pgo=default.pgo` explicitly, as above; it is not applied
automatically. The default `-pgo=auto` selects a `default.pgo` only from each
*main package's own directory*, and this module's root is `package objstore`:
a library, not a main package. Downstream binaries that import `objstore`
therefore need the explicit flag, or a copy of this profile in the directory of
the main package being built.

### Optional libdeflate backend (cgo)

For another ~2x on decompression-bound scans, build with the `gitpack_libdeflate`
tag against a static [libdeflate](https://github.com/ebiggers/libdeflate):

```bash
git clone --depth 1 -b v1.24 https://github.com/ebiggers/libdeflate /tmp/libdeflate
cmake -S /tmp/libdeflate -B /tmp/libdeflate/build -DCMAKE_BUILD_TYPE=Release \
  -DLIBDEFLATE_BUILD_SHARED_LIB=OFF -DLIBDEFLATE_BUILD_GZIP=OFF
cmake --build /tmp/libdeflate/build -j

CGO_CFLAGS="-I/tmp/libdeflate" \
CGO_LDFLAGS="-L/tmp/libdeflate/build" \
GOARM64=v8.4 go build -tags gitpack_libdeflate -pgo=default.pgo ./...
```

`CGO_LDFLAGS` must supply a `-L` search directory rather than the archive path
alone: cgo *adds* these flags to the `#cgo LDFLAGS: -ldeflate` directive in
`zlib_cgo.go` instead of replacing it, so the link still resolves `-ldeflate`
and fails on a host with no system-wide libdeflate.

Pack objects are always inflated to a size known in advance from the object
header, which matches libdeflate's one-shot whole-buffer model exactly. On a
full trufflehog history scan this halves wall time again (300ms → 150ms).
The default build remains pure Go.

### High-throughput consumers: use DiffHistoryHunksFunc

`DiffHistoryHunks` delivers every hunk through one channel, so hunk processing
runs on a single consumer goroutine. If your per-hunk work is CPU-bound
(regex/secret scanning, hashing), use the concurrent-callback API instead:
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
