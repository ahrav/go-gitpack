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
