# HistoryScanner Examples

This directory contains working examples demonstrating how to use the `HistoryScanner` from the go-gitpack library to scan Git repository history.

## Prerequisites

- Go 1.24.4 or later
- A Git repository with **packfiles** (required - the scanner doesn't work with loose objects only)

Before running the examples, ensure your repository has packfiles:

```bash
# Generate packfiles (consolidate loose objects)
git repack -Ad
```

> **Note:** A commit-graph file is not required. The scanner builds commit metadata
> in memory from ref walks automatically. However, packfiles must exist.

## Examples

### 1. Simple Streaming (`simple_streaming/`)

A minimal example demonstrating the streaming `Scan()` API with a `BlobScanner`. Scans every unique blob reachable from the commit history and prints periodic progress summaries.

**Functionality:**

-   Automatically locates the `.git` directory by traversing up the directory tree.
-   Opens the repository and scans all unique blobs using `scanner.Scan(nil, &summaryScanner{})`.
-   Displays progress every 50 blobs and detailed output for the first 10.
-   Shows blob OID, commit, path, and size for each reported blob.

**Execution:**

```bash
cd examples/simple_streaming
go run main.go
```

**Anticipated Output:**

```
Found .git directory at: /path/to/go-gitpack/.git
Scanning all unique blobs from repository history...
[1] blob 03693402  commit a1b2c3d4  path .gitignore  (245 bytes)
[2] blob 091ffc74  commit e5f6a7b8  path history_scanner.go  (8192 bytes)
...
[50] blob 0cc13063  commit c9d0e1f2  path tree.go  (4096 bytes)
[100] ...

Scan completed. Total blobs: 1234, total bytes: 5678901
```

### 2. History Scan (`history_scan/`)

A detailed example demonstrating `Scan()` with per-blob metadata output including OID, commit, path, and size.

**Functionality:**

-   Automatically locates the Git repository by traversing up the directory tree.
-   Scans every unique blob with detailed per-blob output (OID, commit, path, size).
-   Uses atomic counters for thread-safe accumulation across concurrent decode workers.

**Execution:**

```bash
cd examples/history_scan
go run main.go
```

**Expected Output:**

```
Opening Git repository at: /path/to/go-gitpack/.git
Scanning all unique blobs from repository history...

Blob #1:
  OID:    036934028e673710825b6a347a129558804b31a5
  Commit: a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0
  Path:   .gitignore
  Size:   245 bytes
  ---
Blob #2:
  ...

Processed 1234 blobs (5678901 bytes total)
```

### 3. Debug Simple (`debug_simple/`)

A minimal debugging example that exercises the `Scan()` API with a `BlobScanner`. Counts every blob reachable from commit history to verify the pack store, commit walker, and blob streaming pipeline work end-to-end.

**Functionality:**

-   Opens a `HistoryScanner` and calls `Scan(nil, scanner)` with a counting `BlobScanner`.
-   Implements the `BlobScanner` interface by draining each blob to `io.Discard` and incrementing a counter.
-   Passing `nil` as the `SeenSet` means every unique blob is scanned exactly once per run.

**Execution:**

```bash
cd examples/debug_simple
go run main.go
```

**Expected Output:**

```
Found .git directory at: /path/to/go-gitpack/.git
Scanner created successfully!
Successfully streamed 1234 blobs
```

### 4. Profiling Example (`profiling/`)

A performance profiling example that demonstrates how to diagnose memory and CPU performance issues when scanning large repositories using the built-in profiling capabilities of `HistoryScanner`.

**Functionality:**

-   Enables HTTP profiling server exposing pprof endpoints for real-time performance analysis.
-   Scans repository history using `Scan()` with a `BlobScanner` for memory-efficient processing.
-   Provides configurable profiling options via command-line flags.
-   Displays progress statistics during scanning including processing rate and throughput.
-   Supports execution tracing for detailed runtime analysis.

**Command-line Flags:**

-   `-repo`: Path to .git directory to scan (auto-detects if not specified)
-   `-profile`: Enable HTTP profiling server (default: true)
-   `-profile-addr`: Address for profiling HTTP server (default: ":6060")
-   `-trace`: Enable execution tracing (default: false)
-   `-trace-path`: Path for trace output (default: "./trace.out")

**Execution:**

```bash
cd examples/profiling
go run main.go

# With custom options:
go run main.go -repo /path/to/large/repo/.git -profile-addr :8080
```

**Capturing Profiles During Scan:**

While the profiling example is running, open another terminal and capture profiles:

```bash
# Capture a 30-second CPU profile
curl http://localhost:6060/debug/pprof/profile?seconds=30 > cpu.prof

# Capture heap (memory) profile
curl http://localhost:6060/debug/pprof/heap > heap.prof

# Capture goroutine profile
curl http://localhost:6060/debug/pprof/goroutine > goroutine.prof

# Analyze profiles
go tool pprof cpu.prof
go tool pprof heap.prof
```

## Key Features Demonstrated

This section provides code snippets that highlight key features of the `HistoryScanner`.

#### Creating a New HistoryScanner

```go
scanner, err := objstore.NewHistoryScanner(gitDir)
if err != nil {
    log.Fatalf("Failed to open repository: %v", err)
}
defer scanner.Close()
```

#### Streaming Blob Scan

```go
type counter struct {
    blobs atomic.Int64
}

func (c *counter) ScanBlob(r io.Reader, meta objstore.ScanMeta) error {
    _, err := io.Copy(io.Discard, r)
    if err == nil {
        c.blobs.Add(1)
    }
    return err
}

c := &counter{}
if err := scanner.Scan(nil, c); err != nil {
    log.Fatalf("streaming blob scan failed: %v", err)
}
fmt.Printf("Scanned %d blobs\n", c.blobs.Load())
```

`Scan` visits every unique blob exactly once in pack-offset order and passes the
full content to `ScanBlob`. The `ScanMeta` provides blob OID, introducing commit,
and file path. `ScanBlob` may be called from multiple goroutines concurrently, so
shared state must be synchronized (e.g. `sync/atomic` or a mutex).

#### Enabling Profiling

```go
// Create scanner with profiling configuration
scanner, err := objstore.NewHistoryScanner(gitDir,
    objstore.WithProfiling(&objstore.ProfilingConfig{
        EnableProfiling: true,
        ProfileAddr:     ":6060",
        Trace:           false,
        TraceOutputPath: "./trace.out",
    }),
)
```

This snippet shows how to enable profiling when creating a `HistoryScanner`. The `WithProfiling` option allows you to configure HTTP profiling server settings and execution tracing.

## Requirements

The examples require:
1. A Git repository with **packfiles**
2. The repository should have some commit history
3. Run from within or specify the path to the go-gitpack repository

## Repository Setup

Before running the examples, ensure your repository has packfiles:

```bash
# Check if you have packfiles
ls .git/objects/pack/
# Should show *.pack and *.idx files

# If missing, generate them:
git repack -Ad                        # Create packfiles
```

## Error Handling

Common errors you might encounter:

### Missing Dependencies
- `object X not found`: Usually means you need packfiles - run `git repack -Ad`

### Path Issues
- `Failed to open repository`: The specified path is not a valid Git repository
- `Could not find .git directory`: Run from within a Git repository

### Example Solution
```bash
# Complete setup for a new repository:
cd your-git-repo
git repack -Ad                        # Create packfiles from loose objects
cd path/to/go-gitpack/examples/simple_streaming
go run main.go                        # Should work now!
```
