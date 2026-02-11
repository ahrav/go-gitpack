# HistoryScanner Examples

This directory contains working examples demonstrating how to use the `HistoryScanner` from the go-gitpack library to stream Git commit history and analyze repository changes.

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

A minimal example demonstrating the streaming `DiffHistoryHunks()` API. Shows how to efficiently stream and process commit-history hunk additions.

**Functionality:**

-   Automatically locates the `.git` directory by traversing up the directory tree.
-   Opens the repository and streams all hunk additions using `DiffHistoryHunks()`.
-   Displays progress every 50 hunks and detailed content for the first 10.
-   Shows line-range and line-count information for each hunk.

**Execution:**

```bash
cd examples/simple_streaming
go run main.go
```

**Anticipated Output:**

```
Found .git directory at: /path/to/go-gitpack/.git
Streaming commit hunks from repository history...
[1] 03693402 added hunk in .gitignore (lines 33-34, 2 lines)
     Line 33: ...
     ... and 1 more lines
[2] 091ffc74 added hunk in history_scanner.go (lines 119-121, 3 lines)
     Line 119: ...
     ... and 2 more lines
...
[50] 0cc13063 added hunk in tree.go (lines 10-25, 16 lines)
[100] ...
...
Streaming completed successfully! Total hunks: 5432, total lines: 89123
```

### 2. Full History Scan (`history_scan/`)

A comprehensive example demonstrating `DiffHistoryHunks()` with a goroutine-based drain pattern and detailed per-hunk output.

**Functionality:**

-   Automatically locates the Git repository by traversing up the directory tree.
-   Streams hunk additions with detailed output (commit, file, line range, content preview).
-   Processes the entire history (configurable via the `maxHunks` constant).
-   Uses a separate goroutine to drain the hunk channel while the main goroutine waits on the error channel.

**Execution:**

```bash
cd examples/history_scan
go run main.go
```

**Expected Output:**

```
Opening Git repository at: /path/to/go-gitpack/.git
Starting to stream commit history hunks...
Press Ctrl+C to stop

Hunk #1:
  Commit:     036934028e673710825b6a347a129558804b31a5
  File:       .gitignore
  Start Line: 33
  End Line:   34
  Lines in hunk: 2
  Content (first 3 lines):
    Line 33: ...
  ---
Hunk #2:
  ...
Processed 5432 hunks
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
-   Streams repository history using `DiffHistoryHunks()` for memory-efficient processing.
-   Provides configurable profiling options via command-line flags.
-   Displays progress statistics during scanning including processing rate.
-   Supports execution tracing for detailed runtime analysis.
-   Allows limiting the number of hunks processed for controlled profiling sessions.

**Command-line Flags:**

-   `-repo`: Path to .git directory to scan (auto-detects if not specified)
-   `-profile`: Enable HTTP profiling server (default: true)
-   `-profile-addr`: Address for profiling HTTP server (default: ":6060")
-   `-trace`: Enable execution tracing (default: false)
-   `-trace-path`: Path for trace output (default: "./trace.out")
-   `-max-hunks`: Maximum number of hunks to process, 0 for all (default: 0)

**Execution:**

```bash
cd examples/profiling
go run main.go

# With custom options:
go run main.go -repo /path/to/large/repo/.git -profile-addr :8080 -max-hunks 10000
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

#### Streaming Diff History Hunks

```go
hunks, errs := scanner.DiffHistoryHunks()

go func() {
    for h := range hunks {
        fmt.Printf("Commit %s added hunk in %s (lines %d-%d)\n",
            h.Commit().String()[:8],
            h.Path(),
            h.StartLine(),
            h.EndLine())
    }
}()
if err := <-errs; err != nil {
    log.Fatal(err)
}
```

This snippet showcases the `DiffHistoryHunks()` function, which streams hunk additions and errors on separate channels. Each `HunkAddition` provides the commit hash, file path, line range, and the added lines. The hunk channel must be fully drained before reading the error channel.

#### Streaming Blob Scan

```go
type counter struct {
    blobs int
}

func (c *counter) ScanBlob(r io.Reader, meta objstore.ScanMeta) error {
    _, err := io.Copy(io.Discard, r)
    if err == nil {
        c.blobs++
    }
    return err
}

c := &counter{}
if err := scanner.Scan(nil, c); err != nil {
    log.Fatalf("streaming blob scan failed: %v", err)
}
fmt.Printf("Scanned %d blobs\n", c.blobs)
```

This snippet demonstrates streaming blob scan mode. Blob data is delivered to `ScanBlob` one object at a time.

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

## Understanding the Output

- **Commit**: The SHA-1 hash of the commit that introduced the hunk
- **File/Path**: The file path where lines were added (Unix-style forward slashes)
- **StartLine/EndLine**: The 1-based line range in the new version of the file
- **Lines**: The raw text content of each added line within the hunk

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
