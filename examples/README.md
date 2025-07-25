# HistoryScanner Examples

This directory contains working examples demonstrating how to use the `HistoryScanner` from the go-gitpack library to stream Git commit history and analyze repository changes.

## Prerequisites

- Go 1.24.4 or later
- A Git repository with:
  - **Commit-graph files** (required)
  - **Packfiles** (required - the scanner doesn't work with loose objects only)

⚠️ **Important Setup**: Before running the examples, ensure your repository has both packfiles and commit-graph files:

```bash
# Generate packfiles (consolidate loose objects)
git repack -Ad

# Generate commit-graph file
git commit-graph write --reachable
```

## Examples

### 1. Simple Streaming (`simple_streaming/`)

A minimal example demonstrating the basic streaming functionality of `DiffHistory()`. This example showcases how to efficiently stream and process commit history additions.

**Functionality:**

-   Automatically locates the `.git` directory by traversing up the directory tree.
-   Opens the repository and streams all commit additions using `DiffHistory()`.
-   Displays progress updates every 100 additions.
-   Outputs content for the first 5 additions.

**Execution:**

```bash
cd examples/simple_streaming
go run main.go
```

**Anticipated Output:**

```
Found .git directory at: /path/to/go-gitpack/.git
🚀 Streaming commit additions from repository history...
[1] 03693402 added line 33 in .gitignore
     Content:
[2] 03693402 added line 34 in .gitignore
     Content: *.pprof
[3] 091ffc74 added line 119 in history_scanner.go
     Content:
[4] 091ffc74 added line 120 in history_scanner.go
     Content:   // meta provides cached access to commit author/committer metadata
[5] 091ffc74 added line 121 in history_scanner.go
     Content:   meta *metaCache
[6] 091ffc74 added line 167 in history_scanner.go
...
[100] 0cc13063 added line 20 in tree.go
[200] 0cc13063 added line 120 in tree.go
...
✅ Streaming completed successfully! Total additions: 33490
```

### 2. Full History Scan (`history_scan/`)

A comprehensive example demonstrating various `HistoryScanner` capabilities, including streaming diff history, loading all commits, and retrieving commit timestamps.

**Functionality:**

-   Automatically locates the Git repository by traversing up the directory tree.
-   Streams diff history with detailed output for each addition.
-   Limits output to 100 additions for demonstration purposes.
-   Demonstrates additional scanner capabilities like `LoadAllCommits()` and `Timestamp()`.
-   Displays repository statistics, including the total number of commits and the first 5 commit details.

**Execution:**

```bash
cd examples/history_scan
go run main.go
```

**Expected Output:**

```
Opening Git repository at: /path/to/go-gitpack/.git
Starting to stream commit history additions...
Press Ctrl+C to stop

Addition #1:
  Commit:  036934028e673710825b6a347a129558804b31a5
  File:    .gitignore
  Line:    33
  Content:
  ---
Addition #2:
  Commit:  036934028e673710825b6a347a129558804b31a5
  File:    .gitignore
  Line:    34
  Content: *.pprof
  ---
...
✅ Processed 100 additions

--- Additional Scanner Capabilities ---
📊 Total commits in repository: 124
📝 First 5 commits:
  015f03ca0c3b9c7dc4a243dadc3b9b3ddd52ba04 (tree: ae8a8ded410f39fe73c4aabf5376525a1b84925e, parents: 1, time: 1750305611)
  036934028e673710825b6a347a129558804b31a5 (tree: 356252abdd812613b9205ef6b0214d7040e6b4fb, parents: 1, time: 1750302486)
  ...
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

This code demonstrates how to create a new `HistoryScanner` instance. The `NewHistoryScanner` function takes the path to a Git repository as input and returns a `HistoryScanner` instance. It is crucial to close the scanner using `defer scanner.Close()` to release resources.

#### Streaming Diff History

```go
additions, errors := scanner.DiffHistory()

for {
    select {
    case addition, ok := <-additions:
        if !ok {
            return // Channel closed
        }
        // Process addition
        fmt.Printf("Commit %s added line %d in %s\n",
            addition.Commit().String()[:8],
            addition.Line(),
            addition.Path())

    case err := <-errors:
        if err != nil {
            log.Fatalf("Error: %v", err)
        }
        return // Completed successfully
    }
}
```

This snippet showcases the `DiffHistory()` function, which streams commit additions and errors. The `select` statement concurrently processes additions and errors from the respective channels. Each addition provides details about the commit, line number, and file path where the change occurred.

#### Loading All Commits

```go
commits, err := scanner.LoadAllCommits()
if err != nil {
    log.Printf("Failed to load commits: %v", err)
} else {
    fmt.Printf("Total commits: %d\n", len(commits))
    for _, commit := range commits {
        fmt.Printf("Commit: %s, Tree: %s, Parents: %d\n",
            commit.OID, commit.TreeOID, len(commit.ParentOIDs))
    }
}
```

The `LoadAllCommits()` function loads all commits in the repository. This function is useful for analyzing the entire commit history. The code iterates through the commits and prints details such as the commit OID, tree OID, and the number of parent commits.

#### Getting Metadata

```go
// Get commit timestamp
if timestamp, hasTimestamp := scanner.Timestamp(commitOID); hasTimestamp {
    fmt.Printf("Commit time: %d\n", timestamp)
}

// Get author information
if author, err := scanner.Author(commitOID); err == nil {
    fmt.Printf("Author: %s <%s>\n", author.Name, author.Email)
}
```

This example demonstrates how to retrieve commit metadata, such as the timestamp and author information. The `Timestamp()` function retrieves the commit timestamp, and the `Author()` function retrieves the author's name and email.

## Understanding the Output

- **Commit**: The full SHA-1 hash of the commit that introduced the line
- **File**: The file path where the line was added (using Unix-style forward slashes)
- **Line**: The 1-based line number in the new version of the file
- **Content**: The raw text content of the added line

## Requirements

The examples require:
1. A Git repository with **both** commit-graph files and packfiles
2. The repository should have some commit history
3. Run from within or specify the path to the go-gitpack repository

## Repository Setup

Before running the examples, ensure your repository is properly configured:

```bash
# Check if you have packfiles
ls .git/objects/pack/
# Should show *.pack and *.idx files

# Check if you have a commit-graph
ls .git/objects/info/commit-graph
# Should exist

# If missing, generate them:
git repack -Ad                        # Create packfiles
git commit-graph write --reachable    # Create commit-graph
```

## Performance Notes

- The streaming approach processes commits concurrently across multiple CPU cores
- Memory usage is kept low by processing additions as they're found rather than collecting them all in memory
- The examples limit output for demonstration purposes, but the scanner can handle repositories with thousands of commits efficiently
- The go-gitpack repository example processed **33,490 total additions** across **124 commits**

## Error Handling

Common errors you might encounter:

### Missing Dependencies
- `commit-graph required but not found`: Run `git commit-graph write --reachable`
- `object X not found`: Usually means you need packfiles - run `git repack -Ad`

### Path Issues
- `Failed to open repository`: The specified path is not a valid Git repository
- `Could not find .git directory`: Run from within a Git repository

### Example Solution
```bash
# Complete setup for a new repository:
cd your-git-repo
git repack -Ad                        # Create packfiles from loose objects
git commit-graph write --reachable    # Generate commit-graph
cd path/to/go-gitpack/examples/simple_streaming
go run main.go                        # Should work now!
```
