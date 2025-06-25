# 🔓 Under the Hood: Unpacking Git's Secrets

## Part 1: The Quest for a Faster Scanner

When you run a secret-scanner over a Git repository, you're looking for two things:

**Speed**: So your scans finish in minutes, not hours.
**Precision**: So you only examine the lines that were added (the "+" lines), pinpointing exactly when a secret was introduced.

Our matching engine is already quite fast, but on massive monorepos, the overall run-time is dominated by one innocent-looking command:

```bash
git log -p --all
```

### The Inefficiency Problem

When you run `git log -p --all`, you're forcing Git to walk through every commit, decompress every object, and compute a diff just to print a human-friendly patch. We then discard about half of that output (the "-" lines) and pipe the rest into our scanner.

This is inefficient. The core problem is that we're asking Git to act as a middle-man when we could be accessing its underlying database directly.

**Spoiler alert**: We can, and it's surprisingly straightforward once you understand how Git stores its data.

---

## 🏗️ A New Foundation: Git's Core Components

Before diving into the low-level storage, let's understand the building blocks of any Git repository. Git is essentially a content-addressable filesystem where every piece of data is an "object" identified by a unique SHA-1 hash.

### The Four Fundamental Object Types

#### Blobs: The Raw Content
```
Blob SHA: 5d41402a...
Content: "Hello, World!"
```

Blobs are the raw contents of your files, stripped of any metadata like filename or path. If you rename a file, Git doesn't need to store the content again; it just points to the existing blob.

#### Trees: The Directory Structure
```
Tree SHA: 4b825dc6...
├── 100644 blob a1b2c3d4... README.md
├── 100644 blob e5f6g7h8... package.json
└── 040000 tree i9j0k1l2... src/
    ├── 100644 blob m3n4o5p6... index.js
    └── 100644 blob q7r8s9t0... utils.js
```

A tree object represents a directory. It's a list of entries, each containing a file mode, object type, SHA-1 hash, and filename. By nesting trees within trees, Git builds a complete snapshot of your project's directory structure.

#### Commits: The Complete Snapshots
```
Commit SHA: 2c3d4e5f...
├── tree: 4b825dc6... (points to root directory)
├── parent: 1a2b3c4d... (previous commit)
├── author: Alice <alice@example.com>
├── committer: Alice <alice@example.com>
└── message: "Add user authentication feature"
```

A commit ties everything together. It contains a pointer to a top-level tree representing your project state, pointers to parent commits forming the historical chain, author/committer metadata, and the commit message.

### The Git Graph: A Family Tree of Changes

These objects link together to form what we call a **Directed Acyclic Graph (DAG)**:

```
    A ← B ← C ← D (main branch)
         ↖
          E ← F (feature branch)
```

- Each letter represents a commit
- Arrows point to parent commits
- Branches can split (B→E) and merge back together
- You can never have circular references

---

## 📦 The Packed Object Store: Git's Efficiency Engine

Storing millions of tiny object files is inefficient. Git solves this by bundling loose objects into `.pack` files during garbage collection.

### Key Components

| File Type | Purpose |
|-----------|---------|
| `*.pack` | Compressed storage of thousands of objects as deltas |
| `*.idx` | Quick lookup table for object locations |
| `*.midx` | Master index across multiple pack files |
| `*.rev/.ridx` | Reverse mapping from pack locations to SHA-1 hashes |

### Delta Compression

Instead of storing similar files separately:

```
Version 1: "Hello, World!"
Version 2: "Hello, World! Updated"
Version 3: "Hello, World! Final version"
```

Git stores:
```
Base: "Hello, World!"
Delta 1: +9 " Updated"
Delta 2: -8 "Updated" +13 "Final version"
```

This dramatically reduces storage requirements by only storing the differences between versions.

---

## ⚡ The Secret Weapon: The Commit-Graph File

Even with fast object lookups, walking through repository history can be slow. To get the parents of a commit, you'd typically need to decompress the commit object itself.

### The Problem
```
To find commit A's parents:
1. Look up commit A's SHA in the index
2. Decompress the full commit object
3. Parse the text to find parent SHAs
4. Repeat for each parent...
```

With millions of commits, this becomes a bottleneck.

### The Solution: Pre-computed Metadata
```
commit-graph file:
┌─────────────────────────────────────┐
│ Commit SHA | Parents | Root Tree    │
├─────────────────────────────────────┤
│ abc123...  | def456  | 789abc...   │
│ def456...  | ghi789  | bcd123...   │
│ ghi789...  | (none)  | efg456...   │
└─────────────────────────────────────┘
```

The commit-graph file stores commit metadata in a compact binary format, allowing us to walk the commit graph without decompressing commit objects.

---

## 🚀 The 5-Step Express Lane from Hash to Diff

Instead of the slow `git log` approach, we'll build a focused pipeline where each stage does one optimized task:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   SHA-1     │ →  │ Pack File   │ →  │ Raw Object  │
│   Hash      │    │ Location    │    │ Bytes       │
└─────────────┘    └─────────────┘    └─────────────┘
      ↓                    ↓                    ↓
 1. Index Lookup    2. Inflate/Delta     3. Parse Object
   (O(1) time)        Resolution

┌─────────────┐    ┌─────────────┐
│ Structured  │ →  │ Diff Lines  │
│ Data        │    │ (+ only)    │
└─────────────┘    └─────────────┘
      ↓                    ↓
4. Tree Traversal    5. Myers Diff
   (commit-graph)      Algorithm
```

### Step-by-Step Breakdown

**1. Index Lookup**
```
Input:  SHA-1 "a1b2c3d4..."
Output: Pack file "pack-xyz.pack", offset 12345
Time:   O(1) - binary search in sorted index
```

**2. Inflate/Delta Resolution**
```
Read bytes at offset 12345 → "∆base_sha +5 Hello"
Resolve delta chain → "Hello, World!"
Decompress with zlib → actual file content
```

**3. Commit-Graph Traversal**
```
Instead of: Parse full commit object (slow)
We use:    Pre-computed metadata (fast)
Result:    Parent SHAs + root tree SHA
```

**4. Semantic Parsing**
```
Raw bytes: "tree abc123\nparent def456\nauthor..."
Parsed:    {tree: "abc123", parents: ["def456"], author: ...}
```

**5. Tree & Blob Diff**
```
Tree A: {README.md: sha1, src/: sha2}
Tree B: {README.md: sha1, src/: sha3, .env: sha4}
Diff:   Added .env file, modified src/ directory
Result: Only the + lines that contain potential secrets
```

---

## 🛣️ Updated Roadmap for the Series

| Part | What We'll Build | Key Focus |
|------|------------------|-----------|
| **2** | Pack Index Parser | Building lightning-fast object lookups |
| **3** | Multi-Pack-Index Support | Scaling across multiple pack files |
| **4** | Packfile Reader & Delta Resolver | Reconstructing objects from compressed deltas |
| **5** | The Commit-Graph Advantage | Leveraging pre-computed commit metadata |
| **6** | Commit/Tree Parsing | Efficiently parsing Git's object formats |
| **7** | Commit-Graph Walker | Traversing repository history optimally |
| **8** | Tree & Blob Diff | Computing precise change detection |
| **9** | Pipeline Integration | Bringing it all together with benchmarks |

### Expected Performance Gains

Based on our approach, here's what you can expect:

```
Traditional approach (git log -p --all):
├── Large repo (1M commits): ~45 minutes
├── Memory usage: ~2GB
└── CPU efficiency: ~15% (lots of waiting)

Our optimized approach:
├── Large repo (1M commits): ~3 minutes
├── Memory usage: ~200MB
└── CPU efficiency: ~85% (purpose-built pipeline)

Performance improvement: ~15x faster!
```

---

## 🎯 Why This Matters

This blog series teaches you Git internals while building a faster secret scanner. By implementing these components from scratch, you'll understand Git's internal architecture and how it achieves its performance.

You'll learn not just what Git does, but precisely how and why it's so fast, giving you the knowledge to build high-performance tools that work directly with Git's data structures.

---

## 🔮 Up Next

In Part 2, we'll crack open the `.idx` format, build a lightning-fast lookup table, and write the very first line of Go that peeks inside a packfile.

We'll start by exploring questions like:
- How does Git find a needle (specific SHA-1) in a haystack (millions of objects) in constant time?
- What's the clever binary format that makes this possible?
- How can we replicate Git's magic in our own code?

Stay tuned, and happy (secret) hunting!
