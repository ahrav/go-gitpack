# 🔓 Under the Hood: Unpacking Git's Secrets

## Part 5: The Commit-Graph Advantage – Lightning-Fast History Traversal

In our journey through Git's internals, we've built a powerful object retrieval system. We can find any object using `.idx` files, navigate fragmented repositories with `.midx`, and inflate compressed data from packfiles. But we're about to hit a performance wall.

To find secrets in code, we need to walk through thousands, sometimes millions of commits, extracting only the lines that were added. For each commit, we need its parent relationships and root tree. The naive approach would decompress every commit object, parse it, extract the metadata, and move on.

Today, we'll explore Git's secret weapon for history traversal: the **commit-graph** file. This pre-computed index turns expensive commit lookups into lightning-fast array accesses, making repository-wide scans practical even on massive codebases.

---

## 📚 First, a Git Crash Course: Understanding the Playing Field

Before diving into commit graphs, let's clarify what we're actually traversing. Git stores everything as **objects**, and understanding the difference between them is crucial.

### The Four Object Types

```
┌─────────────────────────────────────────────────────┐
│                   Git Objects                       │
├─────────────┬───────────────┬──────────┬──────────┤
│    Blob     │     Tree      │  Commit  │   Tag    │
│   (Files)   │ (Directories) │ (History)│ (Labels) │
└─────────────┴───────────────┴──────────┴──────────┘
```

**Blobs** store file contents:
```
┌─────────────┐
│  README.md  │ → SHA: abc123...
│ "# My App"  │
└─────────────┘
```

**Trees** are directory snapshots:
```
┌─────────────────────────┐
│        Tree Object      │
├─────────────────────────┤
│ README.md  → abc123...  │ (blob)
│ src/       → def456...  │ (tree)
│ .gitignore → ghi789...  │ (blob)
└─────────────────────────┘
```

**Commits** track history and relationships:
```
┌──────────────────────────┐
│      Commit Object       │
├──────────────────────────┤
│ tree: xyz789...          │ ← Snapshot of entire project
│ parent: previous123...   │ ← What came before
│ author: Alice            │
│ message: "Fix bug #42"   │
└──────────────────────────┘
```

### The Critical Distinction: History vs State

- **Commits** = The history (who changed what when)
- **Trees** = The state (what files exist at that moment)

This distinction is why we need the commit-graph. To trace which lines were added in each commit, we must:
1. Walk the commit history (following parent pointers)
2. Compare each commit's tree with its parent's tree
3. Extract only the added lines

---

## 🏎️ The Performance Problem: Why Walking History Is Slow

Let's trace what happens when we walk history the traditional way:

```
Traditional Commit Walk (No Commit-Graph):
┌─────────────┐
│ Start: HEAD │
└──────┬──────┘
       ↓
┌─────────────────────────────────┐
│ 1. Read packfile index          │ O(log n)
│ 2. Seek to offset               │ O(1)
│ 3. Decompress with zlib         │ ~1ms
│ 4. Parse commit text            │ ~0.5ms
│ 5. Extract parent SHA & tree    │ ~0.1ms
└─────────────────────────────────┘
       ↓
   Repeat for each commit

For 1 million commits: ~1.6 seconds just for metadata!
```

**It's like having to open and read every letter in a filing cabinet when you only need to know who sent them.** The commit-graph pre-indexes all the "From:" addresses so you can skip straight to the information you need.

---

## 🗂️ Enter the Commit-Graph: Pre-computed History Metadata

The commit-graph file stores essential commit metadata in a hyper-efficient binary format:

```
With Commit-Graph:
┌─────────────┐
│ Start: HEAD │
└──────┬──────┘
       ↓
┌─────────────────────────────────┐
│ 1. SHA → Index lookup           │ O(1)
│ 2. Read from array              │ O(1)
│ 3. Done!                        │ ~0.001ms
└─────────────────────────────────┘

For 1 million commits: ~1ms total (1600x faster!)
```

### The Complete Binary Format

```
┌─────────────────────────────────────────┐
│            HEADER (8 bytes)             │
│  "CGPH" + version + chunk count         │
├─────────────────────────────────────────┤
│         CHUNK TABLE (12 bytes each)     │
│  [ChunkID] → [Offset in file]           │
├─────────────────────────────────────────┤
│              OIDF CHUNK                 │
│  Fan-out table for binary search        │
├─────────────────────────────────────────┤
│              OIDL CHUNK                 │
│  List of all commit SHA-1s              │
├─────────────────────────────────────────┤
│              CDAT CHUNK                 │
│  Commit metadata (parents, tree, time)  │
├─────────────────────────────────────────┤
│         EDGE CHUNK (optional)           │
│  Extra parents for octopus merges       │
└─────────────────────────────────────────┘
```

---

## 🔍 A Real-World Example: Parsing Complex Relationships

Let's parse a repository with various Git patterns to see how the commit-graph handles them:

```
                    ┌─────────────┐
                    │  Commit G   │ ← Octopus merge (3 parents!)
                    │ "Add login" │
                    └──────┬──────┘
                           │
    ┌──────────────────────┼───────────────────┐
    │                      │                   │
┌───▼─────┐         ┌──────▼──────┐     ┌──────▼──────┐
│Commit F │         │  Commit E   │     │  Commit H   │
│"Hotfix" │         │"Refactor"   │     │"Update docs"│
└────┬────┘         └──────┬──────┘     └──────┬──────┘
     │                     │                   │
     │              ┌──────▼──────┐            │
     │              │  Commit D   │ ← Merge    │
     │              │"Merge PR#1" │            │
     │              └───┬─────┬───┘            │
     │                  │     │                │
     │           ┌──────▼─┐ ┌─▼──────┐         │
     │           │Commit B│ │Commit C│         │
     │           │"Add API"│ │"Add UI"│        │
     │           └────┬────┘ └───┬────┘        │
     │                │          │             │
     │                └────┬─────┘             │
     │                     │                   │
     └─────────────────────┼───────────────────┘
                           │
                    ┌──────▼──────┐
                    │  Commit A   │ ← Root (no parents)
                    │  "Initial"  │
                    └─────────────┘
```

### How This Maps to Binary Data

```
OIDL Chunk (Commit List):
┌───────┬──────────────────────┐
│ Index │      Commit SHA      │
├───────┼──────────────────────┤
│   0   │ aaaaaa... (Commit A) │
│   1   │ bbbbbb... (Commit B) │
│   2   │ cccccc... (Commit C) │
│   3   │ dddddd... (Commit D) │
│   4   │ eeeeee... (Commit E) │
│   5   │ ffffff... (Commit F) │
│   6   │ gggggg... (Commit G) │
│   7   │ hhhhhh... (Commit H) │
└───────┴──────────────────────┘

CDAT Chunk (36 bytes per commit):
┌───────┬─────────────┬──────────┬──────────┬────────────┐
│ Index │  Tree SHA   │ Parent 1 │ Parent 2 │ Timestamp  │
├───────┼─────────────┼──────────┼──────────┼────────────┤
│   0   │ tree_a...   │   NONE   │   NONE   │ 1640000000 │
│   1   │ tree_b...   │    0     │   NONE   │ 1640001000 │
│   2   │ tree_c...   │    0     │   NONE   │ 1640002000 │
│   3   │ tree_d...   │    1     │    2     │ 1640003000 │
│   4   │ tree_e...   │    3     │   NONE   │ 1640004000 │
│   5   │ tree_f...   │    0     │   NONE   │ 1640005000 │
│   6   │ tree_g...   │    5     │0x80000000│ 1640006000 │
│   7   │ tree_h...   │    0     │   NONE   │ 1640007000 │
└───────┴─────────────┴──────────┴──────────┴────────────┘
                                      ↓
                              What's this magic number?
```

---

## 🎯 The Clever Bit: Encoding Variable Parents with Parent 2

Most commits have 0-2 parents, but Git supports unlimited parents through "octopus merges." How do we store this efficiently without wasting space?

### The Parent 2 Field: Three Modes in One

The Parent 2 field is "overloaded" with three possible meanings:

```
Mode 1: No Second Parent
Parent 2 = 0x70000000 (GRAPH_PARENT_NONE)

Mode 2: Direct Parent Index
Parent 2 = 0x00000000 to 0x6FFFFFFF (regular index)

Mode 3: EDGE Pointer (for 3+ parents)
Parent 2 = 0x80000000 | offset (high bit set)
```

### Understanding the Bit Magic

Let's decode these hex values:

```
0x70000000 = 0111 0000 0000 0000 0000 0000 0000 0000
             ↑
             Special "no parent" marker

0x80000000 = 1000 0000 0000 0000 0000 0000 0000 0000
             ↑
             High bit = 1 means "EDGE pointer"

0x7FFFFFFF = 0111 1111 1111 1111 1111 1111 1111 1111
             ↑
             Mask to extract lower 31 bits
```

When parsing Parent 2:
```go
if p2raw == 0x70000000 {
    // No second parent
} else if p2raw & 0x80000000 != 0 {
    // High bit set → EDGE pointer
    edgeOffset := p2raw & 0x7FFFFFFF  // Clear high bit
    // Read additional parents from EDGE[edgeOffset]
} else {
    // Regular parent index
    parentIndex := p2raw
}
```

### The EDGE Chunk: Storing Extra Parents

For our octopus merge G with parents [F, E, H]:

```
CDAT says:
- Parent 1: 5 (Commit F)
- Parent 2: 0x80000000 (EDGE pointer to offset 0)

EDGE Chunk:
┌───────┬─────────────────────────┐
│ Index │         Value           │
├───────┼─────────────────────────┤
│   0   │ 4 (parent: Commit E)    │
│   1   │ 0x80000007 (Commit H + LAST flag) │
└───────┴─────────────────────────┘

The high bit in EDGE entries means "last parent"
```

**It's like shipping labels on packages**: Every label has space for **sender + recipient** (2 fields). When shipping a single box, that's all you need. But when shipping a pallet with 50 boxes to different locations, the label shows **sender + QR code**. The sender is still printed directly, but scanning the QR code pulls up the full list of all recipient addresses. In Git's case, Parent 1 is always stored directly, and Parent 2 either holds another parent OR a QR code (EDGE pointer) to the full list.

---

## 🔗 Handling Split Chains: Incremental Updates at Scale

Large repositories don't rewrite the entire commit-graph when new commits arrive. Instead, they use **split chains**:

```
                Newest commits
                     ↓
┌─────────────────────────────┐
│   graph-abc123.graph (New)  │ ← Layer 0: This week's commits
│   Commits 10,000 - 10,500   │
└──────────────┬──────────────┘
               │ references
┌──────────────▼──────────────┐
│   graph-def456.graph        │ ← Layer 1: Last month's commits
│   Commits 5,000 - 10,000    │
└──────────────┬──────────────┘
               │ references
┌──────────────▼──────────────┐
│   graph-ghi789.graph        │ ← Layer 2: Historical commits
│   Commits 0 - 5,000         │
└─────────────────────────────┘
```

### Cross-Layer Parent References

When a commit in a newer layer references a parent in an older layer:

```go
// Layer 0 (newest):
//   Commit X at index 0
//   Parent index: 4999 → Points to older layer!

// After merging layers:
//   Parent index = 4999 + 0 (offset) = global index 4999

func (g parsedGraph) resolveParentsInto(dst Parents, all []Hash, offset int) error {
    for i, oid := range g.oids {
        // Adjust parent indices by offset for chained files
        if p := g.p1[i]; p != graphParentNone {
            adjustedP := int(p) + offset
            ps = append(ps, all[adjustedP])
        }
        // ... handle p2 similarly
    }
}
```

---

## 🚀 The Implementation: From Theory to Practice

Here's how our complete parser brings it all together:

```go
func LoadCommitGraph(objectsDir string) (*CommitGraphData, error) {
    // 1. Discover graph files (chain or single)
    chain, err := discoverGraphFiles(objectsDir)
    if len(chain) == 0 {
        return nil, nil // No commit-graph present
    }

    // 2. Parse each layer
    var allOids []Hash
    var allTrees []Hash
    var allTimes []int64
    fileInfo := make([]parsedGraph, len(chain))

    for i, path := range chain {
        pg, err := parseGraphFile(path)
        if err != nil {
            // Clean up mmaps before returning
            for j := 0; j < i; j++ {
                fileInfo[j].mr.Close()
            }
            return nil, err
        }
        fileInfo[i] = pg
        allOids = append(allOids, pg.oids...)
        allTrees = append(allTrees, pg.trees...)
        allTimes = append(allTimes, pg.times...)
    }

    // 3. Build parent relationships
    parents := make(Parents, len(allOids))
    offset := 0
    for _, g := range fileInfo {
        g.resolveParentsInto(parents, allOids, offset)
        offset += len(g.oids)
    }

    // 4. Create O(1) lookup map
    oidToIndex := make(map[Hash]int, len(allOids))
    for i, oid := range allOids {
        if _, dup := oidToIndex[oid]; !dup {
            oidToIndex[oid] = i
        }
    }

    // 5. Close mmaps and return immutable structure
    for _, g := range fileInfo {
        g.mr.Close()
    }

    return &CommitGraphData{
        Parents:     parents,
        OrderedOIDs: allOids,
        TreeOIDs:    allTrees,
        Timestamps:  allTimes,
        OIDToIndex:  oidToIndex,
    }, nil
}
```

### Using the Parsed Data

With the commit-graph loaded, finding added lines for security scanning becomes trivial:

```go
func scanRepository(graphData *CommitGraphData, store *Store) {
    // Start from any commit
    commitSHA := "abc123..."

    for {
        // O(1) lookups all the way!
        idx := graphData.OIDToIndex[commitSHA]
        treeSHA := graphData.TreeOIDs[idx]
        parents := graphData.Parents[commitSHA]

        if len(parents) == 0 {
            break // Reached root
        }

        // Get parent tree for comparison
        parentIdx := graphData.OIDToIndex[parents[0]]
        parentTree := graphData.TreeOIDs[parentIdx]

        // Diff trees to find changes
        added := diffTrees(store, treeSHA, parentTree)
        scanForSecrets(added)

        // Continue with first parent
        commitSHA = parents[0]
    }
}
```

---

## 🎯 Key Takeaways

The commit-graph transforms Git's flexible but slow object model into a lightning-fast lookup table:

**Pre-computed Index**: Turns expensive object inflation into simple array lookups, achieving 1600x speedup for history traversal.

**Clever Encoding**: The Parent 2 field uses bit manipulation to handle unlimited parents without wasting space on the 99% case.

**Incremental Design**: Split chains allow efficient updates without rewriting the entire graph.

**Production-Ready**: Robust validation, cross-layer reference handling, and protection against malformed data.

**Performance at Scale**: O(1) parent lookups regardless of repository size—critical for scanning millions of commits.

The commit-graph is Git's secret weapon for high-speed history traversal. By pre-computing and efficiently encoding commit relationships, it makes repository-wide operations that would otherwise be impractical blazingly fast.

---

## 🔮 Up Next

We now have all the pieces for efficient repository traversal:
- Finding objects in packfiles (`.idx` and `.midx`)
- Inflating and reconstructing objects from compressed data
- Lightning-fast commit history navigation via commit-graph

With the commit-graph giving us instant access to commit metadata, we can now focus entirely on what matters: finding the differences between commits to identify exactly when secrets were introduced.

In **Part 6: Tree & Blob Diff Engine**, we'll build the final piece of our high-speed scanner:
- Comparing tree objects to identify changed files
- Implementing Myers' diff algorithm for precise line-by-line comparison
- Extracting only the "+" lines (additions) where secrets might be hiding
- Optimizing diff performance for massive repositories
- Handling Git's various edge cases and file modes

This is where our speed gains really shine - instead of letting Git compute diffs for the entire repository history, we'll compute targeted diffs only for the changes we care about, using our direct access to Git's object database.
