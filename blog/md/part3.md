# 🔓 Under the Hood: Unpacking Git's Secrets

## Part 3: One Index to Rule Them All – The Multi-Pack Index

In [Part 1](./part1.md), we set our goal: building a high-performance Git scanner. In [Part 2](./part2.md), we mastered the `.idx` file, learning how to find any object in a **single** packfile in microseconds. But real-world repositories rarely have just one pack.

Today, we solve the next major bottleneck: efficiently searching across **hundreds** of packfiles at once using the **multi-pack-index (`.midx`)**.

---

## 📦 The Performance Trap: Why Many Packs Slow Us Down

Git constantly creates new packfiles. Operations like `git gc`, fetching updates from a remote, or regular development activity cause repositories to accumulate them over time:

- A fresh repository might have **1 packfile**
- After a few months of active work: **dozens**
- A large, mature repository: **hundreds**

### The Sequential Search Problem

Without a better strategy, finding an object requires searching every single `.idx` file sequentially until we get a hit:

```
Without MIDX: Search Each Pack Individually
┌─────────┐    ┌─────────┐    ┌─────────┐         ┌─────────┐
│ Pack 1  │ → │ Pack 2  │ → │ Pack 3  │ → ... → │ Pack 150│
│ .idx    │   │ .idx    │   │ .idx    │         │ .idx    │
└─────────┘   └─────────┘   └─────────┘         └─────────┘
     ↓             ↓             ↓                     ↓
   Miss          Miss          Miss                  Hit!

Performance: O(P × log M) where P = packs, M = objects per pack
Worst case: Search all 150 pack indexes sequentially
```

This degrades our lookup performance from fast logarithmic search `O(log M)` to slow linear scan `O(P × log M)` across all packs.

### The MIDX Solution: Unified Index

Git's solution is the **multi-pack-index (`.midx`)** file. It acts as a single, consolidated index for multiple packfiles:

```
With MIDX: Single Unified Search
┌─────────────────────────────────────────────────────────────┐
│                    Multi-Pack Index                         │
│  Contains unified view of all objects across all packs     │
└─────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────┐
│ Result: Pack 47, Offset 12,847,293                         │
└─────────────────────────────────────────────────────────────┘
                                ↓
              Jump directly to Pack 47

Performance: O(log N) where N = total objects across all packs
Best case: Single search finds object regardless of pack count
```

---

## 🏗️ MIDX Anatomy: Same Principles, Bigger Scale

The structure of a `.midx` file is similar to the `.idx` file we know, but adds indirection to manage multiple packs. Looking at the actual implementation:

### Complete MIDX File Structure

```go
// midxFile represents the parsed multi-pack-index
type midxFile struct {
    version byte  // MIDX version (1 or 2)

    // Memory-mapped pack handles in PNAM order
    packReaders []*mmap.ReaderAt
    packNames   []string

    // Same fan-out optimization as .idx files
    fanout [fanoutEntries]uint32

    // Parallel arrays for all objects across all packs
    objectIDs []Hash        // sorted SHA-1 hashes
    entries   []midxEntry   // pack ID + offset + CRC
}

type midxEntry struct {
    packID uint32  // index into packReaders slice
    offset uint64  // byte position within that pack
    crc    uint32  // CRC-32 checksum (v2 only)
}
```

### File Layout
```
Multi-Pack Index (.midx) File Layout:
┌─────────────────────────────────────────────────────────────┐
│                 HEADER & CHUNK DIRECTORY                    │
│              Magic "MIDX" + Version + Chunk Locations      │
├─────────────────────────────────────────────────────────────┤
│                    PNAM CHUNK                               │
│          NUL-terminated pack filenames                     │
├─────────────────────────────────────────────────────────────┤
│                 OIDF CHUNK (Fan-Out)                       │
│              256 entries × 4 bytes each                     │
├─────────────────────────────────────────────────────────────┤
│                 OIDL CHUNK (Object IDs)                    │
│            All SHA-1s from all packs, sorted               │
├─────────────────────────────────────────────────────────────┤
│                 OOFF CHUNK (Object Offsets)                │
│            Pack ID + Offset for each object                │
├─────────────────────────────────────────────────────────────┤
│                 LOFF CHUNK (Large Offsets)                 │
│              For >2GB packfiles (optional)                 │
├─────────────────────────────────────────────────────────────┤
│                 CRCS CHUNK (CRC32s)                        │
│               Checksums for integrity (v2)                 │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

**Header and Chunk Directory**
```go
// From parseMidx - reading the MIDX header
var hdr [12]byte
if _, err := mr.ReadAt(hdr[:], 0); err != nil {
    return nil, err
}
if !bytes.Equal(hdr[0:4], []byte("MIDX")) {
    return nil, fmt.Errorf("not a MIDX file")
}

version := hdr[4]
chunks := int(hdr[6])
packCount := int(binary.BigEndian.Uint32(hdr[8:12]))
```

**PNAM (Pack Names)**
```go
// Multi-pack index chunk identifiers from the actual code
const (
    chunkPNAM = 0x504e414d // 'PNAM' - pack names
    chunkOIDF = 0x4f494446 // 'OIDF' - object ID fanout table
    chunkOIDL = 0x4f49444c // 'OIDL' - object ID list
    chunkOOFF = 0x4f4f4646 // 'OOFF' - object offsets
    chunkLOFF = 0x4c4f4646 // 'LOFF' - large object offsets
    chunkCRCS = 0x43524353 // 'CRCS' - CRC-32 checksums (v2)
)

// Pack names are NUL-terminated strings
var packNames []string
for i, start := 0, 0; i < packCount; i++ {
    end := bytes.IndexByte(pn[start:], 0)
    if end < 0 {
        return nil, fmt.Errorf("unterminated PNAM entry")
    }
    packNames = append(packNames, string(pn[start:start+end]))
    start += end + 1
}
```

**OIDF & OIDL (Object ID Fan-Out & List)**
You might notice the `unsafe` package here. We're using it to directly cast our byte slices into slices of `uint32` or `Hash` types. This avoids copying data from the memory-mapped region into Go's memory, giving us a zero-copy read for maximum performance. It's a powerful, but delicate, optimization.

```go
// Same fan-out optimization as .idx files
var fanout [fanoutEntries]uint32
if _, err = mr.ReadAt(unsafe.Slice((*byte)(unsafe.Pointer(&fanout[0])), fanoutSize), fanOff); err != nil {
    return nil, err
}

objCount := fanout[255]

// Combined object list from all packs
oids := make([]Hash, objCount)
read := unsafe.Slice((*byte)(unsafe.Pointer(&oids[0])), int(objCount*hashSize))
if _, err = mr.ReadAt(read, oidOff); err != nil {
    return nil, err
}
```

**OOFF (Object Offsets) - The Key Innovation**
```go
// midxEntry describes a single object in the MIDX
type midxEntry struct {
    packID uint32  // index into packReaders slice
    offset uint64  // byte position within that pack
    crc    uint32  // CRC-32 checksum (v2 only)
}

// Parse offset entries with pack ID + offset pairs
entries := make([]midxEntry, objCount)
for i := range objCount {
    packID := binary.BigEndian.Uint32(offRaw[i*8 : i*8+4])
    rawOff := binary.BigEndian.Uint32(offRaw[i*8+4 : i*8+8])

    var off64 uint64
    if rawOff&0x80000000 == 0 {
        off64 = uint64(rawOff)
    } else {
        // Large offset - index into LOFF table
        idx := rawOff & 0x7FFFFFFF
        off64 = loff[idx]
    }

    entries[i] = midxEntry{
        packID: packID,
        offset: off64,
    }
}
```

---

## ⚡ The Lookup: Single Search, Multiple Packs

The lookup process using a `.midx` file is nearly identical to Part 2, with one key addition:

### Step-by-Step Process

**1. Fan-Out Lookup**
```go
// From the actual midxFile.findObject implementation
func (m *midxFile) findObject(h Hash) (p *mmap.ReaderAt, off uint64, ok bool) {
    first := h[0]
    start := uint32(0)
    if first > 0 {
        start = m.fanout[first-1]
    }
    end := m.fanout[first]
    if start == end {
        return nil, 0, false
    }

    // Search space narrowed across ALL packs
    searchSpace := end - start
}
```

**2. Binary Search in Unified Object List**
```go
// Binary search within the narrow slice of ALL objects
rel, hit := slices.BinarySearchFunc(
    m.objectIDs[start:end],
    h,
    func(a, b Hash) int { return bytes.Compare(a[:], b[:]) },
)
if !hit {
    return nil, 0, false
}

abs := int(start) + rel
ent := m.entries[abs]
```

**3. The Payoff - Direct Pack Access**
```go
// Use Pack ID to get the correct packfile
packReader := m.packReaders[ent.packID]
offset := ent.offset

// Return the pack handle and offset for immediate use
return packReader, offset, true
```

The result: same performance as single-pack lookup, but works across hundreds of packs.

---

## 🚀 Implementation: Pack Handle Management

Looking at the actual Store implementation, pack handles are managed efficiently during initialization:

### Store Structure
```go
type Store struct {
    // packs contains one immutable idxFile per mapped pack
    packs []*idxFile

    // midx holds the multi-pack index if one was found
    midx *midxFile

    // packMap tracks unique mmap handles to prevent duplicate mappings
    packMap map[string]*mmap.ReaderAt

    // Other fields, including our advanced ARC cache for hot objects
    cache         *arc.ARCCache[Hash, []byte]
    maxDeltaDepth int
    VerifyCRC     bool
}
```

### MIDX Pack Management
```go
// From parseMidx - handles are opened once and reused
func parseMidx(dir string, mr *mmap.ReaderAt, packCache map[string]*mmap.ReaderAt) (*midxFile, error) {
    // ... parse PNAM section to get pack names ...

    // Memory-map every pack for stable ReaderAt slice
    packs := make([]*mmap.ReaderAt, len(packNames))
    for i, name := range packNames {
        p := filepath.Join(dir, name)
        if h, ok := packCache[p]; ok {
            packs[i] = h  // Reuse existing handle
            continue
        }
        r, err := mmap.Open(p)
        if err != nil {
            return nil, fmt.Errorf("mmap pack %q: %w", name, err)
        }
        packs[i] = r
        packCache[p] = r  // Cache for future use
    }

    return &midxFile{
        packReaders: packs,
        packNames:   packNames,
        // ... other fields
    }, nil
}
```

### How Object Lookup Uses Cached Handles
```go
// The Store.Get method leverages both MIDX and individual packs
func (s *Store) Get(oid Hash) ([]byte, ObjectType, error) {
    // Try MIDX first for unified lookup
    if s.midx != nil {
        if p, off, ok := s.midx.findObject(oid); ok {
            return s.inflateFromPack(p, off, oid, ctx)
        }
    }

    // Fall back to individual pack lookups
    for _, pf := range s.packs {
        if offset, found := pf.findObject(oid); found {
            return s.inflateFromPack(pf.pack, offset, oid, ctx)
        }
    }

    return nil, ObjBad, fmt.Errorf("object %x not found", oid)
}
```

---

## 🎯 Key Takeaways

The multi-pack-index (`.midx`) solves the pack fragmentation problem elegantly:

**Unified Access**: Single search across all packs, regardless of repository fragmentation.

**Familiar Structure**: Uses the same fan-out optimization as `.idx` files, just at a larger scale.

**Efficient Indirection**: Pack ID + Offset system provides direct access without redundant data.

**Scalable Performance**: Maintains O(log n) lookup performance regardless of pack count.

The MIDX transforms what could be a crippling performance problem into a negligible overhead, maintaining Git's signature speed even in massive, highly-fragmented repositories.

---

## 🔮 Up Next

We've achieved true random access. We can now take any SHA-1 hash and, in microseconds, identify exactly which packfile it lives in and where, regardless of repository size or fragmentation.

Now it's time to read the actual data. In **Part 4**, we'll crack open the packfile itself. We'll learn:

- How Git uses zlib compression for storage efficiency
- How delta compression stores objects as changes against other objects
- A single-pass, stack-based algorithm for resolving delta chains on the fly
- Building a complete object reader that handles all Git's storage optimizations

We're getting closer to our final goal: lightning-fast diffs that only show the lines we need to scan for secrets.
