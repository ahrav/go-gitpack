# 🔓 Under the Hood: Unpacking Git's Secrets

## Part 2: The Pack Index — Git's High-Speed Access Engine

In [Part 1](./part1.md), we established our goal: to bypass the slow `git log` command by building our own Git engine for secret scanning. We learned that this requires fast, random access to Git's object database.

Today, we build the component that makes this speed possible: the pack index (`.idx`) file. This is the secret to turning a slow, sequential repository scan into a lightning-fast lookup.

---

## 📦 The Warehouse Problem: Why an Index Is Non-Negotiable

A Git **packfile** (`.pack`) is one enormous binary file where millions of compressed objects—commits, trees, and blobs, are concatenated together. Without an index, finding a specific object requires a linear scan through every object until you find the right one.

### The Sequential Search Problem

```
Without Index: Linear Search Through Packfile
┌─────────────────────────────────────────────────────────────────┐
│ Object1 │ Object2 │ ... │ Object847,293 │ ... │ Object2,000,000 │
└─────────────────────────────────────────────────────────────────┘
    ↑                                              ↑
Start here                           Looking for this object?
                                    Must check every single one!

Time Complexity: O(n) - Gets slower as repo grows
Typical Search Time: Minutes to hours for large repos
```

### The Index Solution: Direct Access

The **pack index** (`.idx`) file is the master inventory for the packfile. For every object's SHA-1 hash, the index tells us its exact byte offset within the packfile.

```
With Index: Direct Access
┌─────────────────────────────────────────────────────────────────┐
│ Object1 │ Object2 │ ... │ Object847,293 │ ... │ Object2,000,000 │
└─────────────────────────────────────────────────────────────────┘
                                    ↑
                          Jump directly here!

Index Lookup: SHA-1 "a1b2c3..." → Offset 12,847,293
Time Complexity: O(log n) - Stays fast regardless of repo size
Typical Search Time: Microseconds
```

---

## 🏗️ Anatomy of a Perfect Lookup: The .idx File Format

A modern Git pack index (version 2) is a masterclass in file format design. It's not just a simple list; it's a multi-stage data structure where each layer is designed to filter the search space, ensuring lookups are incredibly fast.

### The Complete Structure

```
Pack Index (.idx) File Layout:
┌─────────────────────────────────────────────────────────────────┐
│                    HEADER (8 bytes)                            │
│                Magic + Version                                  │
├─────────────────────────────────────────────────────────────────┤
│                 FAN-OUT TABLE (1024 bytes)                     │
│              256 entries × 4 bytes each                        │
├─────────────────────────────────────────────────────────────────┤
│                 SHA-1 TABLE (N × 20 bytes)                     │
│                 All object hashes, sorted                      │
├─────────────────────────────────────────────────────────────────┤
│                CRC-32 TABLE (N × 4 bytes)                      │
│              Checksums for data integrity                      │
├─────────────────────────────────────────────────────────────────┤
│                OFFSET TABLE (N × 4 bytes)                      │
│            Where each object lives in .pack                    │
├─────────────────────────────────────────────────────────────────┤
│           LARGE OFFSET TABLE (M × 8 bytes)                     │
│              For >2GB packfiles (optional)                     │
├─────────────────────────────────────────────────────────────────┤
│                   TRAILER (40 bytes)                           │
│            Checksums for .pack and .idx files                  │
└─────────────────────────────────────────────────────────────────┘
```

Let's break down each component:

### Header: File Identification

```
Bytes 0-7: Header
├── Magic Number: \377tOc (4 bytes)
└── Version: 2 (4 bytes)
```

Acts as a sanity check. If these bytes aren't correct, we know we're reading the wrong file type and can fail immediately.

### The Fan-Out Table: The Core Optimization

This is the index's most important performance feature. Instead of searching through all objects, the fan-out table lets us narrow the search space dramatically.

```
Fan-Out Table Structure:
┌─────────┬─────────┬─────────┬─────┬─────────┐
│ Byte 00 │ Byte 01 │ Byte 02 │ ... │ Byte FF │
├─────────┼─────────┼─────────┼─────┼─────────┤
│   247   │   501   │   889   │ ... │2,000,000│
└─────────┴─────────┴─────────┴─────┴─────────┘

Each entry shows: "How many objects have first byte ≤ this value"
```

**Cumulative Counts**

Each entry stores the **total cumulative count** of all objects whose first byte is less than or equal to that position.

```
Example: Looking for SHA-1 starting with 0xAB
┌─────────────────────────────────────────────────────────────┐
│ fanout[0xAA] = 1,847,293  (objects with first byte ≤ 0xAA) │
│ fanout[0xAB] = 1,885,791  (objects with first byte ≤ 0xAB) │
└─────────────────────────────────────────────────────────────┘

Objects starting with 0xAB: positions 1,847,293 → 1,885,791
Search space reduced from 2,000,000 to just 38,498 objects!
```

### The Main Tables: Detailed Object Information

After the fan-out table narrows our search, we have three parallel tables:

#### SHA-1 Table
```
Entry 0:  000a1b2c3d4e5f6789abcdef0123456789abcdef
Entry 1:  000a1b2c3d4e5f6789abcdef0123456789abce00
Entry 2:  000a1b2c3d4e5f6789abcdef0123456789abce01
...
Entry N:  ffff1b2c3d4e5f6789abcdef0123456789abcdef
```

All object SHA-1s, sorted lexicographically. Binary search happens here.

#### CRC-32 Table
```
Entry 0:  A1B2C3D4  (CRC32 of compressed object 0)
Entry 1:  E5F6G7H8  (CRC32 of compressed object 1)
Entry 2:  I9J0K1L2  (CRC32 of compressed object 2)
...
```

Checksums for data integrity verification before decompression.

#### Offset Table
```
Entry 0:  00000000  (Object 0 starts at byte 0 in .pack)
Entry 1:  00012847  (Object 1 starts at byte 12,847 in .pack)
Entry 2:  00025694  (Object 2 starts at byte 25,694 in .pack)
...
```

Exact byte locations where each object's data begins in the `.pack` file.

### Large Offset Handling

For packfiles larger than 2GB, the implementation uses the same two-tier system as MIDX:

```go
// From parseIdx - handling large offsets
for i := range numEntries {
    rawOff := binary.BigEndian.Uint32(offsets[i*4:])

    var finalOffset uint64
    if rawOff&0x80000000 == 0 {
        // Normal 31-bit offset
        finalOffset = uint64(rawOff)
    } else {
        // Large offset - index into 64-bit table
        largeIdx := rawOff & 0x7FFFFFFF
        if int(largeIdx) >= len(largeOffsets) {
            return nil, fmt.Errorf("invalid large offset index")
        }
        finalOffset = largeOffsets[largeIdx]
    }

    entries[i] = idxEntry{
        offset: finalOffset,
        crc:    crcs[i],
    }
}
```

This keeps the index compact for most repositories while scaling to enormous sizes.

### File Integrity Verification

```go
// Pack trailer verification from the actual implementation
func verifyPackTrailer(pack *mmap.ReaderAt) error {
    size := pack.Len()
    if size < hashSize {
        return fmt.Errorf("pack too small for trailer")
    }

    // Read the 20-byte SHA-1 trailer
    trailer := make([]byte, hashSize)
    if _, err := pack.ReadAt(trailer, int64(size-hashSize)); err != nil {
        return fmt.Errorf("failed to read pack trailer: %w", err)
    }

    // Compute checksum over entire pack except trailer
    h := sha1.New()
    sec := io.NewSectionReader(pack, 0, int64(size-hashSize))
    if _, err := io.Copy(h, sec); err != nil {
        return fmt.Errorf("failed to checksum pack: %w", err)
    }

    computed := h.Sum(nil)
    if !bytes.Equal(computed, trailer) {
        return ErrPackTrailerCorrupt
    }
    return nil
}
```

---

## ⚡ The Two-Step Search in Practice

Here's how these components work together to find an object in microseconds:

### Step 1: The Fan-Out Lookup

```go
// Looking for SHA-1: a1b2c3d4e5f6789abcdef0123456789abcdef01
targetSHA := "a1b2c3d4e5f6789abcdef0123456789abcdef01"
firstByte := 0xa1

// Get search boundaries from fan-out table
var start, end uint32
if firstByte == 0 {
    start = 0
} else {
    start = fanout[firstByte-1]
}
end = fanout[firstByte]

// Now we know our object must be between positions start and end
fmt.Printf("Search space: %d objects (was %d)\n", end-start, totalObjects)
```

### Step 2: The Binary Search

```go
// Binary search within the narrow slice
left := start
right := end

for left < right {
    mid := (left + right) / 2
    midSHA := sha1Table[mid]

    if bytes.Compare(targetSHA, midSHA) < 0 {
        right = mid
    } else if bytes.Compare(targetSHA, midSHA) > 0 {
        left = mid + 1
    } else {
        // Found it!
        objectOffset := offsetTable[mid]
        return objectOffset, nil
    }
}
```

### Step 3: The Payoff

```go
// We now have the exact byte offset in the .pack file
offset := offsetTable[foundIndex]
crc32Expected := crc32Table[foundIndex]

// Jump directly to that location and read the object data
packFile.Seek(offset, io.SeekStart)
objectData := readCompressedObject(packFile)

// Verify integrity before expensive decompression
if crc32.Checksum(objectData) != crc32Expected {
    return fmt.Errorf("object corruption detected")
}

// Now decompress and use the object
decompressedData := zlibInflate(objectData)
```

---

## 📊 Performance Snapshot: Why the Fan-Out Matters

The impact of the fan-out table cannot be overstated. Let's compare different approaches:

| Repository Size | Search Method | Comparisons Needed | Time (Typical) |
|----------------|---------------|-------------------|----------------|
| **Small Repo** (10K objects) | | | |
| | Linear Scan | 5,000 (average) | 50ms |
| | Binary Search Only | ~13 | 0.1ms |
| | Fan-Out + Binary | ~7 | 0.05ms |
| **Medium Repo** (1M objects) | | | |
| | Linear Scan | 500,000 (average) | 5 seconds |
| | Binary Search Only | ~20 | 0.2ms |
| | Fan-Out + Binary | ~12 | 0.1ms |
| **Large Repo** (10M objects) | | | |
| | Linear Scan | 5,000,000 (average) | 50 seconds |
| | Binary Search Only | ~23 | 0.25ms |
| | Fan-Out + Binary | ~16 | 0.15ms |

### The Math Behind the Magic

For a repository with 10 million objects evenly distributed:

```
Without Fan-Out:
├── Search Space: 10,000,000 objects
├── Binary Search Steps: log₂(10,000,000) ≈ 23
└── Total Time: ~0.25ms

With Fan-Out:
├── Fan-Out Lookup: 1 step (O(1))
├── Reduced Search Space: 10,000,000 ÷ 256 ≈ 39,000 objects
├── Binary Search Steps: log₂(39,000) ≈ 15
└── Total Time: ~0.15ms

Speed Improvement: 1.67x faster
Memory Efficiency: 256x better cache locality
```

The real win isn't just speed - it's **predictable performance**. The fan-out ensures that lookups stay fast regardless of repository size.

---

## 🛠️ Building Our Own Index Reader

Let's implement a simple pack index reader to see these concepts in action:

```go
type PackIndex struct {
    fanout    [256]uint32
    sha1Table [][]byte
    crc32s    []uint32
    offsets   []uint32
    largeOffsets []uint64
}

func (idx *PackIndex) FindObject(targetSHA []byte) (uint32, error) {
    // Step 1: Fan-out lookup
    firstByte := targetSHA[0]
    var start, end uint32

    if firstByte == 0 {
        start = 0
    } else {
        start = idx.fanout[firstByte-1]
    }
    end = idx.fanout[firstByte]

    if start == end {
        return 0, fmt.Errorf("object not found")
    }

    // Step 2: Binary search
    left := start
    right := end

    for left < right {
        mid := (left + right) / 2
        comparison := bytes.Compare(targetSHA, idx.sha1Table[mid])

        if comparison == 0 {
            // Found it! Return the offset
            offset := idx.offsets[mid]

            // Handle large offsets
            if offset&0x80000000 != 0 {
                largeIndex := offset & 0x7FFFFFFF
                return uint32(idx.largeOffsets[largeIndex]), nil
            }

            return offset, nil
        } else if comparison < 0 {
            right = mid
        } else {
            left = mid + 1
        }
    }

    return 0, fmt.Errorf("object not found")
}
```

---

## 🎯 Key Takeaways

The pack index (`.idx`) is a masterclass in data structure design:

**Speed**: The fan-out table transforms O(n) searches into O(log n) with much smaller constant factors, making lookups practically instantaneous.

**Integrity**: CRC32 checksums for every object and SHA-1 checksums for entire files ensure data corruption is caught early.

**Scalability**: Smart optimizations like the large offset table handle repositories from kilobytes to terabytes efficiently.

**Simplicity**: Despite its sophistication, the format is straightforward to implement and understand.

---

## 🔮 Up Next

We've now mastered finding any object inside a single packfile. But what happens when a repository has dozens or even hundreds of packs? Searching each `.idx` file one-by-one would be slow.

In Part 3, we'll tackle the **multi-pack-index** (`.midx`), Git's solution for providing O(1) access across an entire repository, no matter how many packfiles it contains. We'll explore:

- How Git maintains performance across hundreds of pack files
- The clever data structure that unifies multiple indices
- Building a multi-pack reader that scales to enterprise-level repositories

The journey from single-pack lookups to repository-wide searches is where Git's architecture truly shines.
