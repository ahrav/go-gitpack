# 🔓 Under the Hood: Unpacking Git's Secrets

## Part 4: Cracking the Pack – From Raw Bytes to Live Objects

In our journey so far, we've built a powerful indexing system. We can take any object's SHA-1 hash and, using the `.idx` and `.midx` files, instantly find its precise location within a packfile. We have the map and the coordinates.

Today, we arrive at our destination. We'll dive into the packfile itself, decompress its contents, unravel Git's clever delta-encoding scheme, and write the code that turns raw, compressed bytes back into live, usable objects.

---

## 🗜️ The First Hurdle: Inflation and Object Headers

When we seek to an object's offset in a packfile, we don't find plain text. We find a small header followed by a stream of bytes compressed with zlib. Our first task is to "inflate" this data.

Before we can decompress, we must read the header to learn two critical facts:

- **Object Type**: Is this a commit, tree, blob, or a delta?
- **Uncompressed Size**: How large will the object be after we inflate it?

### Variable-Length Header Parsing

Git uses a space-saving variable-length encoding for this header:

**Like shipping packages at scale** - a jewelry ring ships in a tiny padded envelope with minimal labeling, while a refrigerator requires a massive crate with extensive handling instructions, weight warnings, and tracking barcodes. The packaging grows precisely to match what's inside. This saves massive space across millions of objects.

```go
// From the actual parseObjectHeaderUnsafe implementation
func parseObjectHeaderUnsafe(data []byte) (ObjectType, uint64, int) {
    if len(data) == 0 {
        return ObjBad, 0, -1
    }

    // First byte contains type and lower bits of size
    b0 := *(*byte)(unsafe.Pointer(&data[0]))
    objType := ObjectType((b0 >> 4) & 7)
    size := uint64(b0 & 0x0f)

    if b0&0x80 == 0 {
        return objType, size, 1
    }

    // Fast path for common 2-3 byte headers
    if len(data) >= 3 {
        b1 := *(*byte)(unsafe.Add(unsafe.Pointer(&data[0]), 1))
        size |= uint64(b1&0x7f) << 4
        if b1&0x80 == 0 {
            return objType, size, 2
        }

        b2 := *(*byte)(unsafe.Add(unsafe.Pointer(&data[0]), 2))
        size |= uint64(b2&0x7f) << 11
        if b2&0x80 == 0 {
            return objType, size, 3
        }
    }

    // Fallback for longer headers
    shift := uint(4)
    for i := 1; i < len(data) && i < 10; i++ {
        b := *(*byte)(unsafe.Add(unsafe.Pointer(&data[0]), i))
        size |= uint64(b&0x7f) << shift
        shift += 7
        if b&0x80 == 0 {
            return objType, size, i + 1
        }
    }

    return ObjBad, 0, -1
}
```

### Object Decompression

Once we have the type and size, we can choose the optimal decompression strategy:

```go
// From the actual readRawObject implementation
func readRawObject(r *mmap.ReaderAt, off uint64) (ObjectType, []byte, error) {
    /* ---- 1. parse the generic variable‑length header ------------------ */

    var hdr [32]byte
    n, err := r.ReadAt(hdr[:], int64(off))
    if err != nil && err != io.EOF {
        return ObjBad, nil, err
    }
    if n == 0 {
        return ObjBad, nil, errors.New("empty object")
    }

    objType, _, hdrLen := parseObjectHeaderUnsafe(hdr[:n])
    if hdrLen <= 0 {
        return ObjBad, nil, errors.New("cannot parse object header")
    }

    pos := int64(off) + int64(hdrLen) // first byte *after* the generic header

    /* ---- 2. read (and keep) the delta prefix, if any ------------------ */

    var prefix []byte
    switch objType {
    case ObjRefDelta:
        prefix = make([]byte, 20)
        if _, err := r.ReadAt(prefix, pos); err != nil {
            return ObjBad, nil, err
        }
        pos += 20

    case ObjOfsDelta:
        // variable‑length negative offset
        for {
            var b [1]byte
            if _, err := r.ReadAt(b[:], pos+int64(len(prefix))); err != nil {
                return ObjBad, nil, err
            }
            prefix = append(prefix, b[0])
            if b[0]&0x80 == 0 { // MSB clear → last byte
                pos += int64(len(prefix))
                break
            }
            if len(prefix) > 12 { // sanity limit
                return ObjBad, nil, errors.New("ofs‑delta base‑ref too long")
            }
        }
    }

    /* ---- 3. inflate the deflate stream that follows ------------------- */

    // We do not know the compressed length in advance, so we give
    // SectionReader an "infinite" length; io.NewSectionReader caps reads at
    // EOF anyway.
    src := io.NewSectionReader(r, pos, 1<<63-1)
    zr, err := zlib.NewReader(src)
    if err != nil {
        return ObjBad, nil, err
    }
    defer zr.Close()

    var out bytes.Buffer
    if _, err := out.ReadFrom(zr); err != nil {
        return ObjBad, nil, err
    }

    // Return [prefix||inflated] for deltas, or just the inflated body otherwise.
    if len(prefix) != 0 {
        return objType, append(prefix, out.Bytes()...), nil
    }
    return objType, out.Bytes(), nil
}
```

If the object's type is a standard commit, tree, or blob, our work is done. But most of the time, we find a delta.

---

## ⚡ The Real Magic: Delta Compression

To save enormous amounts of space, Git stores most objects as **deltas** - a set of instructions for transforming a "base" object into the target object.

**Like a GPS navigation system** - instead of storing millions of complete route maps for every possible journey, it stores one base map and generates turn-by-turn instructions ('turn left in 500ft, continue straight for 2m') to transform any starting point into any destination. Git deltas work the same way, storing just the differences between file versions.

When you have 500 commits changing one line in a 10,000-line file, Git stores the original once plus 499 tiny instruction sets saying "change line 42." Your `.git` folder stays small instead of exploding to gigabytes.

### Two Types of Deltas

**OFS_DELTA (Offset Delta)**
- Base object is in the same packfile
- Referenced by negative offset from current position
- Most common type

**REF_DELTA (Reference Delta)**
- Base object referenced by full 20-byte SHA-1 hash
- Base may be in different packfile

### Delta Header Parsing

```go
// From the actual parseDeltaHeader implementation
func parseDeltaHeader(t ObjectType, data []byte) (Hash, uint64, []byte, error) {
    var h Hash

    if t == ObjRefDelta {
        if len(data) < 20 {
            return h, 0, nil, fmt.Errorf("ref delta too short")
        }
        *(*Hash)(unsafe.Pointer(&h[0])) = *(*Hash)(unsafe.Pointer(&data[0]))
        return h, 0, data[20:], nil
    }

    // Ofs-delta with optimized varint parsing
    if len(data) == 0 {
        return h, 0, nil, fmt.Errorf("ofs delta too short")
    }

    b0 := *(*byte)(unsafe.Pointer(&data[0]))
    off := uint64(b0 & 0x7f)

    if b0&0x80 == 0 {
        return h, off, data[1:], nil
    }

    // Unrolled loop for common cases
    i := 1
    for i < len(data) && i < 10 {
        b := *(*byte)(unsafe.Add(unsafe.Pointer(&data[0]), i))
        off = (off + 1) << 7
        off |= uint64(b & 0x7f)
        i++
        if b&0x80 == 0 {
            break
        }
    }

    if i >= len(data) {
        return h, 0, nil, fmt.Errorf("invalid ofs delta encoding")
    }

    return h, off, data[i:], nil
}
```

### Delta Application

Delta data contains a stream of two opcodes:

**Copy Opcode (MSB=1)**: Copy bytes from base object to output
**Insert Opcode (MSB=0)**: Insert literal bytes from delta stream

**Like following GPS turn-by-turn directions** - The Copy opcode is like "Continue on this road for 500 feet" (copying from the base map), and the Insert opcode is like "Turn left onto 'main.go' street" (inserting a new instruction). A delta is just a stream of these commands that navigate from the base version to the target version.

```go
// From the actual applyDelta implementation
func applyDelta(base, delta []byte) []byte {
    if len(delta) == 0 {
        return nil
    }

    _, n1 := decodeVarInt(delta)
    if n1 <= 0 || n1 >= len(delta) {
        return nil
    }
    targetSize, n2 := decodeVarInt(delta[n1:])
    if n2 <= 0 || n1+n2 >= len(delta) {
        return nil
    }

    out := make([]byte, targetSize)

    deltaLen := len(delta)
    baseLen := len(base)
    opIdx := n1 + n2
    outIdx := 0

    for opIdx < deltaLen {
        op := *(*byte)(unsafe.Add(unsafe.Pointer(&delta[0]), opIdx))
        opIdx++

        if op&0x80 != 0 { // Copy operation
            var cpOff, cpLen uint32

            // Unrolled offset parsing
            if op&0x01 != 0 {
                if opIdx >= deltaLen {
                    return nil
                }
                cpOff = uint32(*(*byte)(unsafe.Add(unsafe.Pointer(&delta[0]), opIdx)))
                opIdx++
            }
            if op&0x02 != 0 {
                if opIdx >= deltaLen {
                    return nil
                }
                cpOff |= uint32(*(*byte)(unsafe.Add(unsafe.Pointer(&delta[0]), opIdx))) << 8
                opIdx++
            }
            if op&0x04 != 0 {
                if opIdx >= deltaLen {
                    return nil
                }
                cpOff |= uint32(*(*byte)(unsafe.Add(unsafe.Pointer(&delta[0]), opIdx))) << 16
                opIdx++
            }
            if op&0x08 != 0 {
                if opIdx >= deltaLen {
                    return nil
                }
                cpOff |= uint32(*(*byte)(unsafe.Add(unsafe.Pointer(&delta[0]), opIdx))) << 24
                opIdx++
            }

            // Unrolled length parsing
            if op&0x10 != 0 {
                if opIdx >= deltaLen {
                    return nil
                }
                cpLen = uint32(*(*byte)(unsafe.Add(unsafe.Pointer(&delta[0]), opIdx)))
                opIdx++
            }
            if op&0x20 != 0 {
                if opIdx >= deltaLen {
                    return nil
                }
                cpLen |= uint32(*(*byte)(unsafe.Add(unsafe.Pointer(&delta[0]), opIdx))) << 8
                opIdx++
            }
            if op&0x40 != 0 {
                if opIdx >= deltaLen {
                    return nil
                }
                cpLen |= uint32(*(*byte)(unsafe.Add(unsafe.Pointer(&delta[0]), opIdx))) << 16
                opIdx++
            }

            if cpLen == 0 {
                cpLen = 65536
            }

            if int(cpOff)+int(cpLen) > baseLen || outIdx+int(cpLen) > int(targetSize) {
                return nil
            }

            // Use memmove for potentially overlapping memory
            copyMemory(
                unsafe.Add(unsafe.Pointer(&out[0]), outIdx),
                unsafe.Add(unsafe.Pointer(&base[0]), cpOff),
                int(cpLen),
            )
            outIdx += int(cpLen)

        } else if op != 0 { // Insert operation
            insertLen := int(op)
            if opIdx+insertLen > deltaLen || outIdx+insertLen > int(targetSize) {
                return nil
            }

            // Direct memory copy
            copyMemory(
                unsafe.Add(unsafe.Pointer(&out[0]), outIdx),
                unsafe.Add(unsafe.Pointer(&delta[0]), opIdx),
                insertLen,
            )
            opIdx += insertLen
            outIdx += insertLen
        } else {
            return nil // Invalid op
        }
    }

    return out
}
```

This implementation works well for applying a single delta, but what happens when we need to resolve a chain of deltas? That's where things get more interesting.

---

## 🔄 The Ping-Pong Buffer Strategy: Resolving Delta Chains Efficiently

When Git stores an object as a delta chain (delta → delta → delta → base), we need to apply each delta in sequence. The naive approach would allocate a new buffer for each step, but that's wasteful. Instead, we use a clever "ping-pong" buffer strategy that reuses memory throughout the entire chain resolution.

### The Two-Phase Approach

Our implementation splits delta chain resolution into two distinct phases:

**1. Walk-Up Phase: Analyzing the Chain**
Starting from the target object, we walk *up* the delta chain until we reach a non-delta base object. During this phase, we:
- Build a stack of all deltas we'll need to apply
- Check for cycles and enforce depth limits
- Calculate the maximum buffer size we'll need
- But we don't actually apply any deltas yet!

**2. Apply-Down (Ping-Pong) Phase: Applying Deltas**
Once we know the full chain, we allocate a single arena with two equal halves:

```
+----------------------+----------------------+
|  Buffer A (input)    |  Buffer B (output)   |
+----------------------+----------------------+
```

Then we apply deltas by "ping-ponging" between these buffers:
1. Start with the base object in Buffer A
2. Apply the first delta: read from A, write to B
3. Swap roles: B becomes input, A becomes output
4. Apply the next delta: read from B, write to A
5. Continue swapping until all deltas are applied

### Why This Matters

This approach has several advantages:
- **Single allocation**: We allocate memory once for the entire chain
- **No copying**: Buffers swap roles instead of copying data
- **Bounded memory**: We know the exact size needed before starting
- **Cache-friendly**: Data stays in the same memory region

Here's a simplified view of how it works in practice:

```go
// From the actual implementation in delta.go
func applyDeltaStack(
    stack deltaStack,
    baseData []byte,
    baseType ObjectType,
) ([]byte, ObjectType, error) {
    // Get a reusable arena from the pool
    arena := getDeltaArena()
    defer putDeltaArena(arena)

    // Calculate the maximum size we'll need
    maxTarget := peekLargestTarget(stack)
    if bs := uint64(len(baseData)); bs > maxTarget {
        maxTarget = bs
    }

    // Split arena into two halves for ping-pong
    bufA := arena.data[:maxTarget]
    bufB := arena.data[maxTarget : maxTarget*2]

    // Start with base data in bufA
    current := bufA[:len(baseData)]
    copy(current, baseData)

    // Track which buffer we're using
    usingA := true

    // Apply deltas in reverse order (oldest to newest)
    for i := len(stack) - 1; i >= 0; i-- {
        d := stack[i]

        // Choose output buffer (the one we're NOT using)
        var out []byte
        if usingA {
            out = bufB[:0]
        } else {
            out = bufA[:0]
        }

        // Apply delta: read from current, write to out
        result, err := applyDeltaStreaming(d.pack, d.offset, d.typ, current, out)
        if err != nil {
            return nil, ObjBad, err
        }

        // Swap: output becomes the new input
        current = result
        usingA = !usingA
    }

    // Copy final result to return
    final := make([]byte, len(current))
    copy(final, current)
    return final, baseType, nil
}
```

The ping-pong approach elegantly solves the problem of applying multiple deltas without excessive allocations or copies. It's a perfect example of how understanding the problem domain (Git's delta chains) leads to optimized solutions.

---

## 🎯 Preventing Re-inflation: A Git-Optimized Cache

Delta resolution creates a unique caching challenge: objects are needed multiple times during long-running operations, but traditional LRU caches can't distinguish between "recently accessed" and "actively being used."

### An Analogy: A Library Checkout System

To understand the problem and the solution, think of a library managing its most popular books.

-   **A standard LRU cache is like a library with an overzealous, automated return policy.** It sees a textbook sitting on a table, notices it hasn't been physically touched in an hour, and whisks it back to deep storage. But a student was just about to use that textbook for their open-book exam! Now they have to waste precious time requesting it again. The system's focus on "recent use" completely missed that the book was still "in use."

-   **Our reference-counted cache is like a proper checkout system.** When the student checks out the textbook, they get a handle for it. The book cannot be returned to storage until that handle is explicitly returned. Even if the book sits on the table for hours, it's considered "in use" and remains available because the checkout is still active. Multiple students can check out the same book, and it will only be eligible for storage once every single one of them has returned it. This is exactly how our `refCountedDeltaWindow` works: an object cannot be evicted while a `Handle` to it exists.

### The Re-inflation Problem

Consider this scenario during delta chain resolution:

```go
// Thread 1: Resolving a deep delta chain (takes 100ms total)
base := cache.Get("large-base-1MB")     // Expensive: 5ms to inflate
delta1 := applyDelta(base, getDelta1())   // 20ms
delta2 := applyDelta(delta1, getDelta2()) // 20ms
// ... doing more work, but base is still "logically in use"

// Meanwhile, Thread 2-8: Processing other objects
// They fill the cache with their work, evicting "large-base-1MB"

// Thread 1: Needs the base again for final step
base2 := cache.Get("large-base-1MB")    // CACHE MISS!
// Must re-read from pack: 2ms disk I/O
// Must re-inflate: 5ms decompression
// 7ms penalty for something we already had!
```

For large packfiles with deep delta chains and high concurrency, this re-inflation cost becomes significant. Large base objects (256KB-1MB+) are expensive to decompress, and delta chains often need the same base object multiple times.

### Reference Counting Solution

Instead of a simple LRU cache, we use reference counting to track which objects are actively being used:

```go
// With reference counting:
handle := cache.acquire("large-base")     // Inflates once: 5ms
// handle now has refCnt=1, cannot be evicted!

delta1 := applyDelta(handle.Data(), getDelta1()) // 20ms
delta2 := applyDelta(handle.Data(), getDelta2()) // 20ms
// ... more work, base stays in cache

finalResult := applyDelta(handle.Data(), getFinalDelta()) // Uses cached copy!
handle.Release() // Now eligible for eviction
```

The key insight: objects with active references cannot be evicted, preventing expensive re-inflation during multi-step operations. This cache design is specifically tailored to Git's delta resolution patterns where objects need to be held across multiple operations.

---

## 🛡️ Staying Safe: Cycle and Depth Protection

A malformed packfile could contain delta chains that reference themselves or are pathologically deep. We use a `deltaContext` to guard against this:

```go
// From the actual deltaContext implementation
type deltaContext struct {
    // visited records every base object reached by object ID during the
    // current resolution. It lets the resolver detect ref-delta cycles.
    visited map[Hash]bool

    // offsets records every packfile offset reached during ofs-delta
    // resolution. It lets the resolver detect cycles that reference a
    // previously visited object in the same pack.
    offsets map[uint64]bool

    // depth is the current recursion depth. It is incremented on entry to
    // a child delta and decremented on exit.
    depth int

    // maxDepth is the maximum permitted depth before resolution aborts
    // with an error. It is fixed when the context is created.
    maxDepth int
}

func (ctx *deltaContext) checkRefDelta(hash Hash) error {
    // Guard against unbounded recursion
    if ctx.depth >= ctx.maxDepth {
        return fmt.Errorf("delta chain too deep (max %d)", ctx.maxDepth)
    }
    // Detect ref-delta cycles
    if ctx.visited[hash] {
        return fmt.Errorf("circular delta reference detected for %x", hash)
    }
    return nil
}

func (ctx *deltaContext) enterRefDelta(hash Hash) {
    ctx.visited[hash] = true
    ctx.depth++
}

func (ctx *deltaContext) exit() {
    ctx.depth--
}
```

This makes our resolver robust against corrupted or malicious repository data.

---

## 🚀 The Complete Pipeline

Here's how all the pieces work together in the actual Store implementation:

```go
// From the actual Store.Get implementation
func (s *Store) Get(oid Hash) ([]byte, ObjectType, error) {
    // Fast path: check the tiny "delta window" that holds the most recently
    // materialized objects, which are likely to be referenced by upcoming
    // deltas.
    if b, ok := s.dw.acquire(oid); ok {
        d, t := b.Data(), b.Type()
        b.Release()
        return d, t, nil
    }

    // Second-level lookup in the larger ARC cache.
    if b, ok := s.cache.Get(oid); ok {
        return b.data, b.typ, nil
    }

    // Cache miss – inflate from pack while tracking delta depth to prevent
    // cycles and excessive recursion.
    return s.getWithContext(oid, newDeltaContext(s.maxDeltaDepth))
}

func (s *Store) inflateFromPack(
    p *mmap.ReaderAt,
    off uint64,
    oid Hash,
    ctx *deltaContext,
) ([]byte, ObjectType, error) {
    // Fast header peek avoids unnecessary zlib work for commits that the
    // commit-graph already covers.
    objType, _, err := peekObjectType(p, off)
    if err != nil {
        return nil, ObjBad, err
    }

    if objType == ObjCommit {
        // Commit‑graph supplies metadata → skip inflation entirely.
        return nil, ObjCommit, nil
    }

    // Handle delta objects - they need decompression too!
    if objType == ObjOfsDelta || objType == ObjRefDelta {
        // readRawObject returns the delta-instruction stream with the raw
        // prefix (20-byte hash or variable-length offset) still attached so
        // that parseDeltaHeader can reuse it.
        _, deltaData, err := readRawObject(p, off)
        if err != nil {
            return nil, ObjBad, err
        }

        // Extract base reference and the delta instruction buffer.
        baseHash, baseOff, deltaBuf, err := parseDeltaHeader(objType, deltaData)
        if err != nil {
            return nil, ObjBad, err
        }

        // Resolve the base object
        var base []byte
        var baseType ObjectType
        if objType == ObjRefDelta {
            if err := ctx.checkRefDelta(baseHash); err != nil {
                return nil, ObjBad, err
            }
            ctx.enterRefDelta(baseHash)
            base, baseType, err = s.getWithContext(baseHash, ctx)
            ctx.exit()
        } else { // ObjOfsDelta
            // For ofs-delta, calculate the actual offset
            actualOffset := off - baseOff
            if err := ctx.checkOfsDelta(actualOffset); err != nil {
                return nil, ObjBad, err
            }
            ctx.enterOfsDelta(actualOffset)
            base, baseType, err = readObjectAtOffsetWithContext(p, actualOffset, s, ctx)
            ctx.exit()
        }
        if err != nil {
            return nil, ObjBad, err
        }

        // Apply the delta to reconstruct the full object
        full := applyDelta(base, deltaBuf)
        if full == nil {
            return nil, ObjBad, fmt.Errorf("delta application failed for object %x", oid)
        }

        s.dw.add(oid, full, baseType)
        s.cache.Add(oid, cachedObj{data: full, typ: baseType})
        return full, baseType, nil
    }

    // For regular objects (blob, tree, tag), use normal decompression
    _, data, err := readRawObject(p, off)
    if err != nil {
        return nil, ObjBad, err
    }

    // Optional CRC-32 integrity check.
    if s.VerifyCRC {
        crc, found := s.findCRCForObject(p, off, oid)
        if found {
            if err := s.verifyCRCForPackObject(p, off, crc); err != nil {
                return nil, ObjBad, err
            }
        } else if s.midx != nil && s.midx.version >= 2 {
            // For MIDX v2, we should have CRCs available.
            return nil, ObjBad, fmt.Errorf("no CRC found for object %x in MIDX v2", oid)
        }
    }

    s.dw.add(oid, data, objType)
    s.cache.Add(oid, cachedObj{data: data, typ: objType})
    return data, objType, nil
}
```
