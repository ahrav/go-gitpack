# 🔓 Under the Hood: Unpacking Git's Secrets

**Part 2: The Pack Index — Git’s Built‑In Search Engine**

> *Last time we answered “Why build our own Git engine?”*\
> The takeaway was that every high‑performance scan needs **fast, random access** to Git objects.\
> *Today we build the piece that makes that access O(1): the pack ****index**** (**``**\*\*\*\*\*\*\*\*).*

---

## 0. Why an index at all?

Imagine a warehouse that stores every edition of every book ever printed, but all the crates look identical.\
Without an inventory list you’d have to open every crate until you stumble on the right book.

A **Git packfile** is that warehouse: one giant blob with millions of objects jammed together. The **pack index** is the inventory list that tells us **exactly which byte offset** a given object lives at.\
Without it Step 2 (*Inflate / Delta resolve*) would degrade from *seek‑and‑read* to *linear scan*, a non‑starter on multi‑gigabyte packs.

---

## 1. The Library Card Analogy, Revisited

We keep the card‑catalog story because it’s intuitive, but add the missing “why”:

| Library metaphor          | What happens in Git                                  | Why it matters                                                          |
| ------------------------- | ---------------------------------------------------- | ----------------------------------------------------------------------- |
| Section guide (A–F, G–M…) | **Fan‑out table** — 256 counters, one per first‑byte | Shrinks our binary‑search window from *the whole pack* to a tiny slice. |
| Alphabetized rack         | **Sorted hash table**                                | Lets us finish with log₂(n) comparisons instead of n.                   |
| Book’s shelf mark         | **Offset array**                                     | Tells us where in the pack to seek.                                     |
| Dust‑jacket barcode       | **CRC32**                                            | Cheap corruption check before we even decompress.                       |

---

## 2. Pack Index File Layout (and What Each Field Buys Us)

1. **Magic + Version** — sanity check; abort early if we mmap the wrong file.
2. **Fan‑out table (256 × 4 bytes)** — narrows search window.\
   *Why?* Saves millions of comparisons on large packs.
3. **Sorted SHA‑1 (20 × N)** — final binary search.\
   *Why?* Guarantees log time look‑ups.
4. **CRC‑32 (4 × N)** — verify bytes *before* we waste CPU inflating them.
5. **32‑bit offsets (4 × N)** — jump straight to the object.
6. **Large‑offset table (optional)** — keeps format compact while allowing > 2 GB packs.

> **Why every field matters:** none of these entries are optional. Each one removes a bottleneck—either on *search cost* or on *data integrity*.  either on *search cost* or on *data integrity*.

---

## 3. Two‑Step Search Walk‑through



```go
// idxFile.findObject implements the two‑stage lookup we described.
func (f *idxFile) findObject(hash Hash) (uint64, bool) {
    first := hash[0]                     // 1️⃣ first‑byte bucket

    // 1. section bounds via fan‑out
    lo := f.fanout[first-1]              // lower inclusive (0 when first==0)
    hi := f.fanout[first] - 1            // upper inclusive

    if lo > hi {                         // empty bucket → object absent
        return 0, false
    }

    // 2. binary search inside that window
    rel, ok := slices.BinarySearchFunc(
        f.oidTable[lo:hi+1], hash,
        func(a, b Hash) int { return bytes.Compare(a[:], b[:]) },
    )
    if !ok {
        return 0, false
    }
    abs := int(lo) + rel
    return f.entries[abs].offset, true
}
```

- **Why first‑byte bucket?** 256 counters fit in CPU cache.
- **Why binary search next?** Hashes are sorted -> O(log k) where *k* is bucket size.

---

## 4. Parsing the Index — Guided Tour (powered by `parseIdx`)



### 4.1 Verify header → *Fail fast on wrong file type*

```go
header := make([]byte, 8)
ix.ReadAt(header, 0)
if !bytes.Equal(header[:4], []byte{0xff, 0x74, 0x4f, 0x63}) {
    return nil, fmt.Errorf("not a Git pack‑index file")
}
if ver := binary.BigEndian.Uint32(header[4:]); ver != 2 {
    return nil, fmt.Errorf("unsupported idx version %d", ver)
}
```

*Why?* Bail out early if we were handed the wrong file or an ancient index version.

### 4.2 Read fan‑out → *Learn object count & bucket bounds*

```go
fanoutData := make([]byte, 256*4)
ix.ReadAt(fanoutData, 8) // 8‑byte header already consumed
for i := 0; i < 256; i++ {
    f.fanout[i] = binary.BigEndian.Uint32(fanoutData[i*4:])
}
objCount := f.fanout[255] // last entry holds total #objects
```

*Why?* Those 256 counters instantly tell us “bucket limits” and the **total** object count used for slice sizing.

### 4.3 Slice hashes / CRC / offsets → *Prepare parallel arrays*

```go
// Byte ranges pre‑computed from objCount
oidBase   := 8 + 256*4
crcBase   := oidBase   + int64(objCount*20)
offBase   := crcBase  + int64(objCount*4)

// One mmap read for the whole block → fewer syscalls
bulk := make([]byte, objCount*(20+4+4))
ix.ReadAt(bulk, oidBase)

// Unsafe slice trick to avoid per‑hash allocations
oids := *(*[]Hash)(unsafe.Pointer(&bulk[0]))[:objCount:objCount]
crcs := bulk[objCount*20 : objCount*24]
offs := bulk[objCount*24:]
```

*Why?* Reading contiguous blocks and keeping **parallel arrays** keeps look‑ups cache‑friendly.

### 4.4 Handle big packs → *Support monorepos without bloating small ones*

```go
// 32‑bit offset with MSB=1 → use 64‑bit table
if off32 & 0x8000_0000 != 0 {
    largeIdx := off32 & 0x7fff_ffff
    entries[i].offset = largeOffsets[largeIdx]
} else {
    entries[i].offset = uint64(off32)
}
```

*Why?* Packs smaller than 2 GB stay compact (4‑byte offsets); jumbo packs get 64‑bit precision only where needed.

---

## 5. Putting It to Work

Show a **10‑line main** that prints

```
Found 3 123 456 objects across 1 pack
Lookup 45dead… → offset 0x1A2B3C, CRC ok
```



---

## 6. Performance Snapshot — Why the Fan‑out Matters

| Objects | Linear scan | Fan‑out + bin search           | Speed‑up      |
| ------- | ----------- | ------------------------------ | ------------- |
| 10 M    | 10 M comps  | 256 + log₂(39 000) ≈ 272 comps | **\~37 000×** |

Even on SSDs, 37 000 fewer comparisons per lookup is a win you *feel*.

---

## 7. Takeaways

- The pack index is not optional; it underpins every O(1) object lookup.
- Fan‑out + sorted table is a textbook example of **two‑level search**.
- CRCs on compressed bytes give *cheap* corruption checks.
- The format scales from tiny repos to multi‑gigabyte monorepos without waste.

---

### Next: Multi‑Pack‑Index (MIDX)

Now that we can find any object *inside one pack*, the next hurdle is: *what if the repo has 20 packs?* A single `.midx` gives us the same O(1) access across them all.\
We’ll decode that in **Part 3**.

