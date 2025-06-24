# 🔓 Under the Hood: Unpacking Git's Secrets
**Part 3: One Index to Rule Them All – The Multi‑Pack Index**

> *Missed the journey so far? In* [Part 1](./part1.md) *we asked why a secret‑scanner should talk to Git’s storage directly, and in* [Part 2](./part2.md) *we learned how a single pack‑index lets us jump to any object in O(1). Today we’re scaling that trick to **hundreds** of packfiles without breaking a sweat.*

---

## 🚚 Why Lots of Packfiles Slow Us Down

When Git runs `git gc`, fetches from remotes, or writes a shallow clone, it often **creates a brand‑new `.pack` file** instead of rewriting the existing one. Over time even a modest‑sized repository can balloon from a single pack to dozens or hundreds:

* fresh repo after first commit – **1 pack**
* three months of active feature work – **17 packs**
* three years of CI fetches and GC cycles – **≈150 packs**

Using the per‑pack `.idx` we built in Part 2, each object lookup is forced to do a tiny binary search **inside *every* pack**:

```text
for each .idx (N)
    binary‑search for <hash>
```

A few microseconds × 150 packs × millions of lookups quickly turns into minutes of wasted CPU.

**Git’s answer is the _multi‑pack index_** (`.midx`). It flattens all those per‑pack fan‑out tables into one master index so we pay the binary‑search cost exactly **once**, no matter how many packs exist.

## 📚 Analogy – The City‑Wide Library Catalogue

> *Think of every packfile as a branch library.*
>
> *The MIDX is the city’s central catalogue: search once, learn the branch + shelf, then go straight there.*

If you enjoyed the card‑catalogue metaphor in Part 2, nothing changes – we simply graft every branch’s catalogue into one gigantic, ordered list. The fanout table still tells us the aisle; the binary search still finds the card. We just tack on **“branch ID”** next to the **“shelf offset”**.

---

## 🏗️ MIDX File Anatomy at 10 000 ft

```
┌────────┐ 12 B  Header   "MIDX", ver, hash‑algo, chunk‑count, pack‑count
│Header  │
├────────┤ 12×N Chunk table – (ID, offset) pairs sorted by offset
│Chunks  │
├────────┤  … PNAM – NUL‑terminated packfile names
│PNAM    │
├────────┤  … OIDF – 256×fanout counts (object‑ID fanout)
│OIDF    │
├────────┤  … OIDL – sorted list of all object IDs
│OIDL    │
├────────┤  … OOFF – (pack‑ID, 32‑bit offset) per object
│OOFF    │
├────────┤  … LOFF – 64‑bit overflow offsets (optional)
│LOFF    │
└────────┘
```

Exactly the same building blocks we met in `.idx`, just stacked side‑by‑side so one index can describe *every* pack.

---

## ✍️ Parsing the MIDX – Walkthrough with Real Code

Below you’ll see trimmed but **literal** excerpts from our production `objstore` package. Each step begins with a one‑liner *why* and then the exact lines that make it happen. Line numbers and minor error‑handling have been snipped for brevity; the logic is unchanged.

### 1️⃣ Header sanity‑check

```go
// parseMidx.go – fast‑fail if the file isn’t a real MIDX
var hdr [12]byte
if _, err := mr.ReadAt(hdr[:], 0); err != nil { return nil, err }
if !bytes.Equal(hdr[0:4], []byte("MIDX")) {
    return nil, fmt.Errorf("not a MIDX file")
}

version   := hdr[4]                // v1 or v2
packCount := int(binary.BigEndian.Uint32(hdr[8:12]))
```

### 2️⃣ Chunk directory – turning offsets into a helper

```go
// chunk rows: 4‑byte ID + 8‑byte offset
cd := make([]struct{ id [4]byte; off uint64 }, chunks+1) // sentinel row
for i := range cd {
    base := int64(12 + i*12)
    _, _ = mr.ReadAt(row[:], base)
    copy(cd[i].id[:], row[0:4])
    cd[i].off = binary.BigEndian.Uint64(row[4:12])
}

sort.Slice(cd, func(i, j int) bool { return cd[i].off < cd[j].off })

findChunk := func(id uint32) (off, size int64, err error) { /* see source */ }
```

### 3️⃣ PNAM – mmap every pack exactly once (thanks, `packCache`)

```go
pnOff, pnSize, _ := findChunk(chunkPNAM)
pn := make([]byte, pnSize)
_, _ = mr.ReadAt(pn, pnOff)

packs := make([]*mmap.ReaderAt, packCount)
for i, start := 0, 0; i < packCount; i++ {
    end := bytes.IndexByte(pn[start:], 0)
    packName := string(pn[start : start+end])
    fullPath := filepath.Join(dir, packName)

    if h, ok := packCache[fullPath]; ok {
        packs[i] = h // reuse handle
    } else {
        packs[i], _ = mmap.Open(fullPath)
        packCache[fullPath] = packs[i]
    }
    start += end + 1
}
```

### 4️⃣ Fan‑out & sorted object IDs

```go
var fanout [256]uint32
fanOff, _, _ := findChunk(chunkOIDF)
_, _ = mr.ReadAt(unsafe.Slice((*byte)(unsafe.Pointer(&fanout[0])), 1024), fanOff)

objCount := fanout[255]

oidOff, _, _ := findChunk(chunkOIDL)
oids := make([]Hash, objCount)
_, _ = mr.ReadAt(unsafe.Slice((*byte)(unsafe.Pointer(&oids[0])), int(objCount*hashSize)), oidOff)
```

### 5️⃣ Offsets (+64‑bit spill‑over) & CRC (v2 only)

```go
offOff, _, _ := findChunk(chunkOOFF)
offRaw := make([]byte, objCount*8)
_, _ = mr.ReadAt(offRaw, offOff)

loffOff, loffSize, _ := findChunk(chunkLOFF)
var loff []uint64
if loffSize > 0 {
    loff = make([]uint64, loffSize/8)
    _, _ = mr.ReadAt(unsafe.Slice((*byte)(unsafe.Pointer(&loff[0])), int(loffSize)), loffOff)
}

entries := make([]midxEntry, objCount)
for i := 0; i < int(objCount); i++ {
    packID := binary.BigEndian.Uint32(offRaw[i*8 : i*8+4])
    rawOff := binary.BigEndian.Uint32(offRaw[i*8+4 : i*8+8])

    off64 := uint64(rawOff)
    if rawOff&0x80000000 != 0 { // overflow table lookup
        off64 = loff[rawOff&0x7FFFFFFF]
    }

    entries[i] = midxEntry{packID: packID, offset: off64, crc: crc32IfAny(i)}
}
```

### 6️⃣ Constant‑time lookup

```go
func (m *midxFile) findObject(h Hash) (*mmap.ReaderAt, uint64, bool) {
    first := h[0]
    start := uint32(0)
    if first > 0 { start = m.fanout[first-1] }
    end := m.fanout[first]

    idx, hit := slices.BinarySearchFunc(
        m.objectIDs[start:end], h,
        func(a, b Hash) int { return bytes.Compare(a[:], b[:]) },
    )
    if !hit { return nil, 0, false }

    ent := m.entries[int(start)+idx]
    return m.packReaders[ent.packID], ent.offset, true
}
```

*(Full source, including exhaustive error checks, lives in the project repo; snippets here focus on control‑flow relevant to the blog.)*

## 🚦 Pack‑Handle Cache – Small Change, Big Win

Mapping the same `.pack` twice wastes both RAM *and* system calls. We fix it with a two‑liner:

```go
if h, ok := packCache[path]; ok {
    m.packs[i] = h   // already mapped – reuse
} else {
    h, _ := mmap.Open(path)
    m.packs[i] = h
    packCache[path] = h
}
```

On the Linux kernel mirror (127 packs) this drops `open()` from **247 → 3** calls when our scanner chases three objects down a commit → tree → blob chain.

---

## 🔮 Coming Up – Time to Crack the Pack

We can now pinpoint any object in a multi‑gigabyte repository in microseconds. Next stop: **opening the packfile itself.** In Part 4 we’ll learn how Git stores objects *compressed* and often as *deltas* of other objects – and how to rebuild them in Go without breaking the bank.

*Stay tuned, and happy (secret) hunting!* 🚀

