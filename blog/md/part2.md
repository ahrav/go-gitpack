# 🔓 Under the Hood: Unpacking Git's Secrets
**Part 2: The Pack Index – Git's Brilliant Search Engine**

Welcome back to our journey into Git's internals! In [Part 1](#) — we set out to build a blazing-fast Git packfile parser in Go. Today, we're tackling the first piece of this puzzle: the pack index file (`.idx`).

But before we dive into code, let me share a story that perfectly illustrates why Git's design is so clever.

---

## 📚 The Library Card Catalog Problem

**Imagine you're in a massive library with millions of books, all perfectly organized…**

**Without Fanout Table (❌):**
All books alphabetized, but no section guide:

- Aardvark…
- Algebra…
- …
- Sailing…
- Science…
- …
- Zebra…

**Finding "Science":**
Binary search through *all* books
> 🔍 Check middle → too far → go left → check middle → repeat…

**With Git's Fanout Table (✅):**
Section directory + alphabetized books:

1. **📋 Section Guide:**
   - A–F: Rack 1–3
   - G–M: Rack 4–7
   - N–S: Rack 8–12
   - T–Z: Rack 13–15

2. **Rack 8–12 (N–S):**
   - Navy
   - Ocean
   - Physics
   - **Science**
   - Space

**Finding "Science":**
1️⃣ Fanout table → "S books are in Rack 8–12"
2️⃣ Binary search within that rack only! 🎯

> **💡 This is exactly how Git's pack index works!** The fanout table tells us "objects starting with 0x45 are in positions 2–5," then we binary search within just those positions instead of the entire pack.

---

## 🧠 Git's Brilliant Solution: The Pack Index

### 🔍 The Magic of the Fanout Table

Here's where Git gets clever. Instead of one giant sorted list, Git uses a **fanout table**. Let me demonstrate with actual data:

#### Example: 10 Git Objects in Our Pack

- `[0]` Hash: **12abcd…** (starts with byte `0x12`)
- `[1]` Hash: **12ffff…** (starts with byte `0x12`)
- `[2]` Hash: **45abcd…** (starts with byte `0x45`)
- `[3]` Hash: **45dead…** (starts with byte `0x45`)
- `[4]` Hash: **45feed…** (starts with byte `0x45`)
- `[5]` Hash: **89abcd…** (starts with byte `0x89`)
- `[6]` Hash: **ababab…** (starts with byte `0xab`)
- `[7]` Hash: **abcdef…** (starts with byte `0xab`)
- `[8]` Hash: **fedcba…** (starts with byte `0xfe`)
- `[9]` Hash: **ffffff…** (starts with byte `0xff`)

#### The Fanout Table (key entries)

- `0x11: 0`
- `0x12: 2`
- `0x44: 2`
- `0x45: 5`
- `0x88: 5`
- `0x89: 6`
- `0xaa: 6`
- `0xab: 8`

> **🎯 Click on any fanout entry above** to see which objects it covers! Each entry tells us: "How many objects have a first byte ≤ this value?"

---

## 🏗️ The Pack Index Format

### 📋 Pack Index File Layout

1. **Magic + Version (8 bytes)**
```
0xff744f63 + version number
```
_Safety check: "Is this really a Git index file?"_

2. **Fanout Table (1024 bytes)**
256 × 4-byte integers
_Our search accelerator – tells us where to look!_

3. **SHA-1 Hashes (20 × N bytes)**
All object hashes, sorted lexicographically
_The actual object identifiers we're searching for_

4. **CRC-32 Values (4 × N bytes)**
Checksums for data integrity
_Verify objects haven't been corrupted_

5. **32-bit Offsets (4 × N bytes)**
Where each object lives in the `.pack` file
_The treasure map to find actual object data!_

6. **Large Offset Table (Optional)**
64-bit offsets for huge packs (>2 GB)
_Because some repositories are REALLY big_

---

## 🔍 The Two-Step Search Process

### Finding Object `45dead…` Step by Step

1. **Step 1:** Extract first byte: `0x45`
2. **Step 2:** Fanout lookup: "Objects with 0x45 are in positions 2–4"
_🏃‍♂️ Jump directly to the right "rack" – no scanning needed!_
3. **Step 3:** Binary search within positions [2, 3, 4]
_📖 Books are alphabetized within the rack_
4. **Step 4:** Check middle position (3): Found `45dead…`!
_🎯 Just 1 comparison instead of potentially 10!_

```go
func (f *idxFile) findObject(targetHash Hash) (offset uint64, found bool) {
// Step 1: Extract the first byte of our target hash
firstByte := targetHash[0]

// Step 2: Use fanout to narrow our search range
searchStart := uint32(0)
if firstByte > 0 {
 searchStart = f.fanout[firstByte-1]
}
searchEnd := f.fanout[firstByte]

// Step 3: Binary search within our narrowed range
left := int(searchStart)        // 2
right := int(searchEnd) - 1     // 4

for left <= right {
 mid := (left + right) / 2
 cmp := bytes.Compare(f.oidTable[mid][:], targetHash[:])
 if cmp == 0 {
   return f.entries[mid].offset, true // Found it!
 } else if cmp < 0 {
   left = mid + 1  // Target is in upper half
 } else {
   right = mid - 1 // Target is in lower half
 }
}

return 0, false // Not found
}
```

<div>
**70%** Search Space Reduced
**1** Comparison Needed
vs 10 Without Fanout
</div>

---

## ⚙️ Parsing the Index – Step by Step

Let's parse the index file, explaining each step as we go:

### 🔍 Step 1: Verify the File Header

```go
func parseIdx(ix *mmap.ReaderAt) (*idxFile, error) {
// Step 1: Verify this is actually a Git index file
header := make([]byte, 8)
ix.ReadAt(header, 0)

// The magic bytes spell "ÿtOc" (0xff744f63) – Git's signature
if !bytes.Equal(header[0:4], []byte{0xff, 0x74, 0x4f, 0x63}) {
 return nil, fmt.Errorf("not a Git pack index file")
}

// We only understand version 2 (the current standard)
version := binary.BigEndian.Uint32(header[4:8])
if version != 2 {
 return nil, fmt.Errorf("unsupported version %d", version)
}
…
}
```

> **🛡️ Why these magic bytes?**
> Git uses them as a safety check. If you accidentally try to parse a JPEG as a pack index, this check saves you immediately!

### 📊 Step 2: Read the Fanout Table

```go
// Step 2: Read the fanout table
fanout := make([]uint32, 256)
fanoutData := make([]byte, 1024) // 256 × 4 bytes
ix.ReadAt(fanoutData, 8)         // Right after the header

// Convert from bytes to integers (Git uses big-endian)
for i := 0; i < 256; i++ {
offset := i * 4
fanout[i] = binary.BigEndian.Uint32(fanoutData[offset : offset+4])
}

// The last fanout entry tells us the total object count!
objectCount := fanout[255]
fmt.Printf("This pack contains %d objects\n", objectCount)
```

> **🔢 Big-endian format:**
> Git stores numbers in "big-endian" format (most significant byte first).
> We need to convert this to our computer's native format.

### 📍 Step 3: Calculate Section Positions

```go
// Step 3: Calculate where each section starts
hashTableStart   := int64(8 + 1024)                                // After header + fanout
crcTableStart    := hashTableStart + int64(objectCount*20)
offsetTableStart := crcTableStart + int64(objectCount*4)
```

### 🔗 Step 4: Read Object Hashes

```go
// Step 4: Read all the hashes
hashes   := make([]Hash, objectCount)
hashData := make([]byte, objectCount*20)
ix.ReadAt(hashData, hashTableStart)

// Split the continuous byte stream into individual hashes
for i := uint32(0); i < objectCount; i++ {
copy(hashes[i][:], hashData[i*20:(i+1)*20])
}
```

> **🔤 Why are the hashes stored sorted?**
> This enables binary search! With a million objects, we can find any object in just 20 comparisons instead of potentially checking all million.

### 🗂️ Step 5: Handle Large Packfiles (The 2 GB Challenge)

```go
// Step 5: Read the offset table
entries    := make([]idxEntry, objectCount)
offsetData := make([]byte, objectCount*4)
ix.ReadAt(offsetData, offsetTableStart)

var largeOffsetRefs []struct{ objIndex, largeIndex uint32 }

for i := uint32(0); i < objectCount; i++ {
offset32 := binary.BigEndian.Uint32(offsetData[i*4:(i+1)*4])

if offset32&0x80000000 == 0 {
 // Normal offset – use it directly
 entries[i].offset = uint64(offset32)
} else {
 // MSB is set – this is a large offset reference
 largeIndex := offset32 & 0x7FFFFFFF
 largeOffsetRefs = append(largeOffsetRefs, struct {
   objIndex, largeIndex uint32
 }{i, largeIndex})
}
}
```

> **🧠 The clever trick:**
> If the most significant bit is 1, the remaining 31 bits aren't an offset – they're an index into a separate "large offset" table at the end of the file. This elegantly handles huge repositories!

---

## 🔧 Putting It All Together

```go
func Open(dir string) (*Store, error) {
// Find all packfiles
packPaths, _ := filepath.Glob(filepath.Join(dir, "*.pack"))

store := &Store{ index: make(map[Hash]ref) }

for packID, packPath := range packPaths {
 // Memory-map both files for efficiency
 packFile, _ := mmap.Open(packPath)
 idxPath := strings.TrimSuffix(packPath, ".pack") + ".idx"
 idxFile, _ := mmap.Open(idxPath)

 // Parse the index
 idx, _ := parseIdx(idxFile)

 // Build our global object map
 for i, hash := range idx.oidTable {
   store.index[hash] = ref{
     packID: packID,
     offset: idx.entries[i].offset,
   }
 }
}

return store, nil
}
```

---

## 🧪 Testing Our Implementation

```go
func main() {
store, _ := Open(".git/objects/pack")

// Try to find a known object
hash, _ := ParseHash("45dead0000000000000000000000000000000000")
if ref, found := store.index[hash]; found {
 fmt.Printf("Found object in pack %d at offset %d\n", ref.packID, ref.offset)
}
}
```

---

## 🧪 Try It Yourself!

```bash
# Create a test repository
git init test-repo
cd test-repo
echo "Hello Git!" > file.txt
git add . && git commit -m "First commit"

# Force Git to create a packfile
git gc

# Examine the pack index
git verify-pack -v .git/objects/pack/*.idx

# Now run our Go code on this repository!
```

---

## 🚀 Performance: Why This Design Is Brilliant

<div>
**10 M** Objects in Large Repo
**23** Comparisons Without Fanout
**15** Comparisons With Fanout
**35 %** Improvement!
</div>

> **⚡ Memory efficiency:**
> Just 44 bytes per object (20 for hash + 4 for CRC + 4 for offset + overhead).
> That's incredibly compact for such powerful functionality!

### 📈 What We've Learned

- **The fanout table** dramatically reduces search space (like library section guides)
- **Sorted hashes** enable efficient binary search within sections
- **Memory mapping** avoids copying gigabytes of data
- **Parallel arrays** keep related data together for cache efficiency
- **Large offset handling** gracefully supports huge repositories

---

## 💾 Core Data Structures

```go
// idxFile represents a parsed Git pack index file
type idxFile struct {
// The sorted list of all object hashes in this pack
// Think of this as the library's sorted card catalog
oidTable    []Hash

// Parallel array containing the location of each object
// If oidTable[5] is hash X, then entries[5] tells us where X lives
entries     []idxEntry

// The fanout table we just discussed – our search accelerator!
fanout      [256]uint32

// For huge repositories (>2 GB packs), some offsets don't fit in 32 bits,
// so we need this overflow table
largeOffsets []uint64
}

type idxEntry struct {
offset uint64 // Where in the .pack file this object starts
crc    uint32 // Checksum to verify the object isn't corrupted
}
```

---

## 🔮 Coming Up Next

We can now find any object in a packfile, but we haven't actually read the object data yet. In **Part 3**, we'll dive into the packfile format itself, learning how to:

- Parse object headers and extract metadata
- Decompress data with zlib compression
- Handle Git's clever delta compression system
- Resolve complex delta chains efficiently

> **🎯 The real magic of Git's storage efficiency is just beginning to reveal itself!**
> We've built the index to find objects quickly, but the packfile itself contains the compressed, delta-encoded data that makes Git so space-efficient.
```
````
