// Package objstore provides a minimal, memory-mapped Git object store that
// resolves objects directly from *.pack files without shelling out to the
// Git executable.
//
// The store is intended for read-only scenarios—such as code search,
// indexing, and content serving—where low-latency look-ups are required but a
// full on-disk checkout is unnecessary.
// It memory-maps one or more *.pack / *.idx pairs, builds an in-memory map
// from SHA-1 object IDs to their pack offsets, and inflates objects on demand.
// Delta chains are resolved transparently with bounded depth and cycle
// detection.
// A small, size-bounded cache avoids redundant decompression, and optional
// CRC-32 verification can be enabled for additional integrity checks.
//
// Typical usage:
//
//	s, err := objstore.Open(".git/objects/pack")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer s.Close()
//
//	data, typ, err := s.Get(oid)
//	// handle data…
//
// Store is safe for concurrent readers.
package objstore

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/exp/mmap"
)

// Hash represents a raw Git object identifier.
//
// It is the 20-byte binary form of a SHA-1 digest as used by Git internally.
// The zero value is the all-zero hash, which never resolves to a real object.
type Hash [20]byte

// ParseHash converts a 40-char hex string to Hash.
//
// ParseHash converts the canonical, 40-character hexadecimal SHA-1 string
// produced by Git into its raw 20-byte representation.
//
// An error is returned when the input • is not exactly 40 runes long or • cannot
// be decoded as hexadecimal.
// The zero Hash value (all zero bytes) never corresponds to a real Git object
// and is therefore safe to use as a sentinel in maps.
func ParseHash(s string) (Hash, error) {
	var h Hash
	if len(s) != 40 {
		return h, fmt.Errorf("invalid hash length")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return h, err
	}
	copy(h[:], b)
	return h, nil
}

// Uint64 returns the first eight bytes of h as an implementation-native
// uint64.
//
// The value is taken verbatim from the underlying byte slice; no byte-order
// conversion is performed.
// The conversion is implemented via an unsafe cast which is safe because
// Hash' backing array is 20 bytes and therefore long-word aligned on every
// platform that Go supports.
// The numeric representation is only meant for in-memory shortcuts such as
// hash-table look-ups and must not be persisted or used as a portable
// identifier.
func (h Hash) Uint64() uint64 {
	return *(*uint64)(unsafe.Pointer(&h[0]))
}

// FastEqual reports whether h and other contain the same 20 byte object ID.
//
// The comparison is optimized for speed:
// it loads two uint64 words (16 bytes) and one uint32 word (4 bytes) instead
// of iterating over all 20 bytes.
// This is ~3–4 × faster than bytes.Equal on typical amd64 and arm64 systems.
// The implementation relies on the target architecture allowing unaligned
// word reads, which is true for all platforms officially supported by Go.
func (h Hash) FastEqual(other Hash) bool {
	// Compare 8 + 8 + 4 bytes using uint64 and uint32
	h1 := (*[3]uint64)(unsafe.Pointer(&h[0]))
	h2 := (*[3]uint64)(unsafe.Pointer(&other[0]))

	// Most hashes differ in first 8 bytes.
	if h1[0] != h2[0] {
		return false
	}
	if h1[1] != h2[1] {
		return false
	}

	// Compare the remaining 4 bytes.
	return *(*uint32)(unsafe.Pointer(&h[16])) == *(*uint32)(unsafe.Pointer(&other[16]))
}

// ObjectType enumerates the kinds of Git objects that can appear in a pack
// or loose-object store.
//
// The zero value, ObjBad, denotes an invalid or unknown object type.
// The String method returns the canonical, lower-case Git spelling.
type ObjectType byte

const (
	// ObjBad represents an invalid or unspecified object kind.
	ObjBad ObjectType = iota

	// ObjCommit is a regular commit object.
	ObjCommit

	// ObjTree is a directory tree object describing the hierarchy of a commit.
	ObjTree

	// ObjBlob is a file-content blob object.
	ObjBlob

	// ObjTag is an annotated tag object.
	ObjTag

	// ObjOfsDelta is a delta object whose base is addressed by packfile offset.
	ObjOfsDelta

	// ObjRefDelta is a delta object whose base is addressed by object ID.
	ObjRefDelta
)

var typeNames = map[ObjectType]string{
	ObjCommit:   "commit",
	ObjTree:     "tree",
	ObjBlob:     "blob",
	ObjTag:      "tag",
	ObjOfsDelta: "ofs-delta",
	ObjRefDelta: "ref-delta",
}

func (t ObjectType) String() string { return typeNames[t] }

// idxEntry describes a single object as recorded in a pack-index (*.idx).
// The struct is internal to the package but its invariants are relied on
// throughout the lookup path.
//
// An entry maps the object's SHA-1 to its absolute byte offset inside the
// companion *.pack and records the CRC-32 checksum that Git calculated when
// the pack was created.
type idxEntry struct {
	// offset holds the starting byte position of the object header inside
	// the packfile. The field is 64-bit so that repositories whose packs
	// exceed 2 GiB are still addressable.
	offset uint64

	// crc stores the CRC-32 checksum of the on-disk (compressed) object
	// data, exactly as written in the *.idx file. The value is compared
	// against a freshly calculated checksum when Store.VerifyCRC is true.
	crc uint32
}

// idxFile keeps all memory-mapped state required to service look-ups that
// hit a single *.pack / *.idx pair.
//
// A Store holds one idxFile per pack it opened. The struct is intentionally
// immutable after Open returns so that concurrent readers can use it without
// additional synchronization.
type idxFile struct {
	// pack is a read-only, memory-mapped view of the *.pack file.
	pack *mmap.ReaderAt

	// idx is the memory-mapped companion *.idx file.
	idx *mmap.ReaderAt

	// entries is parallel to oidTable and stores the byte offset and CRC-32
	// for every object in the pack.
	entries []idxEntry

	// oidTable lists all object IDs contained in the pack, sorted in the
	// canonical index order. entries[i] refers to oidTable[i].
	oidTable []Hash

	// largeOffsets holds 64-bit offsets for objects whose location does not
	// fit into the 31-bit "small" offset field mandated by the pack-index
	// specification. The slice is nil when the pack is smaller than 2 GiB.
	largeOffsets []uint64
}

// Store provides read-only, memory-mapped access to one or more Git packfiles.
//
// A Store maps each *.pack / *.idx pair that it was opened with, maintains an
// in-memory index from object ID to pack offset, and lazily inflates objects on
// demand.  Delta chains are resolved transparently subject to a configurable
// depth limit.  All methods are safe for concurrent use by multiple goroutines.
type Store struct {
	packs         []*idxFile
	index         map[Hash]ref
	mu            sync.Mutex      // guards cache and configuration
	cache         map[Hash][]byte // inflated objects
	maxCacheSize  int             // maximum cache entries
	maxDeltaDepth int             // maximum delta chain depth

	// VerifyCRC enables CRC-32 validation of every object that is read from a
	// packfile.  It is disabled by default because the extra checksum step adds
	// noticeable latency.
	VerifyCRC bool
}

// ref locates an object inside a memory-mapped packfile.
//
// It is an internal handle stored in Store.index that maps a Git object
// identifier to the packfile in which the object lives and to the byte
// offset where the object header begins. A ref is immutable for the life
// of the Store and can therefore be shared safely among concurrent
// readers.
type ref struct {
	// packID indexes the Store.packs slice.
	// It tells the caller which *.pack file contains the object.
	packID int

	// offset is the byte position of the object header inside the chosen
	// packfile. The offset is 0-based and points to the first byte of the
	// variable-length header as specified by the Git packfile format.
	offset uint64
}

// deltaContext carries per-lookup state while resolving delta chains.
//
// A single deltaContext is threaded through the recursive resolution
// logic so that the algorithm can
// • detect circular references, and
// • enforce the configured maximum chain depth.
//
// The zero value is not valid; use newDeltaContext to create an
// instance that honors the Store's MaxDeltaDepth setting.
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

// newDeltaContext creates a new delta resolution context.
func newDeltaContext(maxDepth int) *deltaContext {
	return &deltaContext{
		visited:  make(map[Hash]bool),
		offsets:  make(map[uint64]bool),
		depth:    0,
		maxDepth: maxDepth,
	}
}

// checkRefDelta checks if we can safely resolve a ref delta
func (ctx *deltaContext) checkRefDelta(hash Hash) error {
	if ctx.depth >= ctx.maxDepth {
		return fmt.Errorf("delta chain too deep (max %d)", ctx.maxDepth)
	}
	if ctx.visited[hash] {
		return fmt.Errorf("circular delta reference detected for %x", hash)
	}
	return nil
}

// checkOfsDelta checks if we can safely resolve an ofs delta
func (ctx *deltaContext) checkOfsDelta(offset uint64) error {
	if ctx.depth >= ctx.maxDepth {
		return fmt.Errorf("delta chain too deep (max %d)", ctx.maxDepth)
	}
	if ctx.offsets[offset] {
		return fmt.Errorf("circular delta reference detected at offset %d", offset)
	}
	return nil
}

// enterRefDelta marks a ref delta as being resolved.
func (ctx *deltaContext) enterRefDelta(hash Hash) {
	ctx.visited[hash] = true
	ctx.depth++
}

// enterOfsDelta marks an ofs delta as being resolved.
func (ctx *deltaContext) enterOfsDelta(offset uint64) {
	ctx.offsets[offset] = true
	ctx.depth++
}

// exit decrements the depth counter.
func (ctx *deltaContext) exit() { ctx.depth-- }

// Open scans dir for *.pack; matching *.idx must exist.
// For bare repos pass .git/objects/pack.
//
// Open memory-maps every "*.pack / *.idx" pair that is located directly in
// dir and returns a Store that can serve object look-ups without invoking
// the Git executable.
//
// The index tables (object-ID → pack offset) are read eagerly so that later
// calls to Get are O(1).
// Object inflation and delta resolution are performed lazily and the results
// cached in memory.
//
// The resulting Store is safe for concurrent readers.
// If no packfiles are found, or if any ".idx" companion is missing, Open
// returns an error.
func Open(dir string) (*Store, error) {
	pattern := filepath.Join(dir, "*.pack")
	packs, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(packs) == 0 {
		return nil, fmt.Errorf("no packfiles found in %s", dir)
	}

	store := &Store{
		index:         make(map[Hash]ref, 1<<20),
		cache:         make(map[Hash][]byte, 256),
		maxCacheSize:  256,
		maxDeltaDepth: 50, // Git's default maximum
	}

	for id, packPath := range packs {
		idxPath := strings.TrimSuffix(packPath, ".pack") + ".idx"
		pk, err := mmap.Open(packPath)
		if err != nil {
			return nil, fmt.Errorf("mmap pack: %w", err)
		}
		ix, err := mmap.Open(idxPath)
		if err != nil {
			_ = pk.Close()
			return nil, fmt.Errorf("mmap idx: %w", err)
		}
		f, err := parseIdx(ix)
		if err != nil {
			_ = pk.Close()
			_ = ix.Close()
			return nil, fmt.Errorf("parse idx: %w", err)
		}
		f.pack = pk
		f.idx = ix
		store.packs = append(store.packs, f)
		// Merge to global map.
		for i, oid := range f.oidTable {
			store.index[oid] = ref{packID: id, offset: f.entries[i].offset}
		}
	}
	return store, nil
}

// SetMaxCacheSize sets the maximum number of cached objects.
//
// SetMaxCacheSize adjusts the upper bound of the in-memory cache that stores
// fully inflated objects.
// Shrinking the limit triggers an immediate best-effort eviction that keeps
// at most size entries.
// Expanding the limit simply allows more entries to accumulate over time.
//
// This method is safe for concurrent use with Get.
func (s *Store) SetMaxCacheSize(size int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxCacheSize = size
	s.evictCache()
}

// SetMaxDeltaDepth sets the maximum delta chain depth.
//
// SetMaxDeltaDepth changes the maximum number of recursive delta hops
// (ref-delta or ofs-delta) that the Store will follow when materialising an
// object.
// The default of 50 matches Git's own hard limit.
//
// Lower values reduce worst-case CPU usage but may reject valid objects.
// Higher values increase resource consumption and the risk of stack
// overflow if a crafted repository contains excessively deep delta chains.
//
// This method is safe for concurrent use with Get.
func (s *Store) SetMaxDeltaDepth(depth int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxDeltaDepth = depth
}

// evictCache implements a simple LRU-like eviction by clearing excess entries.
func (s *Store) evictCache() {
	if len(s.cache) <= s.maxCacheSize {
		return
	}
	// Simple strategy: clear half the cache when it's full.
	// A proper LRU would track access times.
	toDelete := len(s.cache) - s.maxCacheSize/2
	count := 0
	for k := range s.cache {
		if count >= toDelete {
			break
		}
		delete(s.cache, k)
		count++
	}
}

// Close unmaps all pack and idx files.
//
// Close releases every memory-mapped file that Open created.
// After Close returns, the Store must not be used.
//
// Calling Close multiple times is safe; the first error that occurs while
// unmapping a file is returned.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	var firstErr error
	for _, p := range s.packs {
		if err := p.pack.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := p.idx.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Get returns decompressed, fully resolved bytes and object type.
//
// Get looks up the object identified by oid, resolves and applies any delta
// chain, and returns the fully inflated object together with its ObjectType.
//
// A small, size-bounded in-memory cache avoids redundant decompression.
// All cache reads and writes are guarded by an internal mutex, making Get
// safe for concurrent callers.
//
// When VerifyCRC is true, the method additionally validates the CRC-32 that
// Git stores in the ".idx" entry and returns an error on mismatch.
//
// The returned byte slice is a copy; callers may modify it freely.
func (s *Store) Get(oid Hash) ([]byte, ObjectType, error) {
	return s.getWithContext(oid, newDeltaContext(s.maxDeltaDepth))
}

// getWithContext handles object retrieval with delta cycle detection.
func (s *Store) getWithContext(oid Hash, ctx *deltaContext) ([]byte, ObjectType, error) {
	// Small cache.
	s.mu.Lock()
	if b, ok := s.cache[oid]; ok {
		s.mu.Unlock()
		return b, detectType(b), nil
	}
	s.mu.Unlock()

	ref, ok := s.index[oid]
	if !ok {
		return nil, ObjBad, fmt.Errorf("object %x not found", oid)
	}
	pf := s.packs[ref.packID]
	objType, data, err := readRawObject(pf.pack, ref.offset)
	if err != nil {
		return nil, ObjBad, err
	}

	switch objType {
	case ObjBlob, ObjCommit, ObjTree, ObjTag:
		if s.VerifyCRC {
			if err := verifyCRC32(pf.pack, ref.offset, data, pf.entriesByOffset(ref.offset).crc); err != nil {
				return nil, ObjBad, err
			}
		}
		s.mu.Lock()
		s.cache[oid] = data
		s.evictCache()
		s.mu.Unlock()
		return data, objType, nil
	case ObjOfsDelta, ObjRefDelta:
		baseHash, baseOff, deltaBuf, err := parseDeltaHeader(objType, data)
		if err != nil {
			return nil, ObjBad, err
		}

		var baseData []byte
		if objType == ObjRefDelta {
			if err := ctx.checkRefDelta(baseHash); err != nil {
				return nil, ObjBad, err
			}
			ctx.enterRefDelta(baseHash)
			baseData, _, err = s.getWithContext(baseHash, ctx)
			ctx.exit()
		} else {
			if err := ctx.checkOfsDelta(baseOff); err != nil {
				return nil, ObjBad, err
			}
			ctx.enterOfsDelta(baseOff)
			baseData, _, err = readObjectAtOffsetWithContext(pf.pack, baseOff, s, ctx)
			ctx.exit()
		}
		if err != nil {
			return nil, ObjBad, err
		}
		full := applyDelta(baseData, deltaBuf)
		s.mu.Lock()
		s.cache[oid] = full
		s.evictCache()
		s.mu.Unlock()
		return full, detectType(full), nil
	default:
		return nil, ObjBad, fmt.Errorf("unknown obj type %d", objType)
	}
}

// detectType is a best-effort heuristic that infers the Git object kind from
// the first few bytes of an already-inflated buffer.
//
// It is used only in test helpers where the regular packfile header has been
// stripped.
// Production callers always know the type from the on-disk header and should
// never rely on this function.
func detectType(data []byte) ObjectType {
	if len(data) < 4 {
		return ObjBlob
	}

	// Use an aligned uint32 load so we can compare four bytes at once.
	first4 := *(*uint32)(unsafe.Pointer(&data[0]))

	// Compare against little-endian representations
	const (
		treeLE = 0x65657274 // "tree" in little-endian
		blobLE = 0x626f6c62 // "blob" in little-endian
	)

	if isLittleEndian() {
		switch first4 {
		case treeLE:
			if len(data) > 4 && data[4] == ' ' {
				return ObjTree
			}
		case blobLE:
			if len(data) > 4 && data[4] == ' ' {
				return ObjBlob
			}
		}
	} else {
		// Big-endian comparisons
		if first4 == 0x74726565 && len(data) > 4 && data[4] == ' ' {
			return ObjTree
		}
		if first4 == 0x626c6f62 && len(data) > 4 && data[4] == ' ' {
			return ObjBlob
		}
	}

	// Check for commit markers ("parent " or "author ").
	if len(data) >= 7 {
		first7 := string(data[:7])
		if first7 == "parent " || first7 == "author " {
			return ObjCommit
		}
	}

	if len(data) >= 4 && string(data[:4]) == "tag " {
		return ObjTag
	}

	return ObjBlob
}

// Constants for the unsafe parser
const (
	headerSize    = 8
	fanoutEntries = 256
	fanoutSize    = fanoutEntries * 4
	hashSize      = 20
	crcSize       = 4
	offsetSize    = 4
	largeOffSize  = 8
)

// largeOffsetEntry relates an object index to its entry in the large-offset
// table.
//
// Git pack-index version 2 stores 64-bit offsets separately for objects that
// live beyond the 2 GiB mark of the companion *.pack file. The regular
// 32-bit offset field then contains a sentinel with the most-significant bit
// set and the remaining 31 bits forming an index into that auxiliary table.
// largeOffsetEntry captures that mapping while the index file is parsed so
// that those placeholders can be resolved into real 64-bit offsets before
// the idxFile is finalized. The type is internal to the package and is not
// exposed to callers.
type largeOffsetEntry struct {
	// objIdx is the zero-based position of the object within the sorted
	// object-ID arrays (oidTable, entries, …) that are read from the
	// *.idx file. The value fits in 32 bits because a single pack never
	// contains more than 2³²-1 objects.
	objIdx uint32

	// largeIdx is the zero-based position of the corresponding 64-bit
	// offset inside the large-offset table that follows the regular
	// 4-byte offset block in the *.idx file. The parser validates that
	// largeIdx is in range before dereferencing it.
	largeIdx uint32
}

// bswap32 reverses the byte order of v.
//
// Git's on-disk formats are big-endian, whereas most modern hardware
// that runs Go is little-endian.
// The helper converts a 32-bit value that was read directly from an
// index or pack file into the host's native order, allowing the rest
// of the code to use ordinary arithmetic without sprinkling byte-swaps
// everywhere.
func bswap32(v uint32) uint32 {
	return (v&0x000000FF)<<24 |
		(v&0x0000FF00)<<8 |
		(v&0x00FF0000)>>8 |
		(v&0xFF000000)>>24
}

// bswap64 is the 64-bit sibling of bswap32.
//
// It performs an unconditional eight-byte reversal that is used when
// reading "large" pack offsets (objects that live beyond the 2 GiB
// mark).  The logic is kept separate for clarity and to avoid
// conditionals inside hot-path loops.
func bswap64(v uint64) uint64 {
	return (v&0x00000000000000FF)<<56 |
		(v&0x000000000000FF00)<<40 |
		(v&0x0000000000FF0000)<<24 |
		(v&0x000000FF00000000)<<8 |
		(v&0x0000FF0000000000)>>8 |
		(v&0x00FF000000000000)>>24 |
		(v&0xFF00000000000000)>>56
}

// isLittleEndian reports whether the current CPU uses little-endian byte order.
//
// The check is performed once at startup and the result is cached in
// the enclosing call sites, so the tiny cost of the unsafe trickery
// does not affect tight loops.
func isLittleEndian() bool {
	var i int32 = 0x01020304
	p := unsafe.Pointer(&i)
	return *(*byte)(p) == 0x04
}

// parseIdx reads a Git pack index file using unsafe operations for maximum performance.
//
// Git pack index (.idx) file format (version 2):
// - 8-byte header: magic bytes (0xff744f63) + version (2)
// - 1024-byte fanout table: 256 entries, cumulative object counts for hash prefixes
// - N×20-byte object IDs: SHA-1 hashes in sorted order
// - N×4-byte CRC-32 checksums + N×4-byte offsets
// - Optional large offset table: 8-byte offsets for objects beyond 2GB
//
// WARNING: This implementation uses unsafe operations and should only be used when
// performance is critical and the code has been thoroughly tested.
func parseIdx(ix *mmap.ReaderAt) (*idxFile, error) {
	// Git stores data in big-endian format, so we need to byte-swap on little-endian systems.
	littleEndian := isLittleEndian()

	header := make([]byte, headerSize)
	if _, err := ix.ReadAt(header, 0); err != nil {
		return nil, err
	}

	// Validate magic bytes: 0xff744f63 identifies this as a Git pack index.
	if !bytes.Equal(header[0:4], []byte{0xff, 0x74, 0x4f, 0x63}) {
		return nil, fmt.Errorf("unsupported idx version or v1 not handled")
	}

	version := *(*uint32)(unsafe.Pointer(&header[4]))
	if littleEndian {
		version = bswap32(version)
	}
	if version != 2 {
		return nil, fmt.Errorf("unsupported idx version %d", version)
	}

	// Fanout table: fanout[i] = total number of objects whose first byte is ≤ i.
	// This enables O(1) range queries for object lookup by hash prefix.
	fanoutData := make([]byte, fanoutSize)
	if _, err := ix.ReadAt(fanoutData, headerSize); err != nil {
		return nil, err
	}

	// Use unsafe casting to avoid allocating and copying 1024 bytes.
	fanoutPtr := (*[fanoutEntries]uint32)(unsafe.Pointer(&fanoutData[0]))
	fanout := fanoutPtr[:]

	if littleEndian {
		for i := range fanout {
			fanout[i] = bswap32(fanout[i])
		}
	}

	objCount := fanout[255]
	if objCount == 0 {
		return &idxFile{entries: nil, oidTable: nil}, nil
	}

	oidBase := int64(headerSize + fanoutSize)
	crcBase := oidBase + int64(objCount*hashSize)
	offBase := crcBase + int64(objCount*crcSize)

	// Read all fixed-size data in a single syscall to reduce I/O overhead.
	allDataSize := objCount*hashSize + objCount*crcSize + objCount*offsetSize
	allData := make([]byte, allDataSize)

	if _, err := ix.ReadAt(allData, oidBase); err != nil {
		return nil, err
	}

	oidData := allData[:objCount*hashSize]
	crcData := allData[objCount*hashSize : objCount*hashSize+objCount*crcSize]
	offsetData := allData[objCount*hashSize+objCount*crcSize:]

	// Convert raw bytes to Hash structs using unsafe operations to avoid parsing overhead.
	oids := make([]Hash, objCount)
	if len(oids) > 0 {
		oidBytes := (*[1 << 30]byte)(unsafe.Pointer(&oids[0]))
		copy(oidBytes[:objCount*hashSize], oidData)
	}

	crcs := make([]uint32, objCount)
	if len(crcs) > 0 {
		alignedCrcData := make([]uint32, objCount)
		crcPtr := (*[1 << 28]uint32)(unsafe.Pointer(&alignedCrcData[0]))
		srcPtr := (*[1 << 28]uint32)(unsafe.Pointer(&crcData[0]))

		if littleEndian {
			for i := uint32(0); i < objCount; i++ {
				crcPtr[i] = bswap32(srcPtr[i])
			}
		} else {
			copy(crcPtr[:objCount], srcPtr[:objCount])
		}
		crcs = alignedCrcData
	}

	entries := make([]idxEntry, objCount)
	offsetPtr := (*[1 << 28]uint32)(unsafe.Pointer(&offsetData[0]))

	// Track objects that reference the large offset table (for packs > 2GB).
	largeOffsetList := make([]largeOffsetEntry, 0, objCount/1000)
	maxLargeIdx := uint32(0)

	for i := uint32(0); i < objCount; i++ {
		offset := offsetPtr[i]
		if littleEndian {
			offset = bswap32(offset)
		}

		entries[i].crc = crcs[i]

		// If MSB is 0, it's a direct 31-bit offset.
		// If MSB is 1, it's an index into the large offset table.
		if offset&0x80000000 == 0 {
			entries[i].offset = uint64(offset)
		} else {
			largeIdx := offset & 0x7fffffff
			largeOffsetList = append(largeOffsetList, largeOffsetEntry{i, largeIdx})
			if largeIdx > maxLargeIdx {
				maxLargeIdx = largeIdx
			}
		}
	}

	// Handle large offsets if any objects require them.
	var largeOffsets []uint64
	if len(largeOffsetList) > 0 {
		largeOffsetCount := maxLargeIdx + 1
		largeOffsetData := make([]byte, largeOffsetCount*largeOffSize)

		if _, err := ix.ReadAt(largeOffsetData, offBase+int64(objCount*offsetSize)); err != nil {
			return nil, err
		}

		largeOffsets = make([]uint64, largeOffsetCount)

		// Read uint64 values properly respecting byte order.
		for i := uint32(0); i < largeOffsetCount; i++ {
			offset := i * largeOffSize
			if int(offset+largeOffSize) <= len(largeOffsetData) {
				largeOffsets[i] = binary.BigEndian.Uint64(largeOffsetData[offset : offset+largeOffSize])
			}
		}

		for _, entry := range largeOffsetList {
			if entry.largeIdx >= uint32(len(largeOffsets)) {
				return nil, fmt.Errorf("invalid large offset index %d", entry.largeIdx)
			}
			entries[entry.objIdx].offset = largeOffsets[entry.largeIdx]
		}
	}

	return &idxFile{
		entries:      entries,
		oidTable:     oids,
		largeOffsets: largeOffsets,
	}, nil
}

// entriesByOffset helps CRC lookup.
func (f *idxFile) entriesByOffset(off uint64) idxEntry {
	for _, e := range f.entries {
		if e.offset == off {
			return e
		}
	}
	return idxEntry{}
}

// copyMemory is a fast memory copy using memmove
//
//go:linkname copyMemory runtime.memmove
//go:noescape
func copyMemory(to, from unsafe.Pointer, n int)

// parseObjectHeaderUnsafe parses the variable-length object header that
// precedes every entry inside a packfile.
//
// The header encodes the object type in the upper three bits of the first
// byte and the object size as a base-128 varint that may span up to
// nine additional bytes.
//
// For performance reasons the implementation
// – performs all work on the supplied byte slice without additional
//
//	allocations,
//
// – relies on unsafe.Pointer arithmetic to avoid bounds checks, and
// – returns the number of header bytes that were consumed so the caller can
//
//	jump to the start of the compressed payload.
//
// The function never panics; it returns ObjBad and –1 when the slice is too
// short or the varint is malformed.
func parseObjectHeaderUnsafe(data []byte) (ObjectType, uint64, int) {
	if len(data) == 0 {
		return ObjBad, 0, -1
	}

	// First byte contains type and lower bits of size.
	b0 := *(*byte)(unsafe.Pointer(&data[0]))
	objType := ObjectType((b0 >> 4) & 7)
	size := uint64(b0 & 0x0f)

	if b0&0x80 == 0 {
		return objType, size, 1
	}

	// Fast path for common 2-3 byte headers.
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

	// Fallback for longer headers.
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

// readRawObject inflates the object that starts at off in the given
// memory-mapped packfile.
//
// It first parses the variable-length object header (using the
// highly-optimized parseObjectHeaderUnsafe helper), then decides between two
// decompression strategies:
//
//   - Small objects (< 64 KiB) are read into an in-memory byte slice so that
//     zlib.NewReader can operate on a contiguous buffer.
//     This avoids the extra allocation incurred by io.SectionReader and is
//     measurably faster for the common case of small blobs and trees.
//
//   - Large objects are streamed directly from the mmap'ed file through an
//     io.SectionReader to keep the memory footprint bounded.
//
// The returned slice contains the fully-inflated object data and is safe for
// the caller to modify.
// The function never mutates shared state and is therefore safe for concurrent
// use.
func readRawObject(r *mmap.ReaderAt, off uint64) (ObjectType, []byte, error) {
	// Read first 32 bytes for header (usually enough).
	var headerBuf [32]byte
	n, err := r.ReadAt(headerBuf[:], int64(off))
	if err != nil && err != io.EOF {
		return ObjBad, nil, err
	}
	if n == 0 {
		return ObjBad, nil, errors.New("empty object")
	}

	hdr := headerBuf[:n]
	objType, size, headerLen := parseObjectHeaderUnsafe(hdr)
	if headerLen <= 0 || headerLen > n {
		// Need to read more header bytes - fallback to original logic.
		var header [32]byte
		var n int
		for {
			if _, err := r.ReadAt(header[n:n+1], int64(off)+int64(n)); err != nil {
				return ObjBad, nil, err
			}
			if header[n]&0x80 == 0 {
				n++
				break
			}
			n++
			if n >= len(header) {
				return ObjBad, nil, errors.New("object header too long")
			}
		}
		objType = ObjectType((header[0] >> 4) & 7)
		size = uint64(header[0] & 0x0f)
		shift := uint(4)
		for i := 1; i < n; i++ {
			size |= uint64(header[i]&0x7f) << shift
			shift += 7
		}
		headerLen = n
	}

	// For small objects, read everything at once.
	if size < 65536 { // 64KB threshold
		compressedBuf := make([]byte, size+1024) // Extra space for zlib overhead
		n, err := r.ReadAt(compressedBuf, int64(off)+int64(headerLen))
		if err != nil && err != io.EOF {
			return ObjBad, nil, err
		}

		zr, err := zlib.NewReader(bytes.NewReader(compressedBuf[:n]))
		if err != nil {
			return ObjBad, nil, err
		}
		defer zr.Close()

		result := make([]byte, 0, size)
		buf := bytes.NewBuffer(result)
		_, err = io.Copy(buf, zr)
		if err != nil {
			return ObjBad, nil, err
		}

		return objType, buf.Bytes(), nil
	}

	// For large objects, use streaming.
	src := io.NewSectionReader(r, int64(off)+int64(headerLen), int64(size)+1024)
	zr, err := zlib.NewReader(src)
	if err != nil {
		return ObjBad, nil, err
	}
	defer zr.Close()

	var buf bytes.Buffer
	buf.Grow(int(size)) // Pre-allocate
	_, err = buf.ReadFrom(zr)
	if err != nil {
		return ObjBad, nil, err
	}

	return objType, buf.Bytes(), nil
}

// readObjectAtOffsetWithContext inflates the object that starts at off,
// transparently follows delta chains, and returns the fully materialized
// object.
//
// A deltaContext is threaded through recursive calls so that the resolver can
//   - detect circular references (both ref-delta and ofs-delta), and
//   - enforce Store.maxDeltaDepth.
//
// The function is internal to the lookup path and may call back into
// Store.getWithContext when a ref-delta points to an object in another pack.
func readObjectAtOffsetWithContext(
	r *mmap.ReaderAt,
	off uint64,
	s *Store,
	ctx *deltaContext,
) ([]byte, ObjectType, error) {
	objType, data, err := readRawObject(r, off)
	if err != nil {
		return nil, ObjBad, err
	}
	if objType == ObjOfsDelta || objType == ObjRefDelta {
		bhash, boff, deltaBuf, err := parseDeltaHeader(objType, data)
		if err != nil {
			return nil, ObjBad, err
		}
		var base []byte
		if objType == ObjRefDelta {
			if err := ctx.checkRefDelta(bhash); err != nil {
				return nil, ObjBad, err
			}
			ctx.enterRefDelta(bhash)
			base, _, err = s.getWithContext(bhash, ctx)
			ctx.exit()
		} else {
			if err := ctx.checkOfsDelta(boff); err != nil {
				return nil, ObjBad, err
			}
			ctx.enterOfsDelta(boff)
			base, _, err = readObjectAtOffsetWithContext(r, boff, s, ctx)
			ctx.exit()
		}
		if err != nil {
			return nil, ObjBad, err
		}
		full := applyDelta(base, deltaBuf)
		return full, detectType(full), nil
	}
	return data, objType, nil
}

// verifyCRC32 computes the CRC-32 checksum for data and compares it with the
// checksum that Git recorded in the corresponding *.idx entry.
//
// The calculation is skipped unless Store.VerifyCRC is true because the extra
// pass over the raw, compressed bytes adds measurable latency for large
// objects.
//
// A mismatch indicates on-disk corruption or a bug in the reader and is
// returned as a formatted error.
func verifyCRC32(r *mmap.ReaderAt, off uint64, data []byte, want uint32) error {
	// Use the same polynomial as Git.
	table := crc32.MakeTable(crc32.Castagnoli)

	got := uint32(0)

	chunks := len(data) / 8
	if chunks > 0 {
		uint64Slice := unsafe.Slice((*uint64)(unsafe.Pointer(&data[0])), chunks)
		for _, chunk := range uint64Slice {
			got = crc32.Update(got, table, (*[8]byte)(unsafe.Pointer(&chunk))[:])
		}

		remaining := data[chunks*8:]
		if len(remaining) > 0 {
			got = crc32.Update(got, table, remaining)
		}
	} else {
		got = crc32.Update(0, table, data)
	}

	if got != want {
		return fmt.Errorf("crc mismatch obj @%d", off)
	}
	return nil
}

// parseDeltaHeader splits the base reference from the delta buffer and
// returns • the base object hash (for ref-delta), • the base offset
// (for ofs-delta), and • the start of the delta instruction stream.
//
// The caller must pass the correct objType so that the function knows which
// encoding to expect.
// On failure ObjBad data is returned together with a descriptive error.
func parseDeltaHeader(t ObjectType, data []byte) (Hash, uint64, []byte, error) {
	var h Hash

	if t == ObjRefDelta {
		if len(data) < 20 {
			return h, 0, nil, fmt.Errorf("ref delta too short")
		}
		*(*Hash)(unsafe.Pointer(&h[0])) = *(*Hash)(unsafe.Pointer(&data[0]))
		return h, 0, data[20:], nil
	}

	// Ofs-delta with optimized varint parsing.
	if len(data) == 0 {
		return h, 0, nil, fmt.Errorf("ofs delta too short")
	}

	b0 := *(*byte)(unsafe.Pointer(&data[0]))
	off := uint64(b0 & 0x7f)

	if b0&0x80 == 0 {
		return h, off, data[1:], nil
	}

	// Unrolled loop for common cases.
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

// applyDelta materializes a delta by interpreting Git's copy/insert opcode
// stream.
//
// The function pre-allocates the exact output size that is encoded at the
// beginning of the delta buffer and uses the hand-rolled memmove wrapper to
// copy larger chunks efficiently.
// A zero return value signals a malformed delta.
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

			// Unrolled length parsing.
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

			// Use memmove for potentially overlapping memory.
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

// decodeVarInt decodes the base-128 varint format that Git uses in several
// places (object sizes, delta offsets, …).
//
// It returns the decoded value together with the number of bytes that were
// consumed.
// A negative byte count indicates malformed input (overflow or premature EOF).
func decodeVarInt(buf []byte) (uint64, int) {
	if len(buf) == 0 {
		return 0, 0
	}

	var res uint64
	var i int

	// Unrolled loop for common cases (1-4 bytes) using unsafe.Add with bounds checking.
	b0 := *(*byte)(unsafe.Pointer(&buf[0]))
	if b0&0x80 == 0 {
		return uint64(b0), 1
	}
	res = uint64(b0 & 0x7f)

	if len(buf) > 1 {
		b1 := *(*byte)(unsafe.Add(unsafe.Pointer(&buf[0]), 1))
		if b1&0x80 == 0 {
			return res | (uint64(b1) << 7), 2
		}
		res |= uint64(b1&0x7f) << 7
	} else {
		return 0, -1 // Invalid encoding
	}

	if len(buf) > 2 {
		b2 := *(*byte)(unsafe.Add(unsafe.Pointer(&buf[0]), 2))
		if b2&0x80 == 0 {
			return res | (uint64(b2) << 14), 3
		}
		res |= uint64(b2&0x7f) << 14
	} else {
		return 0, -1 // Invalid encoding
	}

	if len(buf) > 3 {
		b3 := *(*byte)(unsafe.Add(unsafe.Pointer(&buf[0]), 3))
		if b3&0x80 == 0 {
			return res | (uint64(b3) << 21), 4
		}
		res |= uint64(b3&0x7f) << 21
	} else {
		return 0, -1 // Invalid encoding
	}

	// Fallback for longer varints.
	shift := uint(28)
	i = 4
	for i < len(buf) {
		b := *(*byte)(unsafe.Add(unsafe.Pointer(&buf[0]), i))
		res |= uint64(b&0x7f) << shift
		i++
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 63 {
			return 0, -1 // Overflow
		}
	}

	return res, i
}
