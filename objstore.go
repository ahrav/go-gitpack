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
		} else { // ofs-delta
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

// detectType is a heuristic for tests; real callers know type from header.
func detectType(data []byte) ObjectType {
	if bytes.HasPrefix(data, []byte("tree ")) {
		return ObjTree
	}
	if bytes.HasPrefix(data, []byte("parent ")) ||
		bytes.HasPrefix(data, []byte("author ")) {
		return ObjCommit
	}
	return ObjBlob // fallback
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

// largeOffsetEntry tracks objects that reference the large offset table
type largeOffsetEntry struct {
	objIdx   uint32
	largeIdx uint32
}

// bswap32 swaps byte order for uint32 (for little-endian systems)
func bswap32(v uint32) uint32 {
	return (v&0x000000FF)<<24 | (v&0x0000FF00)<<8 |
		(v&0x00FF0000)>>8 | (v&0xFF000000)>>24
}

// bswap64 swaps byte order for uint64 (for little-endian systems)
func bswap64(v uint64) uint64 {
	return (v&0x00000000000000FF)<<56 | (v&0x000000000000FF00)<<40 |
		(v&0x0000000000FF0000)<<24 | (v&0x00000000FF000000)<<8 |
		(v&0x000000FF00000000)>>8 | (v&0x0000FF0000000000)>>24 |
		(v&0x00FF000000000000)>>40 | (v&0xFF00000000000000)>>56
}

// isLittleEndian detects system endianness
func isLittleEndian() bool {
	var i int32 = 0x01020304
	u := unsafe.Pointer(&i)
	pb := (*byte)(u)
	return *pb == 0x04
}

// parseIdx reads a Git pack index file using unsafe operations for maximum performance.
//
// Git pack index (.idx) file format (version 2):
// - 8-byte header: magic bytes (0xff744f63) + version (2)
// - 1024-byte fanout table: 256 entries of 4 bytes each, cumulative object counts
// - N×20-byte object IDs: SHA-1 hashes in sorted order
// - N×4-byte CRC-32 checksums: one per object
// - N×4-byte offsets: pack file positions (or large offset table indices)
// - Optional large offset table: 8-byte offsets for objects beyond 2GB
//
// WARNING: This implementation uses unsafe operations and should only be used when
// performance is critical and the code has been thoroughly tested.
func parseIdx(ix *mmap.ReaderAt) (*idxFile, error) {
	// Detect system endianness once. Git stores data in big-endian format,
	// so we need to byte-swap on little-endian systems.
	littleEndian := isLittleEndian()

	// Read and validate the 8-byte header.
	header := make([]byte, headerSize)
	if _, err := ix.ReadAt(header, 0); err != nil {
		return nil, err
	}

	// Validate magic bytes: 0xff744f63 identifies this as a Git pack index.
	if !bytes.Equal(header[0:4], []byte{0xff, 0x74, 0x4f, 0x63}) {
		return nil, fmt.Errorf("unsupported idx version or v1 not handled")
	}

	// Extract version number from bytes 4-7 (big-endian uint32).
	version := *(*uint32)(unsafe.Pointer(&header[4]))
	if littleEndian {
		version = bswap32(version) // Convert from big-endian to host byte order.
	}
	if version != 2 {
		return nil, fmt.Errorf("unsupported idx version %d", version)
	}

	// Read the fanout table: 256×4-byte entries containing cumulative object counts.
	// fanout[i] = total number of objects whose first byte is ≤ i.
	// This enables O(1) range queries for object lookup by hash prefix.
	fanoutData := make([]byte, fanoutSize)
	if _, err := ix.ReadAt(fanoutData, headerSize); err != nil {
		return nil, err
	}

	// Use unsafe casting to avoid allocating and copying 1024 bytes.
	// Cast byte slice directly to uint32 slice for efficient access.
	fanoutPtr := (*[fanoutEntries]uint32)(unsafe.Pointer(&fanoutData[0]))
	fanout := fanoutPtr[:]

	// Convert from big-endian to host byte order if needed.
	if littleEndian {
		for i := range fanout {
			fanout[i] = bswap32(fanout[i])
		}
	}

	// Total object count is the last fanout entry (cumulative count for all objects).
	objCount := fanout[255]
	if objCount == 0 {
		return &idxFile{entries: nil, oidTable: nil}, nil
	}

	// Calculate file offsets for each data section.
	// Layout after fanout table: [object IDs][CRCs][offsets][large offsets].
	oidBase := int64(headerSize + fanoutSize)     // Start of 20-byte SHA-1 hashes.
	crcBase := oidBase + int64(objCount*hashSize) // Start of 4-byte CRC-32 values.
	offBase := crcBase + int64(objCount*crcSize)  // Start of 4-byte pack offsets.

	// Performance optimization: read all fixed-size data in a single syscall.
	// This reduces I/O overhead compared to reading each section separately.
	allDataSize := objCount*hashSize + objCount*crcSize + objCount*offsetSize
	allData := make([]byte, allDataSize)

	if _, err := ix.ReadAt(allData, oidBase); err != nil {
		return nil, err
	}

	// Slice the single buffer into logical sections - no additional copying needed.
	oidData := allData[:objCount*hashSize]                                     // Object ID bytes.
	crcData := allData[objCount*hashSize : objCount*hashSize+objCount*crcSize] // CRC bytes.
	offsetData := allData[objCount*hashSize+objCount*crcSize:]                 // Offset bytes.

	// Convert raw bytes to Hash structs using unsafe operations.
	// This avoids the overhead of parsing each 20-byte hash individually.
	oids := make([]Hash, objCount)
	if len(oids) > 0 {
		// Cast Hash slice to byte slice and copy directly.
		// Each Hash is exactly 20 bytes, so this is a safe operation.
		oidBytes := (*[1 << 30]byte)(unsafe.Pointer(&oids[0]))
		copy(oidBytes[:objCount*hashSize], oidData)
	}

	// Convert CRC data from bytes to uint32s using unsafe casting.
	crcs := make([]uint32, objCount)
	if len(crcs) > 0 {
		// Create aligned buffer for uint32 values.
		alignedCrcData := make([]uint32, objCount)
		crcPtr := (*[1 << 28]uint32)(unsafe.Pointer(&alignedCrcData[0]))

		// Cast source bytes to uint32 slice for efficient access.
		srcPtr := (*[1 << 28]uint32)(unsafe.Pointer(&crcData[0]))

		// Copy with endianness conversion - CRCs are stored big-endian.
		if littleEndian {
			for i := uint32(0); i < objCount; i++ {
				crcPtr[i] = bswap32(srcPtr[i])
			}
		} else {
			copy(crcPtr[:objCount], srcPtr[:objCount])
		}
		crcs = alignedCrcData
	}

	// Process pack file offsets - these can be either direct offsets or large offset indices.
	entries := make([]idxEntry, objCount)
	offsetPtr := (*[1 << 28]uint32)(unsafe.Pointer(&offsetData[0]))

	// Track objects that reference the large offset table (for packs > 2GB).
	// Pre-allocate assuming ~0.1% of objects need large offsets (typical case).
	largeOffsetList := make([]largeOffsetEntry, 0, objCount/1000)
	maxLargeIdx := uint32(0)

	// Process all offsets in a tight loop for maximum performance.
	for i := uint32(0); i < objCount; i++ {
		offset := offsetPtr[i]
		if littleEndian {
			offset = bswap32(offset) // Convert from big-endian.
		}

		entries[i].crc = crcs[i]

		// Check if this is a direct offset or large offset table index.
		// If MSB is 0, it's a direct 31-bit offset (< 2GB).
		// If MSB is 1, the lower 31 bits index into the large offset table.
		if offset&0x80000000 == 0 {
			// Direct offset: object is within first 2GB of pack file.
			entries[i].offset = uint64(offset)
		} else {
			// Large offset: need to look up actual 64-bit offset from table.
			largeIdx := offset & 0x7fffffff // Remove MSB to get table index.
			largeOffsetList = append(largeOffsetList, largeOffsetEntry{i, largeIdx})
			if largeIdx > maxLargeIdx {
				maxLargeIdx = largeIdx // Track highest index for table size.
			}
		}
	}

	// Handle large offsets if any objects require them.
	var largeOffsets []uint64
	if len(largeOffsetList) > 0 {
		// Read the large offset table: 8-byte big-endian uint64 values.
		largeOffsetCount := maxLargeIdx + 1
		largeOffsetData := make([]byte, largeOffsetCount*largeOffSize)

		// Large offset table starts immediately after the regular offset table.
		if _, err := ix.ReadAt(largeOffsetData, offBase+int64(objCount*offsetSize)); err != nil {
			return nil, err
		}

		// Convert bytes to uint64 slice using unsafe operations.
		largeOffsets = make([]uint64, largeOffsetCount)
		largePtr := (*[1 << 26]uint64)(unsafe.Pointer(&largeOffsetData[0]))

		// Handle endianness conversion for 64-bit values.
		if littleEndian {
			for i := uint32(0); i < largeOffsetCount; i++ {
				largeOffsets[i] = bswap64(largePtr[i])
			}
		} else {
			copy(largeOffsets, largePtr[:largeOffsetCount])
		}

		// Apply the large offsets to the appropriate entries.
		for _, entry := range largeOffsetList {
			if entry.largeIdx >= uint32(len(largeOffsets)) {
				return nil, fmt.Errorf("invalid large offset index %d", entry.largeIdx)
			}
			// Replace the placeholder with the actual 64-bit offset.
			entries[entry.objIdx].offset = largeOffsets[entry.largeIdx]
		}
	}

	return &idxFile{
		entries:      entries,      // Object metadata (offset + CRC).
		oidTable:     oids,         // SHA-1 object identifiers in index order.
		largeOffsets: largeOffsets, // 64-bit offsets for objects beyond 2GB.
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

func readRawObject(r *mmap.ReaderAt, off uint64) (ObjectType, []byte, error) {
	// Read type + size (variable int).
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
	objType := ObjectType((header[0] >> 4) & 7)
	size := uint64(header[0] & 0x0f)
	shift := uint(4)
	for i := 1; i < n; i++ {
		size |= uint64(header[i]&0x7f) << shift
		shift += 7
	}
	// Object data starts after header bytes.
	src := io.NewSectionReader(r, int64(off)+int64(n), int64(size)+1024) // len > size; zlib reader stops
	zr, err := zlib.NewReader(src)
	if err != nil {
		return ObjBad, nil, err
	}
	defer zr.Close()
	var buf bytes.Buffer
	if _, err = buf.ReadFrom(zr); err != nil {
		return ObjBad, nil, err
	}
	return objType, buf.Bytes(), nil
}

// readObjectAtOffsetWithContext resolves object by pack offset with cycle detection.
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

// verifyCRC32 provides optional debug verification.
func verifyCRC32(r *mmap.ReaderAt, off uint64, data []byte, want uint32) error {
	table := crc32.MakeTable(crc32.Castagnoli)
	got := crc32.Update(0, table, data)
	if got != want {
		return fmt.Errorf("crc mismatch obj @%d", off)
	}
	return nil
}

// parseDeltaHeader splits base reference from delta buffer.
func parseDeltaHeader(t ObjectType, data []byte) (Hash, uint64, []byte, error) {
	var h Hash
	if t == ObjRefDelta {
		if len(data) < 20 {
			return h, 0, nil, fmt.Errorf("ref delta too short")
		}
		copy(h[:], data[:20])
		return h, 0, data[20:], nil
	}
	// Ofs-delta: variable-length offset encoding.
	if len(data) == 0 {
		return h, 0, nil, fmt.Errorf("ofs delta too short")
	}
	var off uint64
	i := 0
	b := data[0]
	off = uint64(b & 0x7f)
	for (b & 0x80) != 0 {
		i++
		if i >= len(data) {
			return h, 0, nil, fmt.Errorf("invalid ofs delta encoding")
		}
		b = data[i]
		off = (off + 1) << 7 // spec: add 1 then shift
		off |= uint64(b & 0x7f)
	}
	return h, off, data[i+1:], nil
}

// applyDelta implements Git's copy/insert algorithm.
func applyDelta(base, delta []byte) []byte {
	if len(delta) == 0 {
		return nil
	}

	// Read sizes.
	_, n1 := decodeVarInt(delta)
	if n1 <= 0 || n1 >= len(delta) {
		return nil
	}
	targetSize, n2 := decodeVarInt(delta[n1:])
	if n2 <= 0 || n1+n2 >= len(delta) {
		return nil
	}
	delta = delta[n1+n2:]

	out := make([]byte, targetSize)
	var opPtr, outPtr int
	for opPtr < len(delta) {
		op := delta[opPtr]
		opPtr++
		if op&0x80 != 0 { // Copy.
			var cpOff, cpLen uint32
			if op&0x01 != 0 {
				if opPtr >= len(delta) {
					return nil
				}
				cpOff = uint32(delta[opPtr])
				opPtr++
			}
			if op&0x02 != 0 {
				if opPtr >= len(delta) {
					return nil
				}
				cpOff |= uint32(delta[opPtr]) << 8
				opPtr++
			}
			if op&0x04 != 0 {
				if opPtr >= len(delta) {
					return nil
				}
				cpOff |= uint32(delta[opPtr]) << 16
				opPtr++
			}
			if op&0x08 != 0 {
				if opPtr >= len(delta) {
					return nil
				}
				cpOff |= uint32(delta[opPtr]) << 24
				opPtr++
			}
			if op&0x10 != 0 {
				if opPtr >= len(delta) {
					return nil
				}
				cpLen = uint32(delta[opPtr])
				opPtr++
			}
			if op&0x20 != 0 {
				if opPtr >= len(delta) {
					return nil
				}
				cpLen |= uint32(delta[opPtr]) << 8
				opPtr++
			}
			if op&0x40 != 0 {
				if opPtr >= len(delta) {
					return nil
				}
				cpLen |= uint32(delta[opPtr]) << 16
				opPtr++
			}
			if cpLen == 0 {
				cpLen = 1 << 16 // Spec: 0 means 65_536.
			}

			// Bounds checking for copy operation.
			if uint64(cpOff)+uint64(cpLen) > uint64(len(base)) {
				return nil // Invalid copy range.
			}
			if uint64(outPtr)+uint64(cpLen) > uint64(len(out)) {
				return nil // Output overflow.
			}

			copy(out[outPtr:outPtr+int(cpLen)], base[cpOff:int(cpOff)+int(cpLen)])
			outPtr += int(cpLen)
		} else if op != 0 { // Insert.
			// Bounds checking for insert operation.
			if opPtr+int(op) > len(delta) {
				return nil // Not enough data to insert.
			}
			if outPtr+int(op) > len(out) {
				return nil // Output overflow.
			}

			copy(out[outPtr:outPtr+int(op)], delta[opPtr:opPtr+int(op)])
			opPtr += int(op)
			outPtr += int(op)
		} else {
			return nil // Invalid delta op 0.
		}
	}
	return out
}

// decodeVarInt decodes Git variable-length ints.
func decodeVarInt(buf []byte) (uint64, int) {
	if len(buf) == 0 {
		return 0, 0
	}

	var res uint64
	var shift uint
	i := 0
	for {
		b := buf[i]
		res |= uint64(b&0x7f) << shift
		i++
		if b&0x80 == 0 {
			break
		}
		if i >= len(buf) {
			return 0, -1 // Invalid encoding.
		}
		shift += 7
	}
	return res, i
}
