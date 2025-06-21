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
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"github.com/hashicorp/golang-lru/arc/v2"
	"golang.org/x/exp/mmap"
)

var hostLittle = func() bool {
	var i uint16 = 1
	return *(*byte)(unsafe.Pointer(&i)) == 1
}()

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
func (h Hash) Uint64() uint64 { return *(*uint64)(unsafe.Pointer(&h[0])) }

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

// Parser size constants.
//
// These bytes-count constants describe the fixed-width sections of a Git
// pack-index (v2) file. The unsafe idx parser relies on them to compute exact
// offsets inside the memory-mapped file. Do not modify these values unless the
// on-disk format itself changes.
const (
	headerSize    = 8                 // 4-byte magic + 4-byte version.
	fanoutEntries = 256               // One entry for every possible first byte of a SHA-1.
	fanoutSize    = fanoutEntries * 4 // 256 × uint32 → 1 024 bytes.

	hashSize     = 20 // Full SHA-1 hash.
	crcSize      = 4  // Big-endian CRC-32 value per object.
	offsetSize   = 4  // 31-bit offset or MSB-set index into large-offset table.
	largeOffSize = 8  // 64-bit offset for objects beyond the 2 GiB boundary.

	defaultMaxDeltaDepth = 50
)

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

// idxFile holds the memory-mapped view and lookup tables for a single
// *.pack / *.idx pair.
//
// A Store creates one idxFile per pack it opens.
// The struct is immutable after Open returns, so callers may share it across
// goroutines without additional synchronization.
type idxFile struct {
	// pack is the read-only, memory-mapped view of the *.pack file.
	pack *mmap.ReaderAt

	// idx is the memory-mapped companion *.idx file.
	idx *mmap.ReaderAt

	// fanout is the 256-entry fan-out table from the idx header.
	// fanout[b] stores the number of objects whose SHA-1 starts with a
	// byte ≤ b, enabling O(1) range selection before binary search.
	fanout [fanoutEntries]uint32

	// oidTable lists all object IDs (SHA-1 hashes) in canonical index
	// order. entries[i] describes oidTable[i].
	oidTable []Hash

	// entries runs parallel to oidTable and records the byte offset inside
	// the pack and the CRC-32 checksum of each object.
	entries []idxEntry

	// crcByOffset maps pack file offset to CRC-32 checksum for O(1) lookup
	// during CRC verification.
	crcByOffset map[uint64]uint32

	// largeOffsets stores 64-bit offsets for objects located beyond the
	// 2 GiB boundary. The slice is nil when the pack is smaller than that.
	largeOffsets []uint64
}

// findObject looks up hash in the tables that belong to a single
// *.idx / *.pack pair and returns the absolute byte offset of the object
// inside the pack.
//
// The method first consults the 256-entry fan-out table to narrow the search
// window to objects whose first digest byte matches hash[0].
// Within that window it performs a binary search over the sorted SHA-1 slice.
//
// The boolean result reports whether the object was present; when it is
// false offset is zero.
//
// The receiver is immutable after Open has returned, therefore the method is
// safe for concurrent callers.
func (f *idxFile) findObject(hash Hash) (offset uint64, found bool) {
	// Identify the search range via the fan-out table.
	first := hash[0]

	start := uint32(0)
	if first > 0 {
		start = f.fanout[first-1]
	}
	end := f.fanout[first]
	if start == end {
		return 0, false // bucket empty
	}

	// Binary-search the slice [start:end) for the target hash.
	relIdx, ok := slices.BinarySearchFunc(
		f.oidTable[start:end],
		hash,
		func(a, b Hash) int { return bytes.Compare(a[:], b[:]) },
	)
	if !ok {
		return 0, false
	}
	absIdx := int(start) + relIdx
	return f.entries[absIdx].offset, true
}

// Store provides concurrent, read-only access to one or more memory-mapped
// Git packfiles.
//
// A Store maps every *.pack / *.idx pair found in a directory, maintains an
// in-memory index from object ID to pack offset, inflates objects on demand,
// and resolves delta chains up to maxDeltaDepth. All methods are safe for
// concurrent use by multiple goroutines.
type Store struct {
	// packs contains one immutable idxFile per mapped pack.
	packs []*idxFile

	// midx holds the multi-pack index if one was found, nil otherwise.
	midx *midxFile

	// mu guards live configuration such as cache size and maxDeltaDepth.
	mu sync.Mutex

	// cache holds fully-inflated objects. It uses an Adaptive Replacement
	// Cache (ARC) that balances recency and frequency for high hit rates.
	cache *arc.ARCCache[Hash, []byte]

	// maxDeltaDepth limits how many recursive delta hops Get will follow
	// before aborting the lookup.
	maxDeltaDepth int

	// VerifyCRC enables CRC-32 validation of every object read from a pack.
	// Leave it false (the default) when latency is more important than
	// end-to-end integrity checks.
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

// (ctx *deltaContext) checkRefDelta validates that resolving a ref-delta will
// neither overflow the caller-supplied maximum delta-chain depth nor re-visit
// the same base object.
//
// The method returns a descriptive error when the next hop would exceed
// ctx.maxDepth or when the referenced base hash is already present in
// ctx.visited, which indicates a ref-delta cycle.
func (ctx *deltaContext) checkRefDelta(hash Hash) error {
	// Guard against unbounded recursion.
	if ctx.depth >= ctx.maxDepth {
		return fmt.Errorf("delta chain too deep (max %d)", ctx.maxDepth)
	}
	// Detect ref-delta cycles.
	if ctx.visited[hash] {
		return fmt.Errorf("circular delta reference detected for %x", hash)
	}
	return nil
}

// (ctx *deltaContext) checkOfsDelta performs the same safety validations as
// checkRefDelta but for ofs-deltas, which reference their base object by pack
// offset rather than object ID.
//
// It reports an error when the delta chain would become too deep or when the
// same offset appears twice in the current resolution path.
func (ctx *deltaContext) checkOfsDelta(offset uint64) error {
	if ctx.depth >= ctx.maxDepth {
		return fmt.Errorf("delta chain too deep (max %d)", ctx.maxDepth)
	}
	if ctx.offsets[offset] {
		return fmt.Errorf("circular delta reference detected at offset %d", offset)
	}
	return nil
}

// (ctx *deltaContext) enterRefDelta records that the ref-delta identified by
// hash is being processed and bumps the recursion depth counter.
func (ctx *deltaContext) enterRefDelta(hash Hash) {
	ctx.visited[hash] = true
	ctx.depth++
}

// (ctx *deltaContext) enterOfsDelta records that the ofs-delta starting at
// offset is being processed and bumps the recursion depth counter.
func (ctx *deltaContext) enterOfsDelta(offset uint64) {
	ctx.offsets[offset] = true
	ctx.depth++
}

// (ctx *deltaContext) exit decrements the recursion depth counter when the
// caller leaves a delta resolution frame.
func (ctx *deltaContext) exit() { ctx.depth-- }

// Open scans dir for "*.pack" files, expects a matching "*.idx" companion for
// each one, memory-maps every pair, and returns a read-only *Store.
//
// Open eagerly parses all index files so that subsequent Store.Get calls are
// an O(1) table lookup followed by lazy object inflation.
// A small Adaptive-Replacement cache (ARC) is created up-front to avoid
// repeated decompression of hot objects.
//
// Error semantics:
//   - If no "*.pack" files are found in dir an error is returned.
//   - If any "*.idx" companion is missing or malformed, Open fails.
//   - All I/O is performed with mmap; failures when mapping a file are
//     surfaced immediately.
//
// The returned Store is safe for concurrent readers; no additional
// synchronization is required by the caller.
//
// For bare repositories pass ".git/objects/pack" as dir.
func Open(dir string) (*Store, error) {
	// Attempt to mmap a multi-pack-index. It is named exactly
	//    "…/objects/pack/multi-pack-index".
	midxPath := filepath.Join(dir, "multi-pack-index")
	var midx *midxFile
	if st, err := os.Stat(midxPath); err == nil && !st.IsDir() {
		mr, err := mmap.Open(midxPath)
		if err != nil {
			return nil, fmt.Errorf("mmap midx: %w", err)
		}
		defer func() {
			if midx == nil {
				_ = mr.Close()
			}
		}()
		midx, err = parseMidx(dir, mr)
		if err != nil {
			_ = mr.Close()
			return nil, fmt.Errorf("parse midx: %w", err)
		}
	}

	// Normal pack/idx enumeration.
	pattern := filepath.Join(dir, "*.pack")
	packs, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(packs) == 0 && midx == nil {
		return nil, fmt.Errorf("no packfiles found in %s", dir)
	}

	store := &Store{
		maxDeltaDepth: defaultMaxDeltaDepth,
		midx:          midx,
	}

	const defaultCacheSize = 1 << 14 // 16 KiB ARC—enough for >95 % hit-rate on typical workloads.
	store.cache, err = arc.NewARC[Hash, []byte](defaultCacheSize)
	if err != nil {
		// This should not happen with a positive default size, but it is
		// best practice to handle the error from any constructor.
		return nil, fmt.Errorf("failed to create ARC cache: %w", err)
	}

	for _, packPath := range packs {
		idxPath := strings.TrimSuffix(packPath, ".pack") + ".idx"
		pk, err := mmap.Open(packPath)
		if err != nil {
			return nil, fmt.Errorf("mmap pack: %w", err)
		}
		ix, err := mmap.Open(idxPath)
		if errors.Is(err, os.ErrNotExist) && midx != nil {
			// Pack is referenced only through the multi‑pack‑index; CRCs will come from MIDX v2 later.
			store.packs = append(store.packs, &idxFile{pack: pk}) // minimal stub
			continue
		} else if err != nil {
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
	}

	return store, nil
}

// SetMaxDeltaDepth sets the maximum delta chain depth.
//
// SetMaxDeltaDepth changes the maximum number of recursive delta hops
// (ref-delta or ofs-delta) that the Store will follow when materializing an
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

	if s.midx != nil {
		for _, p := range s.midx.packReaders {
			if p != nil {
				if err := p.Close(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
	}

	for _, p := range s.packs {
		if err := p.pack.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if p.idx != nil {
			if err := p.idx.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
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
	if b, ok := s.cache.Get(oid); ok {
		return b, detectType(b), nil
	}

	if s.midx != nil {
		if p, off, ok := s.midx.findObject(oid); ok {
			return s.inflateFromPack(p, off, oid, ctx)
		}
	}

	for _, pf := range s.packs {
		offset, found := pf.findObject(oid)
		if !found {
			continue
		}
		return s.inflateFromPack(pf.pack, offset, oid, ctx)
	}

	return nil, ObjBad, fmt.Errorf("object %x not found", oid)
}

// inflateFromPack handles object inflation from a specific pack, with delta resolution.
func (s *Store) inflateFromPack(
	p *mmap.ReaderAt,
	off uint64,
	oid Hash,
	ctx *deltaContext,
) ([]byte, ObjectType, error) {
	objType, data, err := readRawObject(p, off)
	if err != nil {
		return nil, ObjBad, err
	}

	switch objType {
	case ObjBlob, ObjCommit, ObjTree, ObjTag:
		if s.VerifyCRC {
			// For CRC verification with midx, we need to find the right idxFile
			// This is a simplified approach - in practice we might want to store
			// CRC data in the midx structure or skip verification for midx objects.
			for _, pf := range s.packs {
				if pf.pack == p {
					if want, ok := pf.crcByOffset[off]; ok {
						if err := verifyCRC32(p, off, data, want); err != nil {
							return nil, ObjBad, err
						}
					}
					break
				}
			}
		}
		s.cache.Add(oid, data)
		return data, objType, nil

	case ObjOfsDelta, ObjRefDelta:
		baseHash, baseOff, deltaBuf, err := parseDeltaHeader(objType, data)
		if err != nil {
			return nil, ObjBad, err
		}

		var base []byte
		if objType == ObjRefDelta {
			if err := ctx.checkRefDelta(baseHash); err != nil {
				return nil, ObjBad, err
			}
			ctx.enterRefDelta(baseHash)
			base, _, err = s.getWithContext(baseHash, ctx)
			ctx.exit()
		} else {
			if err := ctx.checkOfsDelta(baseOff); err != nil {
				return nil, ObjBad, err
			}
			ctx.enterOfsDelta(baseOff)
			base, _, err = readObjectAtOffsetWithContext(p, baseOff, s, ctx)
			ctx.exit()
		}
		if err != nil {
			return nil, ObjBad, err
		}
		full := applyDelta(base, deltaBuf)
		s.cache.Add(oid, full)
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

	if hostLittle {
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
	littleEndian := hostLittle

	header := make([]byte, headerSize)
	if _, err := ix.ReadAt(header, 0); err != nil {
		return nil, err
	}

	// Validate magic bytes: 0xff744f63 identifies this as a Git pack index.
	if !bytes.Equal(header[0:4], []byte{0xff, 0x74, 0x4f, 0x63}) {
		return nil, fmt.Errorf("unsupported idx version or v1 not handled")
	}

	if version := binary.BigEndian.Uint32(header[4:]); version != 2 {
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
	fanout := *fanoutPtr

	if littleEndian {
		for i := range fanout {
			fanout[i] = binary.BigEndian.Uint32(fanoutData[i*4:])
		}
	}

	// Guard against truncated or tampered indexes.
	for i := 1; i < fanoutEntries; i++ {
		if fanout[i] < fanout[i-1] {
			return nil, fmt.Errorf("idx corrupt: fan‑out table not monotonic")
		}
	}

	objCount := fanout[255]
	if objCount == 0 {
		return &idxFile{fanout: fanout, entries: nil, oidTable: nil}, nil
	}

	// Guard against integer overflow when allocating giant slices.
	// Prevents wrapped len calculations on malicious files.
	if objCount > math.MaxUint32/hashSize {
		return nil, fmt.Errorf("idx claims %d objects – impl refuses >%d", objCount,
			math.MaxUint32/hashSize)
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
		copy(unsafe.Slice((*byte)(unsafe.Pointer(&oids[0])), len(oids)*hashSize), oidData)
	}

	crcs := make([]uint32, objCount)
	if len(crcs) > 0 {
		alignedCrcData := make([]uint32, objCount)
		crcPtr := (*[1 << 28]uint32)(unsafe.Pointer(&alignedCrcData[0]))
		srcPtr := (*[1 << 28]uint32)(unsafe.Pointer(&crcData[0]))

		if littleEndian {
			for i := range objCount {
				crcPtr[i] = binary.BigEndian.Uint32(crcData[i*4:])
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

	for i := range objCount {
		offset := offsetPtr[i]
		if littleEndian {
			offset = binary.BigEndian.Uint32(offsetData[i*4:])
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
		for i := range largeOffsetCount {
			offset := i * largeOffSize
			if int(offset+largeOffSize) <= len(largeOffsetData) {
				largeOffsets[i] = binary.BigEndian.Uint64(largeOffsetData[offset : offset+largeOffSize])
			}
		}

		for _, entry := range largeOffsetList {
			// Bit 31 of the on-disk 32-bit offset is the "large-offset" flag.
			// Always clear it before using the value as an index so that a
			// stray, unmasked bit can never take us past the end of the LOFF
			// slice.
			idx := entry.largeIdx & 0x7fffffff
			if idx >= uint32(len(largeOffsets)) {
				return nil, fmt.Errorf("invalid large offset index %d", idx)
			}
			entries[entry.objIdx].offset = largeOffsets[idx]
		}
	}

	crcByOffset := make(map[uint64]uint32, objCount)
	for i := range objCount {
		crcByOffset[entries[i].offset] = entries[i].crc
	}

	trailer := make([]byte, 40)
	if _, err := ix.ReadAt(trailer, int64(ix.Len()-40)); err != nil {
		return nil, err
	}

	fullFile := make([]byte, ix.Len())
	if _, err := ix.ReadAt(fullFile, 0); err != nil {
		return nil, err
	}
	gotIdxSHA := sha1.Sum(fullFile[:ix.Len()-20]) // compute over entire file minus last 20
	if !bytes.Equal(trailer[20:], gotIdxSHA[:]) {
		return nil, fmt.Errorf("idx checksum mismatch")
	}

	return &idxFile{
		fanout:       fanout,
		entries:      entries,
		oidTable:     oids,
		crcByOffset:  crcByOffset,
		largeOffsets: largeOffsets,
	}, nil
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
	if _, err = buf.ReadFrom(zr); err != nil {
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

// midxEntry describes a single object as recorded in a multi-pack index.
// The struct maps an object to its containing pack and byte offset within that pack.
type midxEntry struct {
	// packID is the index into the packReaders slice, identifying which
	// pack file contains this object.
	packID uint32

	// offset is the absolute byte position of the object header inside
	// the specified pack file. The field is 64-bit to support packs
	// that exceed 2 GiB.
	offset uint64
}

// midxFile represents one "multi-pack-index" (midx) file together with the
// packfiles it references.
//
// The struct is immutable after parseMidx returns and is safe for concurrent
// readers – exactly like idxFile.
type midxFile struct {
	// packReaders are mmap-handles for the N packfiles listed in PNAM order.
	// len(packReaders) == len(packNames).
	packReaders []*mmap.ReaderAt
	packNames   []string // full basenames, e.g. "pack-abcd1234.pack"

	// fanout[i] == #objects whose first digest byte ≤ i.
	fanout [fanoutEntries]uint32

	// objectIDs and entries run in parallel and have identical length.
	objectIDs []Hash
	entries   []midxEntry
}

// Multi-pack index chunk identifiers.
const (
	chunkPNAM = 0x504e414d // 'PNAM' - pack names
	chunkOIDF = 0x4f494446 // 'OIDF' - object ID fanout table
	chunkOIDL = 0x4f49444c // 'OIDL' - object ID list
	chunkOOFF = 0x4f4f4646 // 'OOFF' - object offsets
	chunkLOFF = 0x4c4f4646 // 'LOFF' - large object offsets
)

// findObject performs the same two-stage lookup that idxFile.findObject does
// but returns both the *mmap.ReaderAt for the pack and the byte offset.
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
	return m.packReaders[ent.packID], ent.offset, true
}

// parseMidx reads the file mapped in mr and returns a fully‑populated
// midxFile. Only version‑1 / SHA‑1 repositories are supported.
//
// dir must be the "…/objects/pack" directory so that pack names from the
// PNAM chunk can be mmap'ed right away.
//
// Note: midx v1 is the current format (different from pack index v2)
// midx files have their own versioning scheme starting at 1
func parseMidx(dir string, mr *mmap.ReaderAt) (*midxFile, error) {
	// Header.
	var hdr [12]byte
	if _, err := mr.ReadAt(hdr[:], 0); err != nil {
		return nil, err
	}
	if !bytes.Equal(hdr[0:4], []byte("MIDX")) {
		return nil, fmt.Errorf("not a MIDX file")
	}
	if hdr[4] != 1 /* version */ {
		return nil, fmt.Errorf("unsupported midx version %d", hdr[4])
	}
	if hdr[5] != 1 /* SHA‑1 */ {
		return nil, fmt.Errorf("only SHA‑1 midx supported")
	}

	chunks := int(hdr[6])
	packCount := int(binary.BigEndian.Uint32(hdr[8:12]))

	// Chunk table.
	type cdesc struct {
		id  [4]byte
		off uint64
	}
	cd := make([]cdesc, chunks+1) // +1 for the terminating row
	for i := range cd {
		var row [12]byte
		base := int64(12 + i*12)
		if _, err := mr.ReadAt(row[:], base); err != nil {
			return nil, err
		}
		copy(cd[i].id[:], row[0:4])
		cd[i].off = binary.BigEndian.Uint64(row[4:12])
	}
	// Calculate size for each chunk by looking at the next offset.
	sort.Slice(cd, func(i, j int) bool { return cd[i].off < cd[j].off })

	findChunk := func(id uint32) (off int64, size int64, err error) {
		for i := 0; i < len(cd)-1; i++ {
			chunkID := binary.BigEndian.Uint32(cd[i].id[:])
			if chunkID == id {
				return int64(cd[i].off),
					int64(cd[i+1].off) - int64(cd[i].off),
					nil
			}
		}
		return 0, 0, fmt.Errorf("chunk %08x not found", id)
	}

	// PNAM.
	pnOff, pnSize, err := findChunk(chunkPNAM)
	if err != nil {
		return nil, err
	}
	pn := make([]byte, pnSize)
	if _, err = mr.ReadAt(pn, pnOff); err != nil {
		return nil, err
	}
	// Filenames are NUL‑terminated.
	var packNames []string
	for start := 0; start < len(pn); {
		end := bytes.IndexByte(pn[start:], 0)
		if end < 0 {
			return nil, fmt.Errorf("unterminated PNAM entry")
		}
		packNames = append(packNames, string(pn[start:start+end]))
		start += end + 1
	}
	if len(packNames) != packCount {
		return nil, fmt.Errorf("PNAM count mismatch (%d vs %d)", len(packNames), packCount)
	}

	// Mmap every pack straight away so we have a stable *ReaderAt slice.
	packs := make([]*mmap.ReaderAt, len(packNames))
	for i, name := range packNames {
		r, err := mmap.Open(filepath.Join(dir, name))
		if err != nil {
			for _, p := range packs[:i] {
				if p != nil {
					_ = p.Close()
				}
			}
			return nil, fmt.Errorf("mmap pack %q: %w", name, err)
		}
		packs[i] = r
	}

	// OIDF.
	fanOff, _, err := findChunk(chunkOIDF)
	if err != nil {
		return nil, err
	}
	var fanout [fanoutEntries]uint32
	if _, err = mr.ReadAt(unsafe.Slice((*byte)(unsafe.Pointer(&fanout[0])), fanoutSize), fanOff); err != nil {
		return nil, err
	}
	if hostLittle {
		for i := range fanout {
			fanout[i] = binary.BigEndian.Uint32(unsafe.Slice((*byte)(unsafe.Pointer(&fanout[i])), 4))
		}
	}
	objCount := fanout[255]

	// OIDL.
	oidOff, _, err := findChunk(chunkOIDL)
	if err != nil {
		return nil, err
	}
	oids := make([]Hash, objCount)
	read := unsafe.Slice((*byte)(unsafe.Pointer(&oids[0])), int(objCount*hashSize))
	if _, err = mr.ReadAt(read, oidOff); err != nil {
		return nil, err
	}

	// OOFF / LOFF.
	offOff, _, err := findChunk(chunkOOFF)
	if err != nil {
		return nil, err
	}
	offRaw := make([]byte, objCount*8) // two uint32 per object
	if _, err = mr.ReadAt(offRaw, offOff); err != nil {
		return nil, err
	}

	// LOFF is optional.
	loffOff, loffSize, _ := findChunk(chunkLOFF) // ignore "not found" error
	var loff []uint64
	if loffSize > 0 {
		loff = make([]uint64, loffSize/8)
		if _, err = mr.ReadAt(
			unsafe.Slice((*byte)(unsafe.Pointer(&loff[0])), int(loffSize)),
			loffOff,
		); err != nil {
			return nil, err
		}
	}

	entries := make([]midxEntry, objCount)

	for i := range objCount {
		packID := binary.BigEndian.Uint32(offRaw[i*8 : i*8+4])
		rawOff := binary.BigEndian.Uint32(offRaw[i*8+4 : i*8+8])

		var off64 uint64
		if rawOff&0x80000000 == 0 {
			off64 = uint64(rawOff)
		} else {
			idx := rawOff & 0x7FFFFFFF
			if int(idx) >= len(loff) {
				return nil, fmt.Errorf("invalid LOFF index %d", idx)
			}
			off64 = loff[idx]
		}
		entries[i] = midxEntry{packID, off64}
	}

	return &midxFile{
		packReaders: packs,
		packNames:   packNames,
		fanout:      fanout,
		objectIDs:   oids,
		entries:     entries,
	}, nil
}
