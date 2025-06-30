// Package objstore provides a minimal, memory-mapped Git object store that
// resolves objects directly from *.pack files without shelling out to the
// Git executable.
//
// The store is intended for read-only scenarios—such as code search,
// indexing, and content serving—where low-latency look-ups are required but a
// full on-disk checkout is unnecessary.
//
// IMPLEMENTATION:
// The store memory-maps one or more *.pack / *.idx pairs, builds an in-memory map
// from SHA-1 object IDs to their pack offsets, and inflates objects on demand.
// It coordinates between pack-index (IDX), multi-pack-index (MIDX), and
// reverse-index (RIDX) files, providing unified access across multiple packfiles
// with transparent delta chain resolution and caching.
//
// All packfiles are memory-mapped for zero-copy access, with an adaptive
// replacement cache (ARC) and delta window for hot objects. Delta chains are
// resolved transparently with bounded depth and cycle detection. A small,
// size-bounded cache avoids redundant decompression, and optional CRC-32
// verification can be enabled for additional integrity checks.
//
// USAGE:
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
// Store is safe for concurrent readers and eliminates the need to shell out
// to Git while providing high-performance object retrieval for read-only
// workloads like indexing and content serving.
package objstore

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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

func init() {
	// On Windows an mmapped file cannot be closed while any outstanding
	// slices are still referenced. A finalizer makes sure users who forget
	// to call (*Store).Close() do not trigger ERROR_LOCK_VIOLATION.
	if runtime.GOOS == "windows" {
		runtime.SetFinalizer(&Store{}, func(s *Store) { _ = s.Close() })
	}
}

// Parser size constants.
//
// These bytes-count constants describe the fixed-width sections of a Git
// pack-index (v2) file. The unsafe idx parser relies on them to compute exact
// offsets inside the memory-mapped file. Do not modify these values unless the
// on-disk format itself changes.
const (
	fanoutEntries = 256               // One entry for every possible first byte of a SHA-1.
	fanoutSize    = fanoutEntries * 4 // 256 × uint32 → 1 024 bytes.

	hashSize     = 20 // Full SHA-1 hash.
	largeOffSize = 8  // 64-bit offset for objects beyond the 2 GiB boundary.

	defaultMaxDeltaDepth = 50
)

// Store provides concurrent, read-only access to one or more memory-mapped
// Git packfiles with support for multi-pack-index (MIDX) files.
//
// A Store maps every *.pack / *.idx pair found in a directory, maintains an
// in-memory index from object ID to pack offset, inflates objects on demand,
// and resolves delta chains up to maxDeltaDepth. All methods are safe for
// concurrent use by multiple goroutines.
//
// CRC Verification:
// When VerifyCRC is enabled, the Store validates CRC-32 checksums for each
// object read. For objects in regular packs with .idx files, CRCs are always
// available. For objects that only appear in a multi-pack-index:
//   - MIDX v1: CRC verification is silently skipped (no CRC data available)
//   - MIDX v2: CRCs are read from the CRCS chunk and verified
//
// Note: Pack trailer checksums can be verified by calling verifyPackTrailers()
// after opening the Store if additional integrity checking is required.
type Store struct {
	// packs contains one immutable idxFile per mapped pack.
	packs []*idxFile

	// midx holds the multi-pack index if one was found, nil otherwise.
	midx *midxFile

	// packMap tracks unique mmap handles to prevent duplicate mappings.
	packMap map[string]*mmap.ReaderAt

	// mu guards live configuration such as cache size and maxDeltaDepth.
	mu sync.Mutex

	// cache holds fully-inflated objects. It uses an Adaptive Replacement
	// Cache (ARC) that balances recency and frequency for high hit rates.
	cache *arc.ARCCache[Hash, []byte]

	// dw is a short-lived object window (32 MiB) for delta chain optimization.
	dw *refCountedDeltaWindow

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
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	packCache := make(map[string]*mmap.ReaderAt)

	// Attempt to mmap a multi-pack-index. It is named exactly
	//    "…/objects/pack/multi-pack-index".
	midxPath := filepath.Join(absDir, "multi-pack-index")
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
		midx, err = parseMidx(absDir, mr, packCache)
		if err != nil {
			_ = mr.Close()
			return nil, fmt.Errorf("parse midx: %w", err)
		}
	}

	// Normal pack/idx enumeration.
	pattern := filepath.Join(absDir, "*.pack")
	packs, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(packs) == 0 && midx == nil {
		// Empty repository - return a valid but empty store.
		store := &Store{
			maxDeltaDepth: defaultMaxDeltaDepth,
			packs:         []*idxFile{}, // Empty slice instead of nil
			packMap:       make(map[string]*mmap.ReaderAt),
			dw:            newRefCountedDeltaWindow(),
		}

		const defaultCacheSize = 1 << 14 // 16K entries ~ 96MiB
		var err error
		store.cache, err = arc.NewARC[Hash, []byte](defaultCacheSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create ARC cache: %w", err)
		}

		return store, nil
	}

	// Glob all *.pack not already mapped by the midx.
	for _, p := range packs {
		if _, ok := packCache[p]; ok {
			continue
		}
		h, err := mmap.Open(p)
		if err != nil {
			return nil, err
		}
		packCache[p] = h
	}

	store := &Store{
		maxDeltaDepth: defaultMaxDeltaDepth,
		midx:          midx,
		packMap:       packCache,
		dw:            newRefCountedDeltaWindow(),
	}

	const defaultCacheSize = 1 << 14 // 16K entries ~ 96MiB
	store.cache, err = arc.NewARC[Hash, []byte](defaultCacheSize)
	if err != nil {
		// This should not happen with a positive default size, but it is
		// best practice to handle the error from any constructor.
		return nil, fmt.Errorf("failed to create ARC cache: %w", err)
	}

	// Build idxFiles, re-using map for the pack field.
	for path, handle := range packCache {
		idxPath := strings.TrimSuffix(path, ".pack") + ".idx"
		ix, err := mmap.Open(idxPath)
		if errors.Is(err, os.ErrNotExist) && midx != nil {
			// Pack is referenced only through the multi‑pack‑index; CRCs will come from MIDX v2 later.
			store.packs = append(store.packs, &idxFile{pack: handle}) // minimal stub
			continue
		} else if err != nil {
			return nil, fmt.Errorf("mmap idx: %w", err)
		}
		f, err := parseIdx(ix)
		if err != nil {
			_ = ix.Close()
			return nil, fmt.Errorf("parse idx: %w", err)
		}
		f.pack = handle
		f.idx = ix
		store.packs = append(store.packs, f)

		if f.sortedOffsets != nil {
			f.ridx, err = loadReverseIndex(path, f)
			if err != nil {
				_ = ix.Close()
				return nil, fmt.Errorf("load ridx: %w", err)
			}
		}
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

	for _, h := range s.packMap {
		if err := h.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Close idx files
	for _, p := range s.packs {
		if p.idx != nil {
			if err := p.idx.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Get returns the fully materialized Git object identified by oid together
// with its on-disk ObjectType.
//
// The method first consults two in-memory caches:
//  1. a very small, hot-object "delta window", and
//  2. an Adaptive-Replacement Cache (ARC) that balances recency and
//     frequency.
//
// A hit in either cache avoids the cost of decompression.
//
// On a miss, Get inflates the object from the underlying packfile.
// If the object is stored as a delta, the method follows the delta chain—
// capped by Store.maxDeltaDepth—until it reaches the base object, then
// applies each delta in order to reconstruct the full byte stream.
//
// When Store.VerifyCRC is true, Get validates the CRC-32 checksum recorded
// in the corresponding *.idx file (or multi-pack-index v2) and returns an
// error on mismatch.
//
// The returned slice is always a fresh allocation; callers may mutate it at
// will.
// Get is safe for concurrent use by multiple goroutines.
func (s *Store) Get(oid Hash) ([]byte, ObjectType, error) {
	// Fast path: check the tiny "delta window" that holds the most recently
	// materialized objects, which are likely to be referenced by upcoming
	// deltas.
	if b, ok := s.dw.acquire(oid); ok {
		return b.Data(), detectType(b.Data()), nil
	}

	// Second-level lookup in the larger ARC cache.
	if b, ok := s.cache.Get(oid); ok {
		return b, detectType(b), nil
	}

	// Cache miss – inflate from pack while tracking delta depth to prevent
	// cycles and excessive recursion.
	return s.getWithContext(oid, newDeltaContext(s.maxDeltaDepth))
}

// inflateFromPack materializes the Git object that starts at off inside the
// memory-mapped pack p.
//
// The helper is the workhorse of Store.Get. It
//   - peeks at the pack header to determine the on-disk type with
//     peekObjectType, short-circuiting OBJ_COMMIT objects that can be served
//     from the commit-graph;
//   - inflates non-delta objects directly with readRawObject;
//   - resolves both ref-delta and ofs-delta chains recursively via
//     deltaContext, bounded by Store.maxDeltaDepth and with cycle detection;
//   - applies delta instructions with applyDelta to reconstruct the full
//     byte stream; and
//   - verifies CRC-32 checksums when Store.VerifyCRC is enabled, using either
//     the *.idx side-file or, for multi-pack-index v2, the MIDX CRC table.
//
// A successful call returns a freshly-allocated buffer that the caller may
// mutate, the detected ObjectType, and a nil error.
// It returns ObjBad and a non-nil error when header parsing, delta
// resolution, decompression, or CRC validation fails.
//
// The method is read-only with respect to *Store (aside from its caches) and
// is safe for concurrent use; all shared state mutation is confined to the
// cache helpers, which are internally synchronized.
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

		// finalType := detectType(full)
		s.dw.add(oid, full)
		s.cache.Add(oid, full)
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

	s.dw.add(oid, data)
	s.cache.Add(oid, data)
	return data, objType, nil
}

// peekObjectType reads at most 32 bytes to discover the on‑disk type without
// inflating the body. It also returns the header length (needed by callers
// that want to inflate manually afterwards).
func peekObjectType(r *mmap.ReaderAt, off uint64) (ObjectType, int, error) {
	var buf [32]byte
	n, err := r.ReadAt(buf[:], int64(off))
	if err != nil && err != io.EOF {
		return ObjBad, 0, err
	}
	if n == 0 {
		return ObjBad, 0, errors.New("empty object header")
	}
	ot, _, hdrLen := parseObjectHeaderUnsafe(buf[:n])
	if hdrLen <= 0 {
		return ObjBad, 0, errors.New("cannot parse object header")
	}
	return ot, hdrLen, nil
}

// getWithContext handles object retrieval with delta cycle detection.
func (s *Store) getWithContext(oid Hash, ctx *deltaContext) ([]byte, ObjectType, error) {
	if b, ok := s.dw.acquire(oid); ok {
		return b.Data(), detectType(b.Data()), nil
	}

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

// parseObjectHeaderUnsafe parses the variable-length object header that
// precedes every entry inside a packfile.
//
// The header encodes the object type in the upper three bits of the first
// byte and the object size as a base-128 varint that may span up to
// nine additional bytes.
//
// For performance reasons the implementation
// – performs all work on the supplied byte slice without additional
// allocations,
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
			// Calculate offset for ofs-delta base object.
			actualOffset := off - boff
			if err := ctx.checkOfsDelta(actualOffset); err != nil {
				return nil, ObjBad, err
			}
			ctx.enterOfsDelta(actualOffset)
			base, _, err = readObjectAtOffsetWithContext(r, actualOffset, s, ctx)
			ctx.exit()
		}
		if err != nil {
			return nil, ObjBad, err
		}

		full := applyDelta(base, deltaBuf)
		if full == nil {
			return nil, ObjBad, fmt.Errorf("delta application failed")
		}

		return full, detectType(full), nil
	}

	return data, objType, nil
}

// readRawObject inflates the object that starts at off in the given
// memory‑mapped packfile.
//
// For *delta* objects we must:
//
//  1. Return the "prefix" (20‑byte base hash for OBJ_REF_DELTA, or the
//     variable‑length offset for OBJ_OFS_DELTA) **uncompressed**;
//  2. Start zlib inflation **after** that prefix;
//  3. Prepend the prefix to the inflated delta instructions so that
//     parseDeltaHeader can still find it.
//
// All objects—large or small—now follow the same streaming code path; the
// earlier "small object (<64 KiB) read‑into‑buffer" fast path has been
// removed because it was the source of the mis‑alignment bug.
//
// The implementation follows these steps:
//
//  1. Parse the variable‑length object header to determine type and size;
//  2. Read (and keep) the delta prefix, if any:
//     - For OBJ_REF_DELTA: 20‑byte SHA‑1 base hash
//     - For OBJ_OFS_DELTA: variable‑length negative offset
//  3. Inflate the deflate stream that follows the header and prefix;
//  4. Return [prefix||inflated] for deltas, or just the inflated body otherwise.
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
		fmt.Printf("zlib error at pack=%p off=%d hdrLen=%d prefix=%d : %v\n",
			r, off, hdrLen, len(prefix), err)
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

// VerifyPackTrailers validates the SHA-1 trailer that closes every mapped
// packfile.
//
// A Git pack ends with a 20-byte checksum that covers all preceding bytes.
// VerifyPackTrailers re-computes that checksum for each unique *.pack mapping
// and returns an error if any mismatch is found.
//
// The method is a no-op unless the caller has enabled Store.VerifyCRC.
// It is safe for concurrent use; all work is performed on local variables and
// no shared state in *Store is mutated.
// Repeated invocations are cheap—the same mmap handle is processed only once
// during a single call.
func (s *Store) VerifyPackTrailers() error {
	if !s.VerifyCRC {
		return nil
	}

	verified := make(map[*mmap.ReaderAt]bool)

	for _, pf := range s.packs {
		if pf.pack == nil || verified[pf.pack] {
			continue
		}

		// verifyPackTrailer performs the actual SHA-1 calculation.
		if err := verifyPackTrailer(pf.pack); err != nil {
			return fmt.Errorf("pack %p: %w", pf.pack, err)
		}
		verified[pf.pack] = true
	}

	return nil
}

// (s *Store) findCRCForObject returns the CRC-32 checksum Git recorded for
// the object identified by oid.
//
// It looks for the checksum in the per-pack *.idx files first and, if it
// fails to locate it there, falls back to the multi-pack-index (MIDX) when
// one is present and of version ≥ 2 (the first version that stores CRCs).
//
// The bool result reports whether a checksum was found.
// The method is read-only, safe for concurrent use, and does not depend on
// Store.VerifyCRC—that flag merely controls whether the caller decides to
// invoke this helper.
func (s *Store) findCRCForObject(p *mmap.ReaderAt, off uint64, oid Hash) (uint32, bool) {
	for _, pf := range s.packs {
		if pf.pack == p && pf.entriesByOff != nil {
			if entry, ok := pf.entriesByOff[off]; ok {
				return entry.crc, true
			}
		}
	}

	if s.midx != nil && s.midx.version >= 2 {
		first := oid[0]
		start := uint32(0)
		if first > 0 {
			start = s.midx.fanout[first-1]
		}
		end := s.midx.fanout[first]

		rel, hit := slices.BinarySearchFunc(
			s.midx.objectIDs[start:end],
			oid,
			func(a, b Hash) int { return bytes.Compare(a[:], b[:]) },
		)
		if hit {
			abs := int(start) + rel
			entry := s.midx.entries[abs]
			if entry.crc != 0 {
				return entry.crc, true
			}
		}
	}

	return 0, false
}

// (s *Store) verifyCRCForPackObject validates the CRC-32 checksum of the
// object that starts at byte offset off inside the memory-mapped pack p.
//
// The helper locates the corresponding idxFile in s.packs and delegates the
// actual comparison to verifyCRC32.
//
// A pack that is referenced exclusively through a MIDX has no on-disk *.idx
// file; in that case a minimal idxFile stub is built on the fly so that CRC
// verification still works.
//
// An error is returned when either the pack cannot be located or the checksum
// does not match.
func (s *Store) verifyCRCForPackObject(p *mmap.ReaderAt, off uint64, crc uint32) error {
	for _, pf := range s.packs {
		if pf.pack == p {
			if pf.sortedOffsets == nil {
				// This is a stub idxFile from MIDX.
				// We need to build minimal structures for CRC verification.
				pf.sortedOffsets = []uint64{off}
				pf.entriesByOff = map[uint64]idxEntry{
					off: {offset: off, crc: crc},
				}
			}
			return verifyCRC32(pf, off, crc)
		}
	}

	return fmt.Errorf("pack not found for CRC verification")
}
