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
// The store is safe for concurrent readers and eliminates the need to shell out
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
	// to call (*store).Close() do not trigger ERROR_LOCK_VIOLATION.
	if runtime.GOOS == "windows" {
		runtime.SetFinalizer(&store{}, func(s *store) { _ = s.Close() })
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

// cachedObj pairs a fully-materialized Git object with its on-disk
// ObjectType.
//
// The ARC cache inside store uses cachedObj as its value type so that a
// cache hit returns both the raw bytes and the already-known object type.
// This avoids having to call detectType on every lookup.
//
// A cachedObj is immutable after it has been placed in the cache.
type cachedObj struct {
	// data holds the complete, inflated object contents.
	// The slice must not be mutated once the value is cached.
	data []byte

	// typ records the object's kind (blob, tree, commit, tag, …) so callers
	// can skip type detection on a cache hit.
	typ ObjectType
}

// store provides concurrent, read-only access to one or more memory-mapped
// Git packfiles with support for multi-pack-index (MIDX) files.
//
// A store maps every *.pack / *.idx pair found in a directory, maintains an
// in-memory index from object ID to pack offset, inflates objects on demand,
// and resolves delta chains up to maxDeltaDepth. All methods are safe for
// concurrent use by multiple goroutines.
//
// CRC Verification:
// When VerifyCRC is enabled, the store validates CRC-32 checksums for each
// object read. For objects in regular packs with .idx files, CRCs are always
// available. For objects that only appear in a multi-pack-index:
//   - MIDX v1: CRC verification is silently skipped (no CRC data available)
//   - MIDX v2: CRCs are read from the CRCS chunk and verified
//
// Note: Pack trailer checksums can be verified by calling verifyPackTrailers()
// after opening the store if additional integrity checking is required.
type store struct {
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
	cache *arc.ARCCache[Hash, cachedObj]

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

// open scans dir for "*.pack" files, expects a matching "*.idx" companion for
// each one, memory-maps every pair, and returns a read-only *store.
//
// open eagerly parses all index files so that subsequent store.Get calls are
// an O(1) table lookup followed by lazy object inflation.
// A small Adaptive-Replacement cache (ARC) is created up-front to avoid
// repeated decompression of hot objects.
//
// Error semantics:
//   - If no "*.pack" files are found in dir an error is returned.
//   - If any "*.idx" companion is missing or malformed, open fails.
//   - All I/O is performed with mmap; failures when mapping a file are
//     surfaced immediately.
//
// The returned store is safe for concurrent readers; no additional
// synchronization is required by the caller.
//
// For bare repositories pass ".git/objects/pack" as dir.
func open(dir string) (*store, error) {
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
		store := &store{
			maxDeltaDepth: defaultMaxDeltaDepth,
			packs:         []*idxFile{}, // Empty slice instead of nil
			packMap:       make(map[string]*mmap.ReaderAt),
			dw:            newRefCountedDeltaWindow(),
		}

		const defaultCacheSize = 1 << 14 // 16K entries ~ 96MiB
		var err error
		store.cache, err = arc.NewARC[Hash, cachedObj](defaultCacheSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create ARC cache: %w", err)
		}

		return store, nil
	}

	// Glob all *.pack not already mapped by the midx.
	for _, pack := range packs {
		if _, ok := packCache[pack]; ok {
			continue
		}
		h, err := mmap.Open(pack)
		if err != nil {
			return nil, err
		}
		packCache[pack] = h
	}

	store := &store{
		maxDeltaDepth: defaultMaxDeltaDepth,
		midx:          midx,
		packMap:       packCache,
		dw:            newRefCountedDeltaWindow(),
	}

	const defaultCacheSize = 1 << 14 // 16K entries ~ 96MiB
	store.cache, err = arc.NewARC[Hash, cachedObj](defaultCacheSize)
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
// (ref-delta or ofs-delta) that the store will follow when materializing an
// object.
// The default of 50 matches Git's own hard limit.
//
// Lower values reduce worst-case CPU usage but may reject valid objects.
// Higher values increase resource consumption and the risk of stack
// overflow if a crafted repository contains excessively deep delta chains.
//
// This method is safe for concurrent use with Get.
func (s *store) SetMaxDeltaDepth(depth int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxDeltaDepth = depth
}

// Close unmaps all pack and idx files.
//
// Close releases every memory-mapped file that open created.
// After Close returns, the store must not be used.
//
// Calling Close multiple times is safe; the first error that occurs while
// unmapping a file is returned.
func (s *store) Close() error {
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

// TreeIter returns a streaming iterator over the contents of the tree object
// identified by oid.  The caller must consume the iterator before the returned
// raw slice would otherwise be garbage‑collected.
func (s *store) TreeIter(oid Hash) (*TreeIter, error) {
	raw, typ, err := s.Get(oid)
	if err != nil {
		return nil, err
	}
	if typ != ObjTree {
		return nil, ErrTypeMismatch
	}
	return newTreeIter(raw), nil
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
// capped by store.maxDeltaDepth—until it reaches the base object, then
// applies each delta in order to reconstruct the full byte stream.
//
// When store.VerifyCRC is true, Get validates the CRC-32 checksum recorded
// in the corresponding *.idx file (or multi-pack-index v2) and returns an
// error on mismatch.
//
// The returned slice is always a fresh allocation; callers may mutate it at
// will.
// Get is safe for concurrent use by multiple goroutines.
func (s *store) Get(oid Hash) ([]byte, ObjectType, error) {
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

// inflateFromPack materializes the Git object that starts at off inside the
// memory-mapped pack p.
//
// The helper is the workhorse of store.Get. It
//   - peeks at the pack header to determine the on-disk type with
//     peekObjectType, short-circuiting OBJ_COMMIT objects that can be served
//     from the commit-graph;
//   - inflates non-delta objects directly with readRawObject;
//   - resolves both ref-delta and ofs-delta chains recursively via
//     deltaContext, bounded by store.maxDeltaDepth and with cycle detection;
//   - applies delta instructions with applyDelta to reconstruct the full
//     byte stream; and
//   - verifies CRC-32 checksums when store.VerifyCRC is enabled, using either
//     the *.idx side-file or, for multi-pack-index v2, the MIDX CRC table.
//
// A successful call returns a freshly-allocated buffer that the caller may
// mutate, the detected ObjectType, and a nil error.
// It returns ObjBad and a non-nil error when header parsing, delta
// resolution, decompression, or CRC validation fails.
//
// The method is read-only with respect to *store (aside from its caches) and
// is safe for concurrent use; all shared state mutation is confined to the
// cache helpers, which are internally synchronized.
func (s *store) inflateFromPack(
	p *mmap.ReaderAt,
	off uint64,
	oid Hash,
	ctx *deltaContext,
) ([]byte, ObjectType, error) {
	// Cheap header peek: skip full inflation for commits that
	// the commit-graph already covers.
	objType, _, err := peekObjectType(p, off)
	if err != nil {
		return nil, ObjBad, err
	}
	if objType == ObjCommit {
		return nil, ObjCommit, nil
	}

	// Delta objects – resolve the whole chain **iteratively**.
	if objType == ObjOfsDelta || objType == ObjRefDelta {
		full, baseType, err := s.inflateDeltaChain(p, off, oid, ctx)
		if err != nil {
			return nil, ObjBad, err
		}
		// Update both caches.
		s.dw.add(oid, full, baseType)
		s.cache.Add(oid, cachedObj{data: full, typ: baseType})
		return full, baseType, nil
	}

	// Regular (non-delta) object: inflate once and cache.
	_, data, err := readRawObject(p, off)
	if err != nil {
		return nil, ObjBad, err
	}

	// Optional CRC-32 integrity check.
	if s.VerifyCRC {
		if crc, ok := s.findCRCForObject(p, off, oid); ok {
			if err := s.verifyCRCForPackObject(p, off, crc); err != nil {
				return nil, ObjBad, err
			}
		} else if s.midx != nil && s.midx.version >= 2 {
			return nil, ObjBad, fmt.Errorf("no CRC found for object %x in MIDX v2", oid)
		}
	}

	s.dw.add(oid, data, objType)
	s.cache.Add(oid, cachedObj{data: data, typ: objType})
	return data, objType, nil
}

// peekObjectType reads at most 32 bytes to discover the on‑disk type without
// inflating the body. It also returns the header length (needed by callers
// that want to inflate manually afterwards).
func peekObjectType(r *mmap.ReaderAt, off uint64) (ObjectType, int, error) {
	var buf [32]byte
	n, err := r.ReadAt(buf[:], int64(off))
	if err != nil && !errors.Is(err, io.EOF) {
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
func (s *store) getWithContext(oid Hash, ctx *deltaContext) ([]byte, ObjectType, error) {
	if b, ok := s.dw.acquire(oid); ok {
		d, t := b.Data(), b.Type()
		b.Release()
		return d, t, nil
	}

	if b, ok := s.cache.Get(oid); ok {
		return b.data, b.typ, nil
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
// The implementation follows these steps:
//
//  1. Parse the variable‑length object header to determine type and size;
//  2. Read (and keep) the delta prefix, if any:
//     - For OBJ_REF_DELTA: 20‑byte SHA‑1 base hash
//     - For OBJ_OFS_DELTA: variable‑length negative offset
//  3. Inflate the deflate stream that follows the header and prefix;
//  4. Return [prefix||inflated] for deltas, or just the inflated body otherwise.
func readRawObject(r *mmap.ReaderAt, off uint64) (ObjectType, []byte, error) {
	// Parse the generic variable‑length header.
	var hdr [32]byte
	n, err := r.ReadAt(hdr[:], int64(off))
	if err != nil && !errors.Is(err, io.EOF) {
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

	// Read (and keep) the delta prefix, if any.
	var prefix []byte
	switch objType {
	case ObjRefDelta:
		prefix = make([]byte, 20)
		if _, err := r.ReadAt(prefix, pos); err != nil {
			return ObjBad, nil, err
		}
		pos += 20

	case ObjOfsDelta:
		// variable‑length negative offset.
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

	// Inflate the deflate stream that follows.

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

// inflateDeltaChain resolves a delta chain iteratively and returns the fully
// materialized object plus its final ObjectType (blob / tree / …).
//
// The algorithm:
//
//  1. Starting at (pack p, offset off) walk _up_ the chain, pushing each
//     delta‑instruction stream onto `chain`, until we reach a non‑delta base.
//  2. Apply the deltas in reverse order, re‑using the same output buffer.
//
// At most ctx.maxDepth hops are followed and ctx.detect… helpers prevent
// cycles in both ref‑ and ofs‑deltas.
func (s *store) inflateDeltaChain(
	p *mmap.ReaderAt,
	off uint64,
	oid Hash,
	ctx *deltaContext,
) ([]byte, ObjectType, error) {
	type hop struct{ delta []byte }

	var (
		chain    []hop
		basePack = p
		baseOff  = off
		baseData []byte
		baseType ObjectType
	)

	for depth := 0; depth < ctx.maxDepth; depth++ {

		typ, data, err := readRawObject(basePack, baseOff)
		if err != nil {
			return nil, ObjBad, err
		}

		if typ != ObjOfsDelta && typ != ObjRefDelta {
			// Reached the non‑delta base object – stop walking.
			baseData = data
			baseType = typ
			break
		}

		bHash, bOff, deltaBuf, err := parseDeltaHeader(typ, data)
		if err != nil {
			return nil, ObjBad, err
		}

		// Record this hop's delta stream.
		chain = append(chain, hop{delta: deltaBuf})

		// Advance to the referenced base.
		if typ == ObjRefDelta {
			if err := ctx.checkRefDelta(bHash); err != nil {
				return nil, ObjBad, err
			}
			ctx.enterRefDelta(bHash)

			// Locate (maybe in another pack).
			if s.midx != nil {
				if np, noff, ok := s.midx.findObject(bHash); ok {
					basePack, baseOff = np, noff
					continue
				}
			}
			found := false
			for _, pf := range s.packs {
				if noff, ok := pf.findObject(bHash); ok {
					basePack, baseOff, found = pf.pack, noff, true
					break
				}
			}
			if !found {
				return nil, ObjBad, fmt.Errorf("base object %x not found", bHash)
			}

		} else { // ObjOfsDelta
			actual := baseOff - bOff
			if err := ctx.checkOfsDelta(actual); err != nil {
				return nil, ObjBad, err
			}
			ctx.enterOfsDelta(actual)

			baseOff = actual
		}
	}

	if baseData == nil {
		return nil, ObjBad, fmt.Errorf("delta chain too deep (>%d)", ctx.maxDepth)
	}

	// Apply deltas bottom‑up.

	out := baseData
	for i := len(chain) - 1; i >= 0; i-- {
		out = applyDelta(out, chain[i].delta)
		if out == nil {
			return nil, ObjBad, fmt.Errorf("delta application failed for %x", oid)
		}
	}

	return out, baseType, nil
}

// verifyPackTrailers validates the SHA-1 trailer that closes every mapped
// packfile.
//
// A Git pack ends with a 20-byte checksum that covers all preceding bytes.
// VerifyPackTrailers re-computes that checksum for each unique *.pack mapping
// and returns an error if any mismatch is found.
//
// The method is a no-op unless the caller has enabled store.VerifyCRC.
// It is safe for concurrent use; all work is performed on local variables and
// no shared state in *store is mutated.
// Repeated invocations are cheap—the same mmap handle is processed only once
// during a single call.
func (s *store) verifyPackTrailers() error {
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

// (s *store) findCRCForObject returns the CRC-32 checksum Git recorded for
// the object identified by oid.
//
// It looks for the checksum in the per-pack *.idx files first and, if it
// fails to locate it there, falls back to the multi-pack-index (MIDX) when
// one is present and of version ≥ 2 (the first version that stores CRCs).
//
// The bool result reports whether a checksum was found.
// The method is read-only, safe for concurrent use, and does not depend on
// store.VerifyCRC—that flag merely controls whether the caller decides to
// invoke this helper.
func (s *store) findCRCForObject(p *mmap.ReaderAt, off uint64, oid Hash) (uint32, bool) {
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

// (s *store) verifyCRCForPackObject validates the CRC-32 checksum of the
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
func (s *store) verifyCRCForPackObject(p *mmap.ReaderAt, off uint64, crc uint32) error {
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
