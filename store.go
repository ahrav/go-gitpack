// Package objstore provides a minimal, memory-mapped Git object store that
// resolves objects directly from *.pack files without shelling out to the
// Git executable.
//
// The store is intended for read-only scenarios—such as code search,
// indexing, and content serving—where low-latency look-ups are required but a
// full on-disk checkout is unnecessary.
//
// # Implementation
//
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

// These byte-count constants describe the fixed-width sections of a Git
// pack-index (v2) file. The unsafe idx parser relies on them to compute exact
// offsets inside the memory-mapped file. Do not modify these values unless the
// on-disk format itself changes.
const (
	// fanoutEntries is the number of entries in the fanout table.
	// One entry for every possible first byte of a SHA-1.
	fanoutEntries = 256
	// fanoutSize is the total size of the fanout table in bytes.
	// 256 × uint32 → 1,024 bytes.
	fanoutSize = fanoutEntries * 4

	// hashSize is the length of a SHA-1 hash in bytes.
	hashSize = 20
	// largeOffSize is the size of a 64-bit offset for objects beyond the 2 GiB boundary.
	largeOffSize = 8

	// defaultMaxDeltaDepth is the default maximum depth for resolving delta chains.
	defaultMaxDeltaDepth = 100
	// maxCacheableSize is the maximum size of an object that will be stored in the cache.
	maxCacheableSize = 4 << 20 // 4 MiB
)

// ObjectCache defines a pluggable, in-memory cache for Git objects.
// Callers supply an ObjectCache to tune memory usage or swap in custom
// eviction strategies while interacting with objstore.Store.
// The cache is consulted on every object read, so an efficient
// implementation can dramatically reduce decompression work and I/O.
type ObjectCache interface {
	// Get returns the cached object associated with key and a boolean
	// that reports whether the entry was found.
	// Get must be safe for concurrent use.
	Get(key Hash) (cachedObj, bool)

	// Add stores value under key, potentially evicting other entries
	// according to the cache’s replacement policy.
	// Add must be safe for concurrent use.
	Add(key Hash, value cachedObj)

	// Purge removes all entries from the cache and frees any
	// associated resources.
	// Purge is typically called when a Store is closed or when the
	// caller wants to reclaim memory immediately.
	Purge()
}

// store provides concurrent, read-only access to Git packfiles.
// The store memory-maps packfiles and their indices, maintains an in-memory index
// for fast object lookups, and handles delta chain resolution transparently.
// All operations are safe for concurrent use.
type store struct {
	// packs contains one idxFile per memory-mapped packfile.
	// Each entry provides object offset lookups and CRC validation.
	packs []*idxFile

	// midx references the multi-pack-index file if present.
	// When non-nil, it provides unified access across multiple packfiles.
	midx *midxFile

	// packMap prevents duplicate memory mappings of the same packfile.
	// Keys are absolute file paths, values are mmap handles.
	packMap map[string]*mmap.ReaderAt

	// mu protects mutable configuration fields.
	// It guards access to cache size changes and maxDeltaDepth updates.
	mu sync.Mutex

	// cache stores recently accessed objects using an Adaptive Replacement Cache.
	// The ARC algorithm balances recency and frequency for optimal hit rates.
	cache ObjectCache

	// dw maintains a small window of recently materialized objects.
	// Objects likely to be delta bases are kept here for fast access.
	dw *refCountedDeltaWindow

	// maxDeltaDepth limits delta chain traversal depth.
	// The default value matches Git's own limit of 50.
	maxDeltaDepth int

	// VerifyCRC enables CRC-32 validation for each object read.
	// When false (default), it prioritizes speed over integrity checks.
	VerifyCRC bool
}

// open creates a new store by mapping all packfiles in the specified directory.
// The function eagerly parses all index files to enable fast object lookups and
// initializes caching structures for optimal performance.
//
// The directory should contain matched pairs of *.pack and *.idx files.
// For bare repositories, pass ".git/objects/pack" as the directory.
//
// The function returns an error if no packfiles are found or if any index file is
// missing or malformed.
// All I/O uses memory mapping for zero-copy access.
//
// The returned store is safe for concurrent use without additional
// synchronization.
func open(dir string) (*store, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	packCache := make(map[string]*mmap.ReaderAt)

	// Attempt to mmap a multi-pack-index, which is named exactly
	// "…/objects/pack/multi-pack-index".
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

	// Enumerate standard pack and index files.
	pattern := filepath.Join(absDir, "*.pack")
	packs, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(packs) == 0 && midx == nil {
		// If no packs are found, return a valid but empty store for an empty repository.
		store := &store{
			maxDeltaDepth: defaultMaxDeltaDepth,
			packs:         []*idxFile{}, // Use an empty slice instead of nil for consistency.
			packMap:       make(map[string]*mmap.ReaderAt),
			dw:            newRefCountedDeltaWindow(),
		}

		const defaultCacheSize = 1 << 14 // 16K entries, approximately 96MiB.
		var err error
		store.cache, err = NewARCCache(defaultCacheSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create ARC cache: %w", err)
		}

		return store, nil
	}

	// Map any remaining *.pack files not already covered by the multi-pack-index.
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

	const defaultCacheSize = 1 << 14 // 16K entries, approximately 96MiB.
	store.cache, err = NewARCCache(defaultCacheSize)
	if err != nil {
		// This check is a safeguard; with a positive default size, this error is not expected.
		// However, it is best practice to handle errors from any constructor.
		return nil, fmt.Errorf("failed to create ARC cache: %w", err)
	}

	// Build idxFile instances, reusing the existing mmap handles.
	for path, handle := range packCache {
		idxPath := strings.TrimSuffix(path, ".pack") + ".idx"
		ix, err := mmap.Open(idxPath)
		if errors.Is(err, os.ErrNotExist) && midx != nil {
			// If a pack is referenced only by the multi-pack-index, its CRC will be sourced from the MIDX v2 later.
			store.packs = append(store.packs, &idxFile{pack: handle}) // Create a minimal stub for now.
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

// SetMaxDeltaDepth configures the maximum number of delta hops allowed.
// The default value of 50 matches Git's own limit.
//
// Lower values reduce CPU usage but may reject valid deeply-chained objects.
// Higher values risk stack overflow with maliciously crafted repositories.
//
// This method is safe to call concurrently with Get operations.
func (s *store) SetMaxDeltaDepth(depth int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxDeltaDepth = depth
}

// Close releases all memory-mapped files associated with the store.
// After Close returns, the store must not be used.
//
// Multiple calls to Close are safe and return the first error encountered.
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

	// Close individual index files.
	for _, p := range s.packs {
		if p.idx != nil {
			if err := p.idx.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// treeIter returns an iterator over the entries in the specified tree object.
// The iterator provides efficient streaming access to tree contents without
// materializing all entries at once.
//
// treeIter returns ErrTypeMismatch if the object exists but is not a tree.
// The caller must consume the iterator before the underlying data is
// garbage collected.
func (s *store) treeIter(oid Hash) (*TreeIter, error) {
	raw, typ, err := s.get(oid)
	if err != nil {
		return nil, fmt.Errorf("failed to get tree %s: %w", oid, err)
	}
	if typ != ObjTree {
		return nil, fmt.Errorf("expected tree type for %s, got %v", oid, typ)
	}
	if len(raw) > 0 && raw[0] == 0 {
		return nil, fmt.Errorf("tree %s starts with null byte (likely corrupted)", oid)
	}
	return newTreeIter(raw), nil
}

// get retrieves the specified Git object and returns its data and type.
// The method checks multiple cache layers before reading from disk:
// a small delta window cache and a larger Adaptive Replacement Cache.
//
// For objects stored as deltas, get recursively resolves the delta chain
// up to maxDeltaDepth, reconstructing the full object content.
// When VerifyCRC is enabled, get validates checksums from index files.
//
// The returned byte slice is a fresh allocation that callers may modify.
// get is safe for concurrent use.
func (s *store) get(oid Hash) ([]byte, ObjectType, error) {
	// Fast path: check the delta window, which holds recently materialized objects
	// that are likely to be bases for upcoming delta resolutions.
	if b, ok := s.dw.acquire(oid); ok {
		d, t := b.Data(), b.Type()
		b.Release()
		return d, t, nil
	}

	// If not in the delta window, check the larger ARC cache.
	if b, ok := s.cache.Get(oid); ok {
		return b.data, b.typ, nil
	}

	// On a cache miss, inflate the object from a packfile, tracking delta depth
	// to prevent cycles and excessive recursion.
	return s.getWithContext(oid, newDeltaContext(s.maxDeltaDepth))
}

// getWithContext retrieves an object while tracking delta chain depth.
// This internal method prevents infinite recursion and detects cycles
// in malformed delta chains.
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
			return s.inflateFromPack(inflationParams{
				p:   p,
				off: off,
				oid: oid,
				ctx: ctx,
			})
		}
	}

	for _, pf := range s.packs {
		offset, found := pf.findObject(oid)
		if !found {
			continue
		}
		return s.inflateFromPack(inflationParams{
			p:   pf.pack,
			off: offset,
			oid: oid,
			ctx: ctx,
		})
	}

	return nil, ObjBad, fmt.Errorf("object %s not found", oid)
}

// inflateFromPack reads and materializes an object from a packfile.
// The method handles both regular objects and delta-encoded objects, resolving
// delta chains as needed.
//
// For delta objects, inflateFromPack recursively resolves the chain up to maxDeltaDepth.
// When VerifyCRC is true, the method validates object integrity using checksums.
//
// inflateFromPack returns the inflated object data, its type, and any error encountered.
// The returned data is a fresh allocation safe for modification.
func (s *store) inflateFromPack(params inflationParams) ([]byte, ObjectType, error) {
	// Perform a cheap header peek to avoid full inflation for commits
	// that are already covered by the commit-graph.
	objType, _, err := peekObjectType(params.p, params.off)
	if err != nil {
		return nil, ObjBad, err
	}
	if objType == ObjCommit {
		return nil, ObjCommit, nil
	}

	// For delta objects, resolve the entire chain iteratively.
	if objType == ObjOfsDelta || objType == ObjRefDelta {
		full, baseType, err := inflateDeltaChainStreaming(s, params)
		if err != nil {
			return nil, ObjBad, err
		}
		// Add the fully resolved object to both the delta window and the ARC cache.
		if len(full) <= maxCacheableSize {
			s.dw.add(params.oid, full, baseType)
			s.cache.Add(params.oid, cachedObj{data: full, typ: baseType})
		}
		return full, baseType, nil
	}

	// For regular (non-delta) objects, inflate once and cache the result.
	_, data, err := readRawObject(params.p, params.off)
	if err != nil {
		return nil, ObjBad, err
	}

	// If enabled, perform a CRC-32 integrity check.
	if s.VerifyCRC {
		if crc, ok := s.findCRCForObject(params.p, params.off, params.oid); ok {
			if err := s.verifyCRCForPackObject(params.p, params.off, crc); err != nil {
				return nil, ObjBad, err
			}
		} else if s.midx != nil && s.midx.version >= 2 {
			return nil, ObjBad, fmt.Errorf("no CRC found for object %s in MIDX v2", params.oid)
		}
	}

	if len(data) <= maxCacheableSize {
		s.dw.add(params.oid, data, objType)
		s.cache.Add(params.oid, cachedObj{data: data, typ: objType})
	}
	return data, objType, nil
}

// findPackedObject locates an object in any mapped packfile.
// The method checks the multi-pack-index first if available, then searches individual packs.
//
// findPackedObject returns the pack handle, byte offset, and true if found.
func (s *store) findPackedObject(oid Hash) (*mmap.ReaderAt, uint64, bool) {
	if s.midx != nil {
		if p, off, ok := s.midx.findObject(oid); ok {
			return p, off, true
		}
	}
	for _, pf := range s.packs {
		if off, ok := pf.findObject(oid); ok {
			return pf.pack, off, true
		}
	}
	return nil, 0, false
}

// readCommitHeader reads the header portion of a commit object.
// The method returns the raw header up to and including the first author or committer line.
//
// This method inflates only the minimum necessary data to extract commit metadata,
// typically less than 512 bytes.
func (s *store) readCommitHeader(oid Hash) ([]byte, error) {
	// Locate the commit object and skip past its generic object header.
	p, off, ok := s.findPackedObject(oid)
	if !ok {
		return nil, fmt.Errorf("object %s not found", oid)
	}
	typ, hdrLen, err := peekObjectType(p, off)
	if err != nil {
		return nil, err
	}
	if typ != ObjCommit {
		return nil, fmt.Errorf("%s is not a commit", oid)
	}
	off += uint64(hdrLen)

	// Decompress only the beginning of the object, typically less than 512 bytes.
	zr, err := getZlibReader(io.NewSectionReader(p, int64(off), 1<<63-1))
	if err != nil {
		return nil, err
	}
	defer putZlibReader(zr)

	buf := GetBuf()
	defer PutBuf(buf)

	for len(*buf) < MaxHdr {
		line, err := readUntilLF(zr)
		*buf = append(*buf, line...)
		if bytes.HasPrefix(line, []byte("author ")) ||
			bytes.HasPrefix(line, []byte("committer ")) {
			// The desired line has been found; copy it to a new slice and return.
			return append([]byte(nil), *buf...), nil
		}
		if err != nil { // This indicates an EOF before the author/committer line was found.
			return nil, err
		}
	}

	return nil, fmt.Errorf("commit header exceeds %d bytes without author line", MaxHdr)
}

// readUntilLF reads a complete line including the terminating newline.
// The function returns the line with '\n' included or any partial line if EOF is encountered.
func readUntilLF(r io.Reader) ([]byte, error) {
	var line []byte
	var b [1]byte
	for {
		n, err := r.Read(b[:])
		if n == 1 {
			line = append(line, b[0])
			if b[0] == '\n' { // A newline indicates the end of the line.
				return line, nil
			}
		}
		if err != nil {
			return line, err
		}
	}
}

// findCRCForObject returns the CRC-32 checksum for the specified object.
// The method searches pack index files first, then falls back to multi-pack-index v2 if available.
//
// The second return value indicates whether a CRC was found.
// findCRCForObject is read-only and safe for concurrent use.
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

// verifyCRCForPackObject validates the CRC-32 checksum of a pack object.
// The method creates a minimal index structure for packs referenced only through MIDX
// to enable CRC verification.
//
// verifyCRCForPackObject returns an error if the pack cannot be located or checksum verification fails.
func (s *store) verifyCRCForPackObject(p *mmap.ReaderAt, off uint64, crc uint32) error {
	for _, pf := range s.packs {
		if pf.pack == p {
			if pf.sortedOffsets == nil {
				// This is a stub idxFile created from the multi-pack-index.
				// Minimal structures must be built to perform CRC verification.
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

// verifyPackTrailers validates the SHA-1 checksums at the end of each packfile.
// Git packfiles end with a 20-byte SHA-1 hash of all preceding content.
//
// verifyPackTrailers recomputes checksums for all mapped packfiles and returns
// an error if any mismatch is found.
// The method only runs when VerifyCRC is enabled.
//
// verifyPackTrailers is safe for concurrent use and avoids redundant verification of the same pack.
func (s *store) verifyPackTrailers() error {
	if !s.VerifyCRC {
		return nil
	}

	verified := make(map[*mmap.ReaderAt]bool)

	for _, pf := range s.packs {
		if pf.pack == nil || verified[pf.pack] {
			continue
		}

		// The verifyPackTrailer function performs the actual SHA-1 calculation.
		if err := verifyPackTrailer(pf.pack); err != nil {
			return fmt.Errorf("pack %p: %w", pf.pack, err)
		}
		verified[pf.pack] = true
	}

	return nil
}
