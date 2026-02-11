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
// It coordinates between pack-index (IDX) and reverse-index (RIDX) data,
// and builds a unified in-memory lookup across packfiles with transparent
// delta chain resolution and caching.
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

var (
	ErrObjectNotCommit = errors.New("object is not a commit")
	ErrObjectNotFound  = errors.New("object not found")
)

func objectNotFoundError(oid Hash) error {
	return fmt.Errorf("%w: %x", ErrObjectNotFound, oid)
}

// copyBytes returns a new byte slice with the same contents as src.
// It is used by cache-hit paths to ensure callers receive an independent
// copy that they may safely mutate without corrupting cached data.
func copyBytes(src []byte) []byte {
	cp := make([]byte, len(src))
	copy(cp, src)
	return cp
}

func init() {
	// On Windows, memory-mapped files hold an OS-level lock that prevents the
	// file from being deleted, renamed, or opened exclusively by another
	// process. If a store is garbage-collected without an explicit Close(),
	// those locks remain until the process exits, causing ERROR_LOCK_VIOLATION
	// for other Git operations. A runtime finalizer provides a safety net by
	// closing the mappings when the GC reclaims the store.
	//
	// This is Windows-only because Unix mmap does not hold a file lock; an
	// unlinked file with outstanding mappings is silently reclaimed by the
	// kernel when the last mapping is unmapped or the process exits.
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

	// defaultMaxDeltaObjectSize bounds delta materialization to prevent runaway
	// allocations on very large delta chains.
	defaultMaxDeltaObjectSize = 512 << 20 // 512 MiB

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
//
// Two-tier caching strategy:
//
// Objects pass through two cache layers before reaching the caller:
//
//  1. Delta window (dw) -- a small, sharded, write-through window that holds
//     recently inflated objects. It is optimized for the access pattern of
//     delta-chain resolution, where the same base object is often needed by
//     multiple child deltas in quick succession. Objects are inserted here on
//     first inflation.
//
//  2. ARC cache (cache) -- a larger Adaptive Replacement Cache that balances
//     recency and frequency. Objects are promoted from the delta window to
//     the ARC on their *second* access (i.e. when get() finds a delta-window
//     hit). This two-step promotion avoids polluting the ARC with one-shot
//     objects during bulk scans and reduces lock contention on the ARC's
//     internal mutexes during high-throughput inflation.
//
// Together, the two layers ensure that hot delta bases remain resident (via
// the delta window) while frequently accessed non-delta objects benefit from
// the ARC's superior eviction policy.
type store struct {
	// packs contains one idxFile per memory-mapped packfile.
	// Each entry provides object offset lookups and CRC validation.
	packs []*idxFile

	// memoryMidx is synthesized from individual *.idx files.
	memoryMidx *inMemoryMidx

	// objectsDir points to ".git/objects" and is used for loose-object fallback.
	objectsDir string

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
	dw deltaWindow

	// maxDeltaDepth limits delta chain traversal depth.
	// The default value is 100 (see defaultMaxDeltaDepth).
	maxDeltaDepth int

	// maxDeltaObjectSize bounds delta target materialization.
	// Zero disables the bound.
	maxDeltaObjectSize uint64

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
// If no packfiles are found, a valid store is still returned and loose-object
// fallback remains available.
//
// The function returns an error if any discovered index file is missing or malformed.
// All I/O uses memory mapping for zero-copy access.
//
// The returned store is safe for concurrent use without additional
// synchronization.
func open(dir string) (*store, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	objectsDir := filepath.Dir(absDir)

	packCache := make(map[string]*mmap.ReaderAt)

	// Enumerate standard pack and index files.
	pattern := filepath.Join(absDir, "*.pack")
	packs, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(packs) == 0 {
		// If no packs are found, return a valid but empty store for an empty repository.
		store := &store{
			maxDeltaDepth:      defaultMaxDeltaDepth,
			maxDeltaObjectSize: defaultMaxDeltaObjectSize,
			packs:              []*idxFile{}, // Use an empty slice instead of nil for consistency.
			objectsDir:         objectsDir,
			packMap:            make(map[string]*mmap.ReaderAt),
			dw:                 newShardedDeltaWindow(64, windowBudget),
		}

		const defaultCacheSize = 1 << 14 // 16K entries, approximately 96MiB.
		var err error
		store.cache, err = NewARCCache(defaultCacheSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create ARC cache: %w", err)
		}

		return store, nil
	}

	// Map all pack files in the directory.
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
		maxDeltaDepth:      defaultMaxDeltaDepth,
		maxDeltaObjectSize: defaultMaxDeltaObjectSize,
		objectsDir:         objectsDir,
		packMap:            packCache,
		dw:                 newShardedDeltaWindow(64, windowBudget),
	}

	const defaultCacheSize = 1 << 14 // 16K entries, approximately 96MiB.
	store.cache, err = NewARCCache(defaultCacheSize)
	if err != nil {
		// This check is a safeguard; with a positive default size, this error is not expected.
		// However, it is best practice to handle errors from any constructor.
		return nil, fmt.Errorf("failed to create ARC cache: %w", err)
	}

	// Build idxFile instances in deterministic path order, reusing existing mmap handles.
	packPaths := make([]string, 0, len(packCache))
	for path := range packCache {
		packPaths = append(packPaths, path)
	}
	slices.Sort(packPaths)
	for _, path := range packPaths {
		handle := packCache[path]
		idxPath := strings.TrimSuffix(path, ".pack") + ".idx"
		ix, err := mmap.Open(idxPath)
		if err != nil {
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
	if len(store.packs) > 1 {
		store.memoryMidx = buildInMemoryMidx(store.packs)
	}

	return store, nil
}

// SetMaxDeltaDepth configures the maximum number of delta hops allowed.
// The default value is 100 (see defaultMaxDeltaDepth).
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

// SetMaxDeltaObjectSize configures the maximum reconstructed delta size in bytes.
// Values greater than zero prevent excessive allocations for huge delta chains.
func (s *store) SetMaxDeltaObjectSize(maxBytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxDeltaObjectSize = maxBytes
}

// Close releases all memory-mapped files associated with the store.
// After Close returns, the store must not be used.
// Close must be called exactly once; calling it multiple times may
// attempt to close already-closed handles.
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
		return nil, fmt.Errorf("%w: expected tree type for %s, got %v", ErrTypeMismatch, oid, typ)
	}
	if len(raw) > 0 && raw[0] == 0 {
		return nil, fmt.Errorf("tree %s starts with null byte (likely corrupted)", oid)
	}
	return getTreeIter(raw), nil
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
		cp := copyBytes(d)
		// Promote to ARC cache on second access (delta window hit).
		// Store the copy in the ARC cache so it doesn't alias the delta window buffer.
		if len(cp) <= maxCacheableSize {
			s.cache.Add(oid, cachedObj{data: cp, typ: t})
		}
		b.Release()
		return cp, t, nil
	}

	// If not in the delta window, check the larger ARC cache.
	if b, ok := s.cache.Get(oid); ok {
		return copyBytes(b.data), b.typ, nil
	}

	// On a cache miss, inflate the object from a packfile, tracking delta depth
	// to prevent cycles and excessive recursion. Skip cache checks since we
	// already verified both caches above.
	ctx := getDeltaContext(s.maxDeltaDepth)
	defer putDeltaContext(ctx)
	return s.getWithContextSkipCache(oid, ctx)
}

// getMaterialized retrieves the fully materialized object, including commit bodies.
func (s *store) getMaterialized(oid Hash) ([]byte, ObjectType, error) {
	if b, ok := s.dw.acquire(oid); ok {
		d, t := b.Data(), b.Type()
		cp := copyBytes(d)
		b.Release()
		return cp, t, nil
	}

	if b, ok := s.cache.Get(oid); ok {
		return copyBytes(b.data), b.typ, nil
	}

	p, off, ok := s.findPackedObject(oid)
	if !ok {
		return s.readLooseObject(oid)
	}

	ctx := getDeltaContext(s.maxDeltaDepth)
	defer putDeltaContext(ctx)
	return s.inflateFromPackWithOptions(inflationParams{
		p:             p,
		off:           off,
		oid:           oid,
		ctx:           ctx,
		maxObjectSize: s.maxDeltaObjectSize,
	}, false, true)
}

// getNoCache retrieves the specified Git object without touching cache layers.
func (s *store) getNoCache(oid Hash) ([]byte, ObjectType, error) {
	p, off, ok := s.findPackedObject(oid)
	if !ok {
		return s.readLooseObject(oid)
	}
	ctx := getDeltaContext(s.maxDeltaDepth)
	defer putDeltaContext(ctx)
	return s.inflateFromPackWithOptions(inflationParams{
		p:             p,
		off:           off,
		oid:           oid,
		ctx:           ctx,
		maxObjectSize: s.maxDeltaObjectSize,
	}, true, false)
}

// getPackedObjectNoCache materializes the object at a known pack offset
// without consulting in-memory caches or performing an OID lookup.
func (s *store) getPackedObjectNoCache(p *mmap.ReaderAt, off uint64, oid Hash) ([]byte, ObjectType, error) {
	if p == nil {
		return nil, ObjBad, fmt.Errorf("pack handle is nil")
	}
	ctx := getDeltaContext(s.maxDeltaDepth)
	defer putDeltaContext(ctx)
	return s.inflateFromPackWithOptions(inflationParams{
		p:             p,
		off:           off,
		oid:           oid,
		ctx:           ctx,
		maxObjectSize: s.maxDeltaObjectSize,
	}, false, false)
}

// getWithContext retrieves an object while tracking delta chain depth.
// This internal method prevents infinite recursion and detects cycles
// in malformed delta chains.
func (s *store) getWithContext(oid Hash, ctx *deltaContext) ([]byte, ObjectType, error) {
	if b, ok := s.dw.acquire(oid); ok {
		d, t := b.Data(), b.Type()
		cp := copyBytes(d)
		b.Release()
		return cp, t, nil
	}

	if b, ok := s.cache.Get(oid); ok {
		return copyBytes(b.data), b.typ, nil
	}

	if s.memoryMidx != nil {
		if p, off, ok := s.memoryMidx.findObject(oid); ok {
			return s.inflateFromPack(inflationParams{
				p:             p,
				off:           off,
				oid:           oid,
				ctx:           ctx,
				maxObjectSize: s.maxDeltaObjectSize,
			})
		}
	}

	for _, pf := range s.packs {
		offset, found := pf.findObject(oid)
		if !found {
			continue
		}
		return s.inflateFromPack(inflationParams{
			p:             pf.pack,
			off:           offset,
			oid:           oid,
			ctx:           ctx,
			maxObjectSize: s.maxDeltaObjectSize,
		})
	}
	data, typ, err := s.readLooseObject(oid)
	if err == nil {
		if len(data) <= maxCacheableSize {
			s.dw.add(oid, data, typ)
			s.cache.Add(oid, cachedObj{data: data, typ: typ})
		}
		// Return a copy so callers cannot corrupt the cached data.
		return copyBytes(data), typ, nil
	}
	return nil, ObjBad, err
}

// getWithContextSkipCache is like getWithContext but skips the delta-window
// and ARC cache checks.
//
// Duplication rationale: getWithContext and getWithContextSkipCache share
// nearly identical pack-lookup and loose-object-fallback logic. The
// duplication is intentional -- the public get() method already checks both
// cache tiers before calling into this code path. Re-checking them here
// would add two lock-guarded map lookups per cache-miss object, which is
// measurable during bulk scans that inflate millions of objects. Keeping a
// separate "skip cache" variant avoids that overhead at the cost of a small
// amount of code duplication that is straightforward to maintain.
func (s *store) getWithContextSkipCache(oid Hash, ctx *deltaContext) ([]byte, ObjectType, error) {
	if s.memoryMidx != nil {
		if p, off, ok := s.memoryMidx.findObject(oid); ok {
			return s.inflateFromPack(inflationParams{
				p:             p,
				off:           off,
				oid:           oid,
				ctx:           ctx,
				maxObjectSize: s.maxDeltaObjectSize,
			})
		}
	}

	for _, pf := range s.packs {
		offset, found := pf.findObject(oid)
		if !found {
			continue
		}
		return s.inflateFromPack(inflationParams{
			p:             pf.pack,
			off:           offset,
			oid:           oid,
			ctx:           ctx,
			maxObjectSize: s.maxDeltaObjectSize,
		})
	}
	data, typ, err := s.readLooseObject(oid)
	if err == nil {
		if len(data) <= maxCacheableSize {
			s.dw.add(oid, data, typ)
			s.cache.Add(oid, cachedObj{data: data, typ: typ})
		}
		// Return a copy so callers cannot corrupt the cached data.
		return copyBytes(data), typ, nil
	}
	return nil, ObjBad, err
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
	return s.inflateFromPackWithOptions(params, true, true)
}

// inflateFromPackWithOptions reads and materializes an object from a packfile
// with fine-grained control over the commit fast-path and caching behavior.
//
// Parameters:
//   - allowCommitFastPath: when true and the object at params.off is a plain
//     (non-delta) commit, the method returns (nil, ObjCommit, nil) immediately
//     without inflating the body. The nil data signals to the caller that
//     commit metadata should be obtained from the commit-graph instead. This
//     optimization avoids zlib decompression for commits whose headers are
//     already available in the graph. Callers that need the full commit body
//     (e.g. getMaterialized) must pass false.
//   - cacheResult: when true the inflated object is written to the delta
//     window (and potentially promoted to the ARC cache on later access).
//     Callers that will not re-read the object should pass false to avoid
//     evicting useful entries.
func (s *store) inflateFromPackWithOptions(params inflationParams, allowCommitFastPath, cacheResult bool) ([]byte, ObjectType, error) {
	// Perform a cheap header peek to avoid full inflation for commits
	// that are already covered by the commit-graph.
	objType, _, err := peekObjectType(params.p, params.off)
	if err != nil {
		return nil, ObjBad, err
	}
	if allowCommitFastPath && objType == ObjCommit {
		return nil, ObjCommit, nil
	}

	// For delta objects, resolve the entire chain iteratively.
	if objType == ObjOfsDelta || objType == ObjRefDelta {
		var full []byte
		var baseType ObjectType
		var err error
		if cacheResult {
			full, baseType, err = inflateDeltaChainStreaming(s, params)
		} else {
			// Borrowed path: skip final copy for consume-and-discard callers.
			full, baseType, err = inflateDeltaChainBorrowed(s, params)
		}
		if err != nil {
			return nil, ObjBad, err
		}
		// Write only to the delta window on first inflation. Objects are
		// promoted to the ARC cache on second access (via get()'s cache-hit
		// path), reducing lock contention on the ARC during bulk inflation.
		if cacheResult && len(full) <= maxCacheableSize {
			s.dw.add(params.oid, full, baseType)
			// Return a copy so the caller cannot corrupt the cached data.
			return copyBytes(full), baseType, nil
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
		}
	}

	if cacheResult && len(data) <= maxCacheableSize {
		s.dw.add(params.oid, data, objType)
		// Return a copy so the caller cannot corrupt the cached data.
		return copyBytes(data), objType, nil
	}
	return data, objType, nil
}

// findPackedObject locates an object in any mapped packfile.
// The method consults the in-memory merged index first, then individual packs.
//
// findPackedObject returns the pack handle, byte offset, and true if found.
func (s *store) findPackedObject(oid Hash) (*mmap.ReaderAt, uint64, bool) {
	if s.memoryMidx != nil {
		if p, off, ok := s.memoryMidx.findObject(oid); ok {
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
		full, typ, err := s.readLooseObject(oid)
		if err != nil {
			return nil, err
		}
		if typ != ObjCommit {
			return nil, fmt.Errorf("%w: %x", ErrObjectNotCommit, oid)
		}
		return trimCommitHeader(full)
	}
	typ, hdrLen, err := peekObjectType(p, off)
	if err != nil {
		return nil, err
	}
	if typ == ObjCommit {
		off += uint64(hdrLen)

		// Decompress only the beginning of the object, typically less than 512 bytes.
		zr, err := getZlibReader(io.NewSectionReader(p, int64(off), 1<<63-1))
		if err != nil {
			return nil, err
		}
		defer putZlibReader(zr)

		return readCommitHeaderFromStream(zr)
	}

	if typ != ObjOfsDelta && typ != ObjRefDelta {
		return nil, fmt.Errorf("%w: %x", ErrObjectNotCommit, oid)
	}

	full, resolvedType, err := s.getMaterialized(oid)
	if err != nil {
		return nil, err
	}
	if resolvedType != ObjCommit {
		return nil, fmt.Errorf("%w: %x", ErrObjectNotCommit, oid)
	}
	return trimCommitHeader(full)
}

// readCommitHeaderFromStream incrementally reads lines from a zlib stream
// until it encounters the "committer" line, then returns all bytes read up
// to and including that line.
//
// This allows the caller to obtain tree, parent, author, and committer
// metadata without decompressing the entire commit object (which may include
// a large message body). The returned slice is a heap-allocated copy that
// does not alias the pooled buffer.
//
// The zlib reader is wrapped in a pooled bufio.Reader so that reads go
// through an 8 KiB buffer instead of issuing one-byte reads against the
// decompressor. ReadBytes('\n') replaces the former readUntilLF loop,
// cutting per-line allocations from ~6 append-growths to 1.
func readCommitHeaderFromStream(r io.Reader) ([]byte, error) {
	br := getBR(r)
	defer putBR(br)

	buf := GetBuf()
	defer PutBuf(buf)

	for len(*buf) < MaxHdr {
		line, err := br.ReadBytes('\n')
		*buf = append(*buf, line...)
		if bytes.HasPrefix(line, []byte("committer ")) {
			return append([]byte(nil), *buf...), nil
		}
		if err != nil {
			return nil, err
		}
	}

	return nil, fmt.Errorf("commit header exceeds %d bytes without committer line", MaxHdr)
}

// trimCommitHeader extracts the header portion of a fully materialized commit
// object (up to and including the "committer" line). Unlike
// readCommitHeaderFromStream, this operates on an already-inflated byte slice
// rather than a streaming reader. The returned slice is a fresh copy.
func trimCommitHeader(full []byte) ([]byte, error) {
	end := 0
	for end < len(full) && end < MaxHdr {
		nl := bytes.IndexByte(full[end:], '\n')
		if nl < 0 {
			break
		}
		lineEnd := end + nl + 1
		line := full[end:lineEnd]
		if bytes.HasPrefix(line, []byte("committer ")) {
			out := make([]byte, lineEnd)
			copy(out, full[:lineEnd])
			return out, nil
		}
		end = lineEnd
	}

	return nil, fmt.Errorf("commit header exceeds %d bytes without committer line", MaxHdr)
}

// findCRCForObject returns the CRC-32 checksum for the specified object.
// The method searches pack index files first, then falls back to the in-memory merged index.
//
// The second return value indicates whether a CRC was found.
// findCRCForObject is read-only and safe for concurrent use.
func (s *store) findCRCForObject(p *mmap.ReaderAt, off uint64, oid Hash) (uint32, bool) {
	for _, pf := range s.packs {
		if pf.pack == p {
			if crc, ok := pf.crcAtOffset(off); ok {
				return crc, true
			}
		}
	}

	if s.memoryMidx != nil {
		if entry, ok := s.memoryMidx.findEntry(oid); ok &&
			entry.pack == p &&
			entry.offset == off &&
			entry.crc != 0 {
			return entry.crc, true
		}
	}

	return 0, false
}

// verifyCRCForPackObject validates the CRC-32 checksum of a pack object.
// verifyCRCForPackObject returns an error if the pack cannot be located or checksum verification fails.
func (s *store) verifyCRCForPackObject(p *mmap.ReaderAt, off uint64, crc uint32) error {
	for _, pf := range s.packs {
		if pf.pack == p {
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
