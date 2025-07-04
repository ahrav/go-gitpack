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
	defaultMaxDeltaDepth = 50
	// maxCacheableSize is the maximum size of an object that will be stored in the cache.
	maxCacheableSize = 4 << 20 // 4 MiB
)

// cachedObj represents a Git object stored in the cache along with its type.
// The struct pairs fully-materialized object data with its ObjectType to avoid
// redundant type detection on cache hits.
// Instances are immutable once placed in the cache.
type cachedObj struct {
	// data holds the complete, inflated object contents.
	// The slice must not be mutated once cached.
	data []byte

	// typ specifies the Git object type (blob, tree, commit, or tag).
	// This avoids calling detectType on every cache hit.
	typ ObjectType
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
	cache *arc.ARCCache[Hash, cachedObj]

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
		store.cache, err = arc.NewARC[Hash, cachedObj](defaultCacheSize)
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
	store.cache, err = arc.NewARC[Hash, cachedObj](defaultCacheSize)
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
		return nil, err
	}
	if typ != ObjTree {
		return nil, ErrTypeMismatch
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

// parseObjectHeaderUnsafe parses a Git object header using unsafe pointer operations.
// The header encodes object type in bits 6-4 of the first byte and size as a
// variable-length integer.
//
// parseObjectHeaderUnsafe returns the object type, decompressed size, and number of header bytes consumed.
// The function uses unsafe operations for performance but never panics.
func parseObjectHeaderUnsafe(data []byte) (ObjectType, uint64, int) {
	if len(data) == 0 {
		return ObjBad, 0, -1
	}

	// The first byte encodes the object type and the four least significant bits of the size.
	b0 := *(*byte)(unsafe.Pointer(&data[0]))
	objType := ObjectType((b0 >> 4) & 7)
	size := uint64(b0 & 0x0f)

	if b0&0x80 == 0 {
		return objType, size, 1
	}

	// Handle common 2- and 3-byte headers efficiently.
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

	// Fallback to a loop for headers longer than three bytes.
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

// peekObjectType reads the object header to determine type without inflating the body.
// The function returns the object type and header length in bytes.
//
// This optimization allows skipping full decompression for objects that
// can be served from other sources like the commit-graph.
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

// inflateFromPack reads and materializes an object from a packfile.
// The method handles both regular objects and delta-encoded objects, resolving
// delta chains as needed.
//
// For delta objects, inflateFromPack recursively resolves the chain up to maxDeltaDepth.
// When VerifyCRC is true, the method validates object integrity using checksums.
//
// inflateFromPack returns the inflated object data, its type, and any error encountered.
// The returned data is a fresh allocation safe for modification.
func (s *store) inflateFromPack(
	p *mmap.ReaderAt,
	off uint64,
	oid Hash,
	ctx *deltaContext,
) ([]byte, ObjectType, error) {
	// Perform a cheap header peek to avoid full inflation for commits
	// that are already covered by the commit-graph.
	objType, _, err := peekObjectType(p, off)
	if err != nil {
		return nil, ObjBad, err
	}
	if objType == ObjCommit {
		return nil, ObjCommit, nil
	}

	// For delta objects, resolve the entire chain iteratively.
	if objType == ObjOfsDelta || objType == ObjRefDelta {
		full, baseType, err := s.inflateDeltaChainStreaming(p, off, oid, ctx)
		if err != nil {
			return nil, ObjBad, err
		}
		// Add the fully resolved object to both the delta window and the ARC cache.
		if len(full) <= maxCacheableSize {
			s.dw.add(oid, full, baseType)
			s.cache.Add(oid, cachedObj{data: full, typ: baseType})
		}
		return full, baseType, nil
	}

	// For regular (non-delta) objects, inflate once and cache the result.
	_, data, err := readRawObject(p, off)
	if err != nil {
		return nil, ObjBad, err
	}

	// If enabled, perform a CRC-32 integrity check.
	if s.VerifyCRC {
		if crc, ok := s.findCRCForObject(p, off, oid); ok {
			if err := s.verifyCRCForPackObject(p, off, crc); err != nil {
				return nil, ObjBad, err
			}
		} else if s.midx != nil && s.midx.version >= 2 {
			return nil, ObjBad, fmt.Errorf("no CRC found for object %x in MIDX v2", oid)
		}
	}

	if len(data) <= maxCacheableSize {
		s.dw.add(oid, data, objType)
		s.cache.Add(oid, cachedObj{data: data, typ: objType})
	}
	return data, objType, nil
}

// applyDeltaStreaming applies delta instructions to reconstruct an object.
// The method uses pre-allocated buffers and streaming decompression to minimize memory usage.
//
// The output buffer must be pre-allocated with sufficient capacity for the
// target object size.
// applyDeltaStreaming returns an error if the delta instructions are malformed or reference
// data outside the base object bounds.
func (s *store) applyDeltaStreaming(
	pack *mmap.ReaderAt,
	offset uint64,
	deltaType ObjectType,
	base []byte,
	out []byte, // Pre-allocated output buffer
) ([]byte, error) {
	// Skip the object header to get to the delta instructions.
	_, hdrLen, err := peekObjectType(pack, offset)
	if err != nil {
		return nil, err
	}
	pos := int64(offset) + int64(hdrLen)

	// Skip the base object reference (hash or offset).
	switch deltaType {
	case ObjRefDelta:
		pos += 20
	case ObjOfsDelta:
		// Skip the variable-length offset for ofs-delta.
		for {
			var b [1]byte
			if _, err := pack.ReadAt(b[:], pos); err != nil {
				return nil, err
			}
			pos++
			if b[0]&0x80 == 0 {
				break
			}
		}
	}

	// Open a zlib stream to decompress the delta instructions.
	src := io.NewSectionReader(pack, pos, 1<<63-1)
	zr, err := getZlibReader(src)
	if err != nil {
		return nil, err
	}
	defer putZlibReader(zr)

	// Use a pooled buffered reader for efficient byte-level operations.
	br := getBR(zr)
	defer putBR(br)

	// Read the delta header, which specifies the base and target object sizes.
	baseSize, _, err := readVarIntFromReader(br)
	if err != nil || baseSize != uint64(len(base)) {
		return nil, errors.New("delta base size mismatch")
	}

	targetSize, _, err := readVarIntFromReader(br)
	if err != nil {
		return nil, errors.New("invalid delta target size")
	}

	// Verify that the output buffer is large enough for the target object.
	if uint64(cap(out)) < targetSize {
		return nil, fmt.Errorf("output buffer too small: need %d, have %d", targetSize, cap(out))
	}

	// Initialize a zero-length slice to ensure no undefined content is present.
	out = out[:0]

	// Process the delta instructions, copying data from the base or inserting new data.
	for uint64(len(out)) < targetSize {
		cmd, err := br.ReadByte()
		if err != nil {
			if err == io.EOF {
				break // A clean EOF is acceptable if the target size has been reached.
			}
			return nil, err
		}

		if cmd&0x80 != 0 {
			// This is a copy command; data will be copied from the base object.
			offset, size, err := decodeCopyCommand(cmd, br)
			if err != nil {
				return nil, err
			}

			if offset+size > len(base) {
				return nil, errors.New("copy beyond base bounds")
			}

			// Extend the output slice and copy the specified segment from the base.
			outPos := len(out)
			out = out[:outPos+size]
			copy(out[outPos:], base[offset:offset+size])
		} else if cmd > 0 {
			// This is an insert command; new data will be read from the delta.
			size := int(cmd)

			// Extend the output slice and read the new data directly into it.
			outPos := len(out)
			out = out[:outPos+size]
			if _, err := io.ReadFull(br, out[outPos:]); err != nil {
				return nil, err
			}
		} else {
			return nil, errors.New("invalid delta command")
		}
	}

	if uint64(len(out)) != targetSize {
		return nil, fmt.Errorf("delta size mismatch: got %d, want %d", len(out), targetSize)
	}

	return out, nil
}

// inflateDeltaChainStreaming resolves a delta chain efficiently using streaming decompression.
// The method walks the chain to find the base object, then applies deltas using
// ping-pong buffers to minimize memory allocation.
//
// inflateDeltaChainStreaming returns the fully reconstructed object and its base type.
func (s *store) inflateDeltaChainStreaming(
	p *mmap.ReaderAt,
	off uint64,
	oid Hash,
	ctx *deltaContext,
) ([]byte, ObjectType, error) {
	var (
		baseData   []byte
		baseType   ObjectType
		deltaStack deltaStack
		err        error
	)

	// Walk up the delta chain to find the non-delta base object.
	currPack, currOff := p, off
	for depth := 0; depth < ctx.maxDepth; depth++ {
		var typ ObjectType
		typ, _, err = peekObjectType(currPack, currOff)
		if err != nil {
			return nil, ObjBad, err
		}

		if typ != ObjOfsDelta && typ != ObjRefDelta {
			// The base object is not a delta, so inflate it directly.
			_, baseData, err = readRawObject(currPack, currOff)
			if err != nil {
				return nil, ObjBad, err
			}
			baseType = typ
			break
		}

		// Push information about the current delta onto the stack for later application.
		deltaStack = append(deltaStack, deltaInfo{
			pack:   currPack,
			offset: currOff,
			typ:    typ,
		})

		// Determine the location of the next object in the delta chain.
		if typ == ObjRefDelta {
			var baseOid Hash
			_, _, baseOid, err = readRefDeltaHeader(currPack, currOff)
			if err != nil {
				return nil, ObjBad, err
			}
			nextPack, nextOff, ok := s.findPackedObject(baseOid)
			if !ok {
				return nil, ObjBad, fmt.Errorf("base object %s not found", baseOid)
			}
			currPack = nextPack
			currOff = nextOff
		} else { // ObjOfsDelta
			var baseOff uint64
			_, _, baseOff, err = readOfsDeltaHeader(currPack, currOff)
			if err != nil {
				return nil, ObjBad, err
			}
			currOff = currOff - baseOff
		}
	}

	if baseData == nil {
		return nil, ObjBad, errors.New("delta chain base not found")
	}

	// If there are no deltas, the base object is the final result.
	if len(deltaStack) == 0 {
		return baseData, baseType, nil
	}

	// Use a memory arena for ping-pong buffering to reduce allocations.
	arena := getDeltaArena()
	defer putDeltaArena(arena)

	// Calculate the exact maximum target size required across all deltas.
	maxTargetSize := s.peekLargestTarget(deltaStack)
	baseSize := uint64(len(baseData))
	if baseSize > maxTargetSize {
		maxTargetSize = baseSize
	}

	// Ensure the arena is large enough to hold the largest object; resize if necessary.
	arenaSize := uint64(len(arena.data) / 2)
	if maxTargetSize > arenaSize {
		arena.data = make([]byte, maxTargetSize*2)
	}

	// Split the arena into two buffers for ping-pong application of deltas.
	needed := maxTargetSize
	buf1 := arena.data[:needed]
	buf2 := arena.data[needed : needed*2]

	// Initialize the first buffer with the base object data.
	in := buf1[:len(baseData)]
	copy(in, baseData)
	out := buf2

	// Apply deltas in reverse order using ping-pong buffering.
	for i := len(deltaStack) - 1; i >= 0; i-- {
		delta := deltaStack[i]

		// Apply the current delta, transforming the 'in' buffer to the 'out' buffer.
		newResult, err := s.applyDeltaStreaming(
			delta.pack,
			delta.offset,
			delta.typ,
			in,
			out,
		)
		if err != nil {
			return nil, ObjBad, err
		}

		// Swap the roles of the buffers for the next iteration.
		in, out = newResult, in[:0]
	}

	// The final reconstructed object is in the 'in' buffer; copy it to a new slice to return.
	result := make([]byte, len(in))
	copy(result, in)
	return result, baseType, nil
}

// readRefDeltaHeader reads the header of a ref-delta object.
// The method returns the object type, header length, and base object hash.
func readRefDeltaHeader(p *mmap.ReaderAt, off uint64) (ObjectType, int, Hash, error) {
	typ, hdrLen, err := peekObjectType(p, off)
	if err != nil {
		return ObjBad, 0, Hash{}, err
	}

	var baseOid Hash
	pos := int64(off) + int64(hdrLen)
	if _, err := p.ReadAt(baseOid[:], pos); err != nil {
		return ObjBad, 0, Hash{}, err
	}
	return typ, hdrLen, baseOid, nil
}

// readOfsDeltaHeader reads the header of an ofs-delta object.
// The method returns the object type, header length, and negative offset to base.
func readOfsDeltaHeader(p *mmap.ReaderAt, off uint64) (ObjectType, int, uint64, error) {
	typ, hdrLen, err := peekObjectType(p, off)
	if err != nil {
		return ObjBad, 0, 0, err
	}

	pos := int64(off) + int64(hdrLen)
	offsetValue := readOfsDeltaOffset(p, pos)

	return typ, hdrLen, offsetValue, nil
}

// readOfsDeltaOffset reads a variable-length offset from an ofs-delta object.
// The offset encoding uses the MSB as a continuation bit.
func readOfsDeltaOffset(pack *mmap.ReaderAt, pos int64) uint64 {
	var offset uint64
	var b [1]byte

	// Read the variable-length negative offset from the pack data.
	pack.ReadAt(b[:], pos)
	offset = uint64(b[0] & 0x7f)
	for b[0]&0x80 != 0 {
		pos++
		offset++
		pack.ReadAt(b[:], pos)
		offset = (offset << 7) | uint64(b[0]&0x7f)
	}

	return offset
}

// readRawObject inflates a Git object from a packfile.
// For delta objects, the function returns the delta prefix (base reference) prepended
// to the inflated delta instructions.
//
// The prefix is:
// - 20-byte SHA-1 for ref-delta objects.
// - Variable-length offset for ofs-delta objects.
//
// Regular objects return just the inflated content without any prefix.
func readRawObject(r *mmap.ReaderAt, off uint64) (ObjectType, []byte, error) {
	// Parse the generic variable-length object header.
	var hdr [32]byte
	n, err := r.ReadAt(hdr[:], int64(off))
	if err != nil && !errors.Is(err, io.EOF) {
		return ObjBad, nil, err
	}
	if n == 0 {
		return ObjBad, nil, errors.New("empty object")
	}

	objType, size, hdrLen := parseObjectHeaderUnsafe(hdr[:n])
	if hdrLen <= 0 {
		return ObjBad, nil, errors.New("cannot parse object header")
	}

	pos := int64(off) + int64(hdrLen) // Position of the first byte after the generic header.

	// For delta objects, read and store the prefix, which contains the base reference.
	var prefix []byte
	switch objType {
	case ObjRefDelta:
		prefix = make([]byte, 20)
		if _, err := r.ReadAt(prefix, pos); err != nil {
			return ObjBad, nil, err
		}
		pos += 20

	case ObjOfsDelta:
		// The offset is a variable-length negative value.
		for {
			var b [1]byte
			if _, err := r.ReadAt(b[:], pos+int64(len(prefix))); err != nil {
				return ObjBad, nil, err
			}
			prefix = append(prefix, b[0])
			if b[0]&0x80 == 0 { // The MSB is clear, indicating the last byte of the offset.
				pos += int64(len(prefix))
				break
			}
			if len(prefix) > 12 { // A sanity check to prevent parsing excessively long offsets.
				return ObjBad, nil, errors.New("ofs‑delta base‑ref too long")
			}
		}
	}

	// Inflate the zlib-compressed data stream that follows the header and prefix.

	// The compressed length is unknown, so provide SectionReader
	// with a virtually infinite length; it will stop at EOF.
	src := io.NewSectionReader(r, pos, 1<<63-1)
	zr, err := zlib.NewReader(src)
	if err != nil {
		return ObjBad, nil, err
	}
	defer zr.Close()

	out := make([]byte, size)
	if _, err := io.ReadFull(zr, out); err != nil {
		return ObjBad, nil, err
	}

	// For deltas, return the concatenated prefix and inflated data; otherwise, return just the inflated data.
	if len(prefix) != 0 {
		return objType, append(prefix, out...), nil
	}
	return objType, out, nil
}

// peekLargestTarget calculates the maximum target size needed for delta application.
// This allows pre-allocating buffers of the correct size to avoid reallocation
// during delta chain resolution.
func (s *store) peekLargestTarget(deltaStack deltaStack) uint64 {
	maxSize := uint64(0)

	for _, delta := range deltaStack {
		// Peek at the delta header to determine the size of the resulting object.
		targetSize, err := s.peekDeltaTargetSize(delta.pack, delta.offset, delta.typ)
		if err != nil {
			// If peeking fails, fall back to a conservative, large buffer size.
			return uint64(16 << 20) // 16MB fallback
		}
		if targetSize > maxSize {
			maxSize = targetSize
		}
	}

	return maxSize
}

// peekDeltaTargetSize reads the target size from a delta header without full decompression.
// This optimization allows proper buffer sizing before applying deltas.
func (s *store) peekDeltaTargetSize(
	pack *mmap.ReaderAt,
	offset uint64,
	deltaType ObjectType,
) (uint64, error) {
	// Skip to delta instructions.
	_, hdrLen, err := peekObjectType(pack, offset)
	if err != nil {
		return 0, err
	}
	pos := int64(offset) + int64(hdrLen)

	// Skip the delta reference.
	switch deltaType {
	case ObjRefDelta:
		pos += 20
	case ObjOfsDelta:
		// Skip the variable offset.
		for {
			var b [1]byte
			if _, err := pack.ReadAt(b[:], pos); err != nil {
				return 0, err
			}
			pos++
			if b[0]&0x80 == 0 {
				break
			}
		}
	}

	// Open a zlib stream just to read the header.
	src := io.NewSectionReader(pack, pos, 1<<63-1)
	zr, err := getZlibReader(src)
	if err != nil {
		return 0, err
	}
	defer putZlibReader(zr)

	// Get a pooled buffered reader.
	br := getBR(zr)
	defer putBR(br)

	// Skip the base size.
	_, _, err = readVarIntFromReader(br)
	if err != nil {
		return 0, err
	}

	// Read the target size.
	targetSize, _, err := readVarIntFromReader(br)
	if err != nil {
		return 0, err
	}

	return targetSize, nil
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
		return nil, fmt.Errorf("object %x not found", oid)
	}
	typ, hdrLen, err := peekObjectType(p, off)
	if err != nil {
		return nil, err
	}
	if typ != ObjCommit {
		return nil, fmt.Errorf("%x is not a commit", oid)
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
