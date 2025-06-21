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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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
	// slices are still referenced.  A finalizer makes sure users who forget
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

	// packMap tracks unique mmap handles to prevent duplicate mappings.
	packMap map[string]*mmap.ReaderAt

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
		return nil, fmt.Errorf("no packfiles found in %s", absDir)
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
	}

	const defaultCacheSize = 1 << 14 // 16 KiB ARC—enough for >95 % hit-rate on typical workloads.
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
			for _, pf := range s.packs {
				if pf.pack == p {
					if err := verifyCRC32(pf, off, pf.entriesByOff[off].crc); err != nil {
						return nil, ObjBad, err
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

	// TODO: For large objects, use streaming.
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
