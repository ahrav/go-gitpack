// delta.go
//
// Delta compression resolution for Git packfile objects.
// The module handles *delta headers* → *base object references* + *delta operations*
// and applies copy/insert instruction streams to reconstruct full objects from
// compressed deltas. This enables space‑efficient storage by storing objects as
// differences from base objects rather than complete content.
//
// The implementation supports both ref‑delta (references by object hash) and
// ofs‑delta (references by pack offset) formats with cycle detection and depth
// limiting. Delta operations are applied using optimized memory operations with
// pre‑allocated output buffers sized according to the encoded target length.

package objstore

import (
	"fmt"
	"slices"
	"sync"
	"unsafe"
)

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

var (
	// Pool size classes based on typical Git object distributions.

	// Small: Most source code files (< 16KB).
	smallPool = sync.Pool{
		New: func() any {
			buf := make([]byte, 0, 16*1024) // 16KB
			return &buf
		},
	}

	// Medium: Larger source files, docs, small assets (< 256KB).
	mediumPool = sync.Pool{
		New: func() any {
			buf := make([]byte, 0, 256*1024) // 256KB
			return &buf
		},
	}

	// Large: Images, binaries, build artifacts (< 2MB).
	largePool = sync.Pool{
		New: func() any {
			buf := make([]byte, 0, 2*1024*1024) // 2MB
			return &buf
		},
	}

	// XLarge: Very large files (videos, archives).
	xlargePool = sync.Pool{
		New: func() any {
			buf := make([]byte, 0, 16*1024*1024) // 16MB
			return &buf
		},
	}
)

// Size thresholds for pool selection.
const (
	smallThreshold  = 8 * 1024        // 8 KiB – direct alloc below this
	mediumThreshold = 128 * 1024      // 128 KiB
	largeThreshold  = 1024 * 1024     // 1 MiB
	xlargeThreshold = 8 * 1024 * 1024 // 8 MiB
)

// applyDelta interprets a Git delta stream and returns the fully
// reconstructed object.
//
// The function first reads the source and target sizes (two Git varints)
// and then executes the delta instruction stream consisting of copy and
// insert op-codes.
//
// Fast-path buffer management:
//
//   - For very small targets (< 8 KiB) it allocates directly on the heap to
//     avoid the overhead of a sync.Pool.
//
//   - For larger targets it chooses one of several size-class pools to
//     amortize allocations across multiple delta applications.
//
// The returned slice is a fresh clone; callers may mutate it freely.
// A nil slice signals malformed input (e.g., invalid op-codes, bounds
// violations, overflow in varint parsing).
func applyDelta(base, delta []byte) []byte {
	if len(delta) == 0 {
		return nil
	}

	// The first two varints encode source & target lengths.
	_, n1 := decodeVarInt(delta)
	if n1 <= 0 || n1 >= len(delta) {
		return nil
	}
	targetSize, n2 := decodeVarInt(delta[n1:])
	if n2 <= 0 || n1+n2 >= len(delta) {
		return nil
	}

	var buf *[]byte
	var pool *sync.Pool

	// Select the appropriate pool (or direct allocation) based on targetSize.
	switch {
	case targetSize <= smallThreshold:
		// Tiny objects: a direct allocation is cheaper than sync.Pool traffic.
		b := make([]byte, targetSize)
		buf = &b
		pool = nil

	case targetSize <= mediumThreshold:
		pool = &smallPool
		buf = pool.Get().(*[]byte)

	case targetSize <= largeThreshold:
		pool = &mediumPool
		buf = pool.Get().(*[]byte)

	case targetSize <= xlargeThreshold:
		pool = &largePool
		buf = pool.Get().(*[]byte)

	default:
		// Very large blobs (≥ 8 MiB) still benefit from pooling to curb GC churn.
		pool = &xlargePool
		buf = pool.Get().(*[]byte)
	}

	// Ensure the buffer is large enough; if not, grow it
	// but keep the (now larger) slice in the pool for future reuse.
	if pool != nil && cap(*buf) < int(targetSize) {
		*buf = make([]byte, targetSize)
	}

	out := (*buf)[:targetSize]

	deltaLen := len(delta)
	baseLen := len(base)
	opIdx := n1 + n2 // Start of the op-code stream.
	outIdx := 0

	for opIdx < deltaLen {
		op := *(*byte)(unsafe.Add(unsafe.Pointer(&delta[0]), opIdx))
		opIdx++

		if op&0x80 != 0 { // -------- Copy operation --------
			var cpOff, cpLen uint32

			// Offsets and lengths are stored bit-sliced in the op byte.
			if op&0x01 != 0 {
				cpOff = uint32(delta[opIdx])
				opIdx++
			}
			if op&0x02 != 0 {
				cpOff |= uint32(delta[opIdx]) << 8
				opIdx++
			}
			if op&0x04 != 0 {
				cpOff |= uint32(delta[opIdx]) << 16
				opIdx++
			}
			if op&0x08 != 0 {
				cpOff |= uint32(delta[opIdx]) << 24
				opIdx++
			}

			if op&0x10 != 0 {
				cpLen = uint32(delta[opIdx])
				opIdx++
			}
			if op&0x20 != 0 {
				cpLen |= uint32(delta[opIdx]) << 8
				opIdx++
			}
			if op&0x40 != 0 {
				cpLen |= uint32(delta[opIdx]) << 16
				opIdx++
			}
			if cpLen == 0 {
				cpLen = 65536
			}

			if int(cpOff)+int(cpLen) > baseLen || outIdx+int(cpLen) > int(targetSize) {
				if pool != nil {
					pool.Put(buf)
				}
				return nil
			}
			copy(out[outIdx:], base[cpOff:cpOff+cpLen])
			outIdx += int(cpLen)

		} else if op != 0 { // ------- Insert operation -------
			insertLen := int(op)
			if opIdx+insertLen > deltaLen || outIdx+insertLen > int(targetSize) {
				if pool != nil {
					pool.Put(buf)
				}
				return nil
			}
			copy(out[outIdx:], delta[opIdx:opIdx+insertLen])
			opIdx += insertLen
			outIdx += insertLen

		} else { // op == 0
			if pool != nil {
				pool.Put(buf)
			}
			return nil // invalid op-code
		}
	}

	// Hand a fresh slice to the caller.
	result := slices.Clone(out)
	if pool != nil {
		pool.Put(buf)
	}
	return result
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
