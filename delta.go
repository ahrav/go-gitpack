package objstore

import (
	"fmt"
	"unsafe"
)

// copyMemory is a fast memory copy using memmove
//
//go:linkname copyMemory runtime.memmove
//go:noescape
func copyMemory(to, from unsafe.Pointer, n int)

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
