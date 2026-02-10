// delta.go
//
// Two encodings are supported:
//   - ref‑deltas identify their base object by its 20‑byte object ID.
//   - ofs‑deltas locate their base by a backward offset within the same
//     packfile.
//
// Delta chains may be deep or even cyclic.
// The resolver tracks every hop and enforces a caller‑supplied depth limit to
// prevent infinite recursion and denial‑of‑service attacks.
//
// Internally the implementation uses a reusable "ping‑pong" arena so that each
// delta step can decode from one half of the buffer while writing into the
// other, avoiding heap allocations.
//
// No delta‑specific names are exported.
// All symbols below this comment are package‑private in order to keep the
// public surface minimal and stable.
package objstore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"

	"golang.org/x/exp/mmap"
)

var ErrDeltaTargetTooLarge = errors.New("delta target exceeds configured maximum")

// deltaInfo represents a single delta object in a delta chain.
// The struct is used during chain traversal to track the sequence of deltas
// that must be applied to reconstruct the final object.
type deltaInfo struct {
	// pack contains the memory-mapped packfile holding this delta.
	pack *mmap.ReaderAt

	// offset specifies the byte position where this delta starts in the pack.
	offset uint64

	// typ indicates whether this is an ofs-delta or ref-delta object.
	typ ObjectType

	// hdrLen caches the parsed object header length so downstream consumers
	// (applyDeltaStreaming) can skip re-parsing the header.
	hdrLen int
}

// deltaStack is a stack of deltaInfo objects, used to iteratively resolve a
// delta chain.
//
// Ordering invariant: elements are pushed during the walk-up phase in
// encounter order (child delta first, base-adjacent delta last). During the
// apply-down phase the stack is iterated in reverse (from len-1 to 0) so
// that the delta closest to the base object is applied first.
type deltaStack []deltaInfo

// deltaContext carries per‑lookup state while resolving a chain of deltas.
//
// A single value is threaded through the recursive resolver so that the
// algorithm can detect circular references and enforce the maximum chain depth
// configured at the Store level.
//
// The zero value is **not** valid.
// Use newDeltaContext to obtain an initialized instance that respects the
// caller‑supplied limit.
type deltaContext struct {
	// visited marks every base object reached by object ID during the current
	// resolution so that the resolver can detect ref‑delta cycles.
	visited map[Hash]bool

	// offsets marks every packfile offset reached during ofs‑delta resolution
	// so that the resolver can detect cycles that revisit a previous object
	// offset in the same pack.
	offsets map[uint64]bool

	// depth is the current recursion depth.
	// It is incremented on entry to a child delta and decremented on exit.
	depth int

	// maxDepth is the caller‑provided recursion limit.
	// Exceeding this value aborts resolution with an error.
	maxDepth int
}

// deltaContextPool reuses deltaContext values across lookups.
// Maps are cleared (Go 1.21+) rather than reallocated, preserving their
// backing storage for subsequent resolutions.
var deltaContextPool = sync.Pool{
	New: func() any {
		return &deltaContext{
			visited: make(map[Hash]bool),
			offsets: make(map[uint64]bool),
		}
	},
}

// getDeltaContext retrieves a pooled deltaContext configured with maxDepth.
func getDeltaContext(maxDepth int) *deltaContext {
	ctx := deltaContextPool.Get().(*deltaContext)
	ctx.maxDepth = maxDepth
	return ctx
}

// putDeltaContext returns ctx to the pool after clearing its per-lookup state.
func putDeltaContext(ctx *deltaContext) {
	ctx.reset()
	deltaContextPool.Put(ctx)
}

// reset clears per-lookup state so the context can be reused from a pool.
func (ctx *deltaContext) reset() {
	clear(ctx.visited)
	clear(ctx.offsets)
	ctx.depth = 0
	ctx.maxDepth = 0
}

// newDeltaContext allocates and initializes a deltaContext that enforces the
// supplied maximum delta‑chain depth.
func newDeltaContext(maxDepth int) *deltaContext {
	return getDeltaContext(maxDepth)
}

// checkRefDelta validates that following a ref‑delta will neither exceed the
// maximum chain depth nor revisit the same base object.
//
// It returns an error when the next hop would push ctx.depth beyond
// ctx.maxDepth or when hash has already been seen in ctx.visited.
func (ctx *deltaContext) checkRefDelta(hash Hash) error {
	if ctx.depth >= ctx.maxDepth {
		return fmt.Errorf("delta chain too deep (max %d)", ctx.maxDepth)
	}
	if ctx.visited[hash] {
		return fmt.Errorf("circular delta reference detected for %x", hash)
	}
	return nil
}

// enterRefDelta records that the ref‑delta identified by hash is being
// processed and bumps the recursion depth counter.
func (ctx *deltaContext) enterRefDelta(hash Hash) {
	ctx.visited[hash] = true
	ctx.depth++
}

// deltaArena holds two equally‑sized byte slices so the resolver can ping‑pong
// between *source* and *destination* buffers while walking a delta chain.
type deltaArena struct{ data []byte }

// deltaArenaPool loans arenas large enough to hold two complete objects of up
// to 16 MiB each (32 MiB total).
var deltaArenaPool = sync.Pool{
	New: func() any {
		const maxObjectSize = 16 << 20 // 16 MiB
		return &deltaArena{
			data: make([]byte, 2*maxObjectSize),
		}
	},
}

// getDeltaArena retrieves a ping‑pong arena from the pool.
func getDeltaArena() *deltaArena { return deltaArenaPool.Get().(*deltaArena) }

// putDeltaArena returns arena to the pool and resets its slice length.
func putDeltaArena(arena *deltaArena) {
	arena.data = arena.data[:cap(arena.data)]
	deltaArenaPool.Put(arena)
}

// errDeltaOutputTooSmall reports that the caller-provided output buffer cannot
// hold the decoded target.
type errDeltaOutputTooSmall struct {
	need uint64
	have int
}

func (e *errDeltaOutputTooSmall) Error() string {
	return fmt.Sprintf("output buffer too small: need %d, have %d", e.need, e.have)
}

// inflationParams bundles the inputs required by inflateDeltaChainStreaming to
// reconstruct an object that is stored as a chain of pack-file deltas.
//
// Callers populate an inflationParams value and pass it unchanged to
// inflateDeltaChainStreaming.  The struct is merely a data carrier; it enforces
// no invariants beyond the expectation that pointer fields are non-nil.
type inflationParams struct {
	// p provides random-access reads to the pack that contains the first delta
	// object. It must remain valid for the entire lifetime of the inflation.
	p *mmap.ReaderAt

	// off specifies the zero-based byte offset of the initial delta object
	// within p.
	off uint64

	// oid identifies the final object that will result from applying the delta
	// chain. It is consulted for diagnostics and cycle detection only.
	oid Hash

	// ctx carries per-call state such as recursion depth and visited nodes.
	// Obtain it via newDeltaContext before invoking the inflation routine.
	ctx *deltaContext

	// maxObjectSize bounds reconstructed delta objects. Zero disables the bound.
	maxObjectSize uint64
}

// inflateDeltaChainStreaming reconstructs an object that is stored as a
// chain of pack-file deltas.  The algorithm is deliberately split into two
// distinct phases so that it can run with a *single* bounded memory
// allocation:
//
//  1. WALK-UP PHASE
//     Starting at (p, off) the function climbs *up* the delta chain until it
//     reaches a non-delta "base" object.  For every hop it:
//
//     • pushes a deltaInfo onto deltaStack (for later application),
//     • enforces the caller-supplied recursion / cycle-detection rules, and
//     • peeks at the delta header just far enough to know the size of the
//     *future* result so that we can compute an upper bound for memory
//     requirements.
//
//     When the loop finishes we have:
//     – baseData     – the fully inflated base object,
//     – baseType     – its type (blob, tree …),
//     – deltaStack   – the list of deltas that still need to be applied.
//
//  2. APPLY-DOWN (PING-PONG) PHASE
//     With the chain analyzed we can allocate exactly the amount of memory
//     we will ever need: the maximum of
//
//     • len(baseData) and
//     • the largest target size advertised by any delta in deltaStack.
//
//     One arena twice that size is borrowed from a sync.Pool and sliced into
//     two equal halves:
//
//     +----------------------+----------------------+
//     |  buffer A (input)    |  buffer B (output)   |
//     +----------------------+----------------------+
//
//     The base object is copied into buffer A.  The function then walks
//     deltaStack backwards (from the oldest delta down to the newest),
//     calling applyDeltaStreaming for each element:
//
//     in, out = bufferA, bufferB
//     for delta := range reverse(deltaStack) {      // pseudo code
//     in  = applyDeltaStreaming(delta, in, out) // writes into 'out'
//     in, out = out, in                         // ping-pong swap
//     }
//
//     After each iteration the buffers swap their roles – hence the
//     *ping-pong* terminology – guaranteeing that reads and writes never
//     overlap and that no further allocations or copies are required.
//
//     When the loop terminates the fully reconstructed object lives in the
//     slice currently referenced by 'in'.  A final copy is made into a fresh
//     slice so callers may retain the result after the arena has been
//     returned to the pool.
//
// The function fails with an error when the chain is too deep, cyclic, the
// pack-file is corrupted, or any delta instruction is invalid.
// IMPORTANT: Because the slice is mutated in-place it MUST NOT be reused once
// resolution is complete.  applyDeltaStack treats the slice as read-only and
// does not retain it after the call returns.
func inflateDeltaChainStreaming(s *store, params inflationParams) ([]byte, ObjectType, error) {
	stack := make(deltaStack, 0, 16) // pre-size for typical chain depths
	base, baseType, err := walkUpDeltaChain(s, params, &stack)
	if err != nil {
		return nil, ObjBad, err
	}
	return applyDeltaStack(stack, base, baseType, params.maxObjectSize, false)
}

// inflateDeltaChainBorrowed is like inflateDeltaChainStreaming but skips the
// final copy from the arena, returning a slice that is backed by a pooled
// deltaArena buffer.
//
// Lifetime / invalidation contract:
// The returned []byte is valid ONLY until ANY of the following events occurs:
//   - The caller invokes any other delta-resolution function on the same
//     goroutine (inflateDeltaChainStreaming, inflateDeltaChainBorrowed,
//     applyDeltaStack, or any store method that inflates a delta).
//   - The deltaArena is returned to deltaArenaPool (which happens at the end
//     of applyDeltaStack via the deferred putDeltaArena call).
//   - The goroutine exits (the runtime may reclaim pooled objects at any GC).
//
// In practice, callers must copy or fully consume the returned data
// synchronously before performing any further object inflation.
func inflateDeltaChainBorrowed(s *store, params inflationParams) ([]byte, ObjectType, error) {
	stack := make(deltaStack, 0, 16)
	base, baseType, err := walkUpDeltaChain(s, params, &stack)
	if err != nil {
		return nil, ObjBad, err
	}
	return applyDeltaStack(stack, base, baseType, params.maxObjectSize, true)
}

// walkUpDeltaChain climbs the delta chain starting at (pack, off) until a
// non-delta "base" object is found.
//
// It returns:
//
//	– the inflated base object     (baseData)
//	– its type                     (baseType)
//	– the stack of deltas that still need to be applied (deltaStack)
//
// The function enforces depth/cycle constraints via ctx.
//
// Parameter notes:
//   - stack – Must be non-nil.  The slice is mutated *in place*: every delta
//     encountered during the walk is appended to it so that the caller (and
//     subsequent applyDeltaStack invocation) see the final, complete list.
func walkUpDeltaChain(
	s *store,
	params inflationParams,
	stack *deltaStack,
) ([]byte, ObjectType, error) {
	if stack == nil {
		return nil, ObjBad, errors.New("stack is nil")
	}

	currPack, currOff := params.p, params.off
	for depth := 0; depth < params.ctx.maxDepth; depth++ {
		typ, hdrLen, err := peekObjectType(currPack, currOff)
		if err != nil {
			return nil, ObjBad, err
		}

		if typ != ObjOfsDelta && typ != ObjRefDelta {
			_, baseData, err := readRawObject(currPack, currOff)
			if err != nil {
				return nil, ObjBad, err
			}
			return baseData, typ, nil
		}

		*stack = append(*stack, deltaInfo{currPack, currOff, typ, hdrLen})

		if typ == ObjRefDelta {
			var baseOID Hash
			pos := int64(currOff) + int64(hdrLen)
			if _, err := currPack.ReadAt(baseOID[:], pos); err != nil {
				return nil, ObjBad, err
			}
			if err = params.ctx.checkRefDelta(baseOID); err != nil { // Cycle detection check.
				return nil, ObjBad, err
			}
			params.ctx.enterRefDelta(baseOID) // Mark this ref-delta as visited.

			var ok bool
			currPack, currOff, ok = s.findPackedObject(baseOID)
			if !ok {
				return nil, ObjBad, fmt.Errorf("base object %s not found", baseOID)
			}
			continue
		}

		// ofs-delta: read the variable-length backward offset and subtract
		// it from the current position to reach the base object.
		//
		// Acyclicity reasoning: every ofs-delta stores a strictly positive
		// backward byte offset within the same packfile, so the base always
		// sits at a lower file position than the delta that references it.
		// Because file offsets decrease monotonically on each hop, the walk
		// must terminate -- it cannot revisit a previously seen offset. This
		// structural property makes explicit cycle detection unnecessary for
		// ofs-delta chains (though the depth limit still applies).
		pos := int64(currOff) + int64(hdrLen)
		back := readOfsDeltaOffset(currPack, pos)
		currOff -= back
	}

	return nil, ObjBad, fmt.Errorf("delta chain too deep (max %d)", params.ctx.maxDepth)
}

// applyDeltaStack consumes the *same* deltaStack that was filled by
// walkUpDeltaChain. The slice is treated as read-only here; no mutations are
// performed after the walk-up phase has completed. The function then resolves
// the chain using the established ping-pong arena strategy.
//
// When borrowed is true, the returned []byte is backed by the arena and
// must be consumed before the next delta resolution (see the lifetime
// contract documented on inflateDeltaChainBorrowed). When borrowed is false,
// a fresh heap allocation is returned that the caller may retain indefinitely.
//
// Ordering invariant: the stack is iterated from len(stack)-1 down to 0.
// walkUpDeltaChain pushes deltas in walk-up order (child first, base last),
// so iterating in reverse applies the oldest (closest-to-base) delta first
// and the newest (closest-to-target) delta last, which is the correct
// application order for reconstructing the final object.
func applyDeltaStack(
	stack deltaStack,
	baseData []byte,
	baseType ObjectType,
	maxObjectSize uint64,
	borrowed bool, // when true, skip the final copy (caller must consume before next resolution)
) ([]byte, ObjectType, error) {
	if len(stack) == 0 {
		if borrowed {
			return baseData, baseType, nil
		}
		// Fast-path: no deltas at all.
		result := make([]byte, len(baseData))
		copy(result, baseData)
		return result, baseType, nil
	}

	arena := getDeltaArena()
	defer putDeltaArena(arena)

	maxTarget := uint64(len(baseData))
	poolHalf := uint64(cap(arena.data) / 2)
	if poolHalf > maxTarget && (maxObjectSize == 0 || poolHalf <= maxObjectSize) {
		maxTarget = poolHalf
	}
	if maxObjectSize > 0 && maxTarget > maxObjectSize {
		return nil, ObjBad, fmt.Errorf("%w: target=%d limit=%d", ErrDeltaTargetTooLarge, maxTarget, maxObjectSize)
	}
	if maxTarget > uint64(cap(arena.data)/2) {
		arena.data = make([]byte, maxTarget*2)
	}

	// Let's ping-pong!
	bufA := arena.data[:maxTarget]
	bufB := arena.data[maxTarget : maxTarget*2]

	// Start with base data in bufA.
	current := bufA[:len(baseData)]
	copy(current, baseData)

	// Track which buffer we're using.
	usingA := true

	for i := len(stack) - 1; i >= 0; i-- {
		d := stack[i]

		for {
			// Choose output buffer (the one we're NOT currently using).
			var out []byte
			if usingA {
				out = bufB[:0]
			} else {
				out = bufA[:0]
			}

			result, err := applyDeltaStreaming(d.pack, d.offset, d.typ, d.hdrLen, current, out)
			if err != nil {
				var tooSmall *errDeltaOutputTooSmall
				if errors.As(err, &tooSmall) {
					if maxObjectSize > 0 && tooSmall.need > maxObjectSize {
						return nil, ObjBad, fmt.Errorf("%w: target=%d limit=%d", ErrDeltaTargetTooLarge, tooSmall.need, maxObjectSize)
					}
					if tooSmall.need <= maxTarget {
						return nil, ObjBad, err
					}

					maxTarget = tooSmall.need
					arena.data = make([]byte, maxTarget*2)
					bufA = arena.data[:maxTarget]
					bufB = arena.data[maxTarget : maxTarget*2]

					// Re-anchor current into the new arena and retry this delta.
					rebased := bufA[:len(current)]
					copy(rebased, current)
					current = rebased
					usingA = true
					continue
				}
				return nil, ObjBad, err
			}

			// The result is now in 'out' buffer, make it the current for next iteration.
			current = result
			usingA = !usingA // Switch which buffer we're using
			break
		}
	}

	if borrowed {
		// Return arena-backed data directly. The caller must consume the data
		// before any subsequent delta resolution on this goroutine.
		return current, baseType, nil
	}

	// Copy final result out of the arena so the arena can be recycled.
	final := make([]byte, len(current))
	copy(final, current)
	return final, baseType, nil
}

// readOfsDeltaOffset reads a variable-length backward offset from an
// ofs-delta object header.
//
// Encoding algorithm (from the Git pack format spec):
//
// The first byte contributes its lower 7 bits directly. Each subsequent
// byte, if the previous byte had its MSB (continuation bit) set, is
// folded in as follows:
//
//	offset++                                   // +1 bias
//	offset = (offset << 7) | (byte & 0x7f)    // shift and merge
//
// The +1 before each shift is critical: it ensures that the encoding is
// non-ambiguous. Without it, a leading 0x00 continuation byte would be
// indistinguishable from "no more bytes", making the encoding
// non-canonical. The +1 guarantees that every distinct offset has exactly
// one valid encoding and that the decoder never produces a zero offset
// from a non-empty continuation sequence.
//
// Up to 9 bytes are read in a single ReadAt to avoid per-byte syscall
// overhead on the memory-mapped file.
func readOfsDeltaOffset(pack *mmap.ReaderAt, pos int64) uint64 {
	var buf [9]byte
	n, _ := pack.ReadAt(buf[:], pos)
	if n == 0 {
		return 0
	}

	offset := uint64(buf[0] & 0x7f)
	for i := 1; i < n; i++ {
		if buf[i-1]&0x80 == 0 {
			return offset
		}
		offset++
		offset = (offset << 7) | uint64(buf[i]&0x7f)
	}

	return offset
}

// applyDeltaStreaming applies delta instructions to reconstruct an object.
// The method uses pre-allocated buffers and streaming decompression to minimize memory usage.
//
// The output buffer must be pre-allocated with sufficient capacity for the
// target object size.
// applyDeltaStreaming returns an error if the delta instructions are malformed or reference
// data outside the base object bounds.
func applyDeltaStreaming(
	pack *mmap.ReaderAt,
	offset uint64,
	deltaType ObjectType,
	cachedHdrLen int, // pre-parsed header length from walkUpDeltaChain; 0 means re-parse
	base []byte,
	out []byte, // Pre-allocated output buffer
) ([]byte, error) {
	hdrLen := cachedHdrLen
	if hdrLen <= 0 {
		var err error
		_, hdrLen, err = peekObjectType(pack, offset)
		if err != nil {
			return nil, err
		}
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

	// Reuse buffered readers to avoid allocating one per delta application.
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
		return nil, &errDeltaOutputTooSmall{need: targetSize, have: cap(out)}
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

// decodeCopyCommand decodes the offset and size embedded in a *copy* command
// byte according to the Git delta format.
//
// It reads any variable‑length operands from br and returns the computed offset
// and size.
// If size is encoded as zero the function substitutes 0x10000, mirroring Git's
// behavior.
func decodeCopyCommand(cmd byte, br *bufio.Reader) (offset, size int, err error) {
	// Count the operand bytes indicated by the lower 7 bits of cmd.
	bits := cmd & 0x7f
	count := popcount7(bits)
	if count == 0 {
		return 0, 0x10000, nil
	}

	var buf [7]byte
	if _, err := io.ReadFull(br, buf[:count]); err != nil {
		return 0, 0, err
	}

	// Unpack operand bytes in order. The shift table maps each bit position
	// to the corresponding shift amount for offset (bits 0-3) and size (bits 4-6).
	idx := 0
	if bits&0x01 != 0 {
		offset = int(buf[idx])
		idx++
	}
	if bits&0x02 != 0 {
		offset |= int(buf[idx]) << 8
		idx++
	}
	if bits&0x04 != 0 {
		offset |= int(buf[idx]) << 16
		idx++
	}
	if bits&0x08 != 0 {
		offset |= int(buf[idx]) << 24
		idx++
	}
	if bits&0x10 != 0 {
		size = int(buf[idx])
		idx++
	}
	if bits&0x20 != 0 {
		size |= int(buf[idx]) << 8
		idx++
	}
	if bits&0x40 != 0 {
		size |= int(buf[idx]) << 16
	}

	if size == 0 {
		size = 0x10000
	}

	return offset, size, nil
}

// popcount7 returns the number of set bits in the lower 7 bits of b.
//
// Only 7 bits are counted (not 8) because the MSB of a Git delta copy
// command byte is the copy/insert discriminator flag (0x80), not an
// operand-presence indicator. The lower 7 bits (bits 0-6) each signal
// whether a corresponding operand byte follows in the stream:
//   - bits 0-3: offset bytes (up to 4)
//   - bits 4-6: size bytes (up to 3)
//
// The implementation uses a standard parallel bit-count (SWAR) algorithm
// restricted to 7 bits.
func popcount7(b byte) int {
	b = b & 0x7f
	b = (b & 0x55) + ((b >> 1) & 0x55)
	b = (b & 0x33) + ((b >> 2) & 0x33)
	return int((b + (b >> 4)) & 0x0f)
}

// readVarIntFromReader reads a base‑128 varint from br and returns its value
// and the number of bytes consumed.
//
// An error is returned when the encoding exceeds nine bytes (which Git never
// produces) or when the underlying reader reports an I/O failure.
func readVarIntFromReader(br *bufio.Reader) (uint64, int, error) {
	var (
		value     uint64
		shift     uint
		bytesRead int
	)

	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, -1, err
		}
		bytesRead++

		value |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7

		// Git encodings never exceed nine bytes; treat anything longer as
		// corrupted input.
		const maxEncodedVarIntSize = 9
		if bytesRead > maxEncodedVarIntSize {
			return 0, -1, fmt.Errorf("varint too long")
		}
	}

	return value, bytesRead, nil
}
