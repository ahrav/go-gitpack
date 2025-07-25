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
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"sync"

	"golang.org/x/exp/mmap"
)

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
}

// deltaStack is a stack of deltaInfo objects, used to iteratively
// resolve a delta chain.
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

// newDeltaContext allocates and initializes a deltaContext that enforces the
// supplied maximum delta‑chain depth.
func newDeltaContext(maxDepth int) *deltaContext {
	return &deltaContext{
		visited:  make(map[Hash]bool),
		offsets:  make(map[uint64]bool),
		depth:    0,
		maxDepth: maxDepth,
	}
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
	var stack deltaStack // will be mutated in-place
	base, baseType, err := walkUpDeltaChain(s, params, &stack)
	if err != nil {
		return nil, ObjBad, err
	}
	return applyDeltaStack(stack, base, baseType)
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
		typ, _, err := peekObjectType(currPack, currOff)
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

		*stack = append(*stack, deltaInfo{currPack, currOff, typ})

		if typ == ObjRefDelta {
			var baseOID Hash
			_, _, baseOID, err = readRefDeltaHeader(currPack, currOff)
			if err != nil {
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

		// ofs-delta.
		var back uint64
		_, _, back, err = readOfsDeltaHeader(currPack, currOff)
		if err != nil {
			return nil, ObjBad, err
		}
		currOff -= back
	}

	return nil, ObjBad, fmt.Errorf("delta chain too deep (max %d)", params.ctx.maxDepth)
}

// applyDeltaStack consumes the *same* deltaStack that was filled by
// walkUpDeltaChain.  The slice is treated as read-only here; no mutations are
// performed after the walk-up phase has completed.  The function then resolves
// the chain using the established ping-pong arena strategy.
func applyDeltaStack(
	stack deltaStack,
	baseData []byte,
	baseType ObjectType,
) ([]byte, ObjectType, error) {
	if len(stack) == 0 {
		// Fast-path: no deltas at all.
		result := make([]byte, len(baseData))
		copy(result, baseData)
		return result, baseType, nil
	}

	arena := getDeltaArena()
	defer putDeltaArena(arena)

	maxTarget := peekLargestTarget(stack)
	if bs := uint64(len(baseData)); bs > maxTarget {
		maxTarget = bs
	}
	if maxTarget > uint64(len(arena.data)/2) {
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

		// Choose output buffer (the one we're NOT currently using).
		var out []byte
		if usingA {
			out = bufB[:0]
		} else {
			out = bufA[:0]
		}

		result, err := applyDeltaStreaming(d.pack, d.offset, d.typ, current, out)
		if err != nil {
			return nil, ObjBad, err
		}

		// The result is now in 'out' buffer, make it the current for next iteration.
		current = result
		usingA = !usingA // Switch which buffer we're using
	}

	// Copy final result.
	in := current

	final := make([]byte, len(in))
	copy(final, in)
	return final, baseType, nil
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

// peekLargestTarget calculates the maximum target size needed for delta application.
// This allows pre-allocating buffers of the correct size to avoid reallocation
// during delta chain resolution.
func peekLargestTarget(deltaStack deltaStack) uint64 {
	maxSize := uint64(0)

	for _, delta := range deltaStack {
		// Peek at the delta header to determine the size of the resulting object.
		targetSize, err := peekDeltaTargetSize(delta.pack, delta.offset, delta.typ)
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
func peekDeltaTargetSize(
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

	bufRdr := getBR(zr)
	defer putBR(bufRdr)

	// Skip the base size.
	if _, _, err = readVarIntFromReader(bufRdr); err != nil {
		return 0, err
	}

	// Read the target size.
	targetSize, _, err := readVarIntFromReader(bufRdr)
	return targetSize, err
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
	zr, err := zlib.NewReader(src)
	if err != nil {
		return nil, err
	}
	defer zr.Close()

	// Use a pooled buffered reader for efficient byte-level operations.
	br := bufio.NewReader(zr)

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

// decodeCopyCommand decodes the offset and size embedded in a *copy* command
// byte according to the Git delta format.
//
// It reads any variable‑length operands from br and returns the computed offset
// and size.
// If size is encoded as zero the function substitutes 0x10000, mirroring Git's
// behavior.
func decodeCopyCommand(cmd byte, br *bufio.Reader) (offset, size int, err error) {
	if cmd&0x01 != 0 {
		b, err := br.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		offset = int(b)
	}
	if cmd&0x02 != 0 {
		b, err := br.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		offset |= int(b) << 8
	}
	if cmd&0x04 != 0 {
		b, err := br.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		offset |= int(b) << 16
	}
	if cmd&0x08 != 0 {
		b, err := br.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		offset |= int(b) << 24
	}

	if cmd&0x10 != 0 {
		b, err := br.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		size = int(b)
	}
	if cmd&0x20 != 0 {
		b, err := br.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		size |= int(b) << 8
	}
	if cmd&0x40 != 0 {
		b, err := br.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		size |= int(b) << 16
	}

	if size == 0 {
		size = 0x10000
	}

	return offset, size, nil
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
