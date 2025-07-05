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

// inflateDeltaChainStreaming resolves a delta chain efficiently using streaming decompression.
// The method walks the chain to find the base object, then applies deltas using
// ping-pong buffers to minimize memory allocation.
//
// inflateDeltaChainStreaming returns the fully reconstructed object and its base type.
func inflateDeltaChainStreaming(
	s *store,
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
	maxTargetSize := peekLargestTarget(deltaStack)
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
		newResult, err := applyDeltaStreaming(
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

// deltaArena holds two equally‑sized byte slices so the resolver can ping‑pong
// between *source* and *destination* buffers while walking a delta chain.
type deltaArena struct{ data []byte }

// deltaArenaPool loans arenas large enough to hold two complete objects of up
// to 16 MiB each (32 MiB total).
var deltaArenaPool = sync.Pool{
	New: func() any {
		const maxObjectSize = 16 << 20 // 16 MiB
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
