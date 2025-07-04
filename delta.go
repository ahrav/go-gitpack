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
	"fmt"
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
