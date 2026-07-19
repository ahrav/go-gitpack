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
// ErrDeltaTargetTooLarge is the only exported symbol.
// All other symbols below this comment are package‑private in order to keep
// the public surface minimal and stable.
package objstore

import (
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
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

	// depth is the current recursion depth.
	// Cleared to zero when the context is returned to the pool via reset().
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
// The len guard matters: most lookups never follow a ref-delta, so visited
// is usually empty, and clear() on an empty map still walks its buckets —
// which showed up as ~3% of scan CPU before the guard.
func (ctx *deltaContext) reset() {
	if len(ctx.visited) > 0 {
		clear(ctx.visited)
	}
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

// defaultDeltaArenaSize is the standard allocation for pooled ping-pong arenas:
// two 16 MiB halves = 32 MiB total.
const defaultDeltaArenaSize = 2 * 16 << 20

// deltaArenaFreeList is a bounded free-list of ping-pong arenas.
//
// A buffered channel is used instead of sync.Pool because sync.Pool is
// emptied on every GC cycle. Bulk scans allocate gigabytes per second, so GC
// runs every few milliseconds and a sync.Pool would shed its 32 MiB arenas
// constantly — each miss re-allocates (and zeroes) 32 MiB, which showed up as
// ~45% of all allocated bytes and ~5% of CPU in memclr. The channel retains
// arenas across GC cycles; capacity bounds worst-case retained memory to
// deltaArenaMaxRetained arenas.
//
// Deliberate trade-off: unlike sync.Pool, the free-list is never drained, so
// after a burst of concurrent multi-hop delta resolutions the process
// permanently retains up to deltaArenaMaxRetained x 32 MiB (256 MiB on
// >= 8-CPU hosts) of idle arena memory. This is process-global and shared by
// every open store; it is the steady-state working set of the bulk-scan
// workload this library targets, not a leak. Long-lived embedders that are
// memory-constrained should bound scan concurrency (or GOMAXPROCS), which
// proportionally bounds the retained reserve.
var deltaArenaFreeList = make(chan *deltaArena, deltaArenaMaxRetained)

// deltaArenaMaxRetained bounds how many idle arenas the free-list may retain.
// The steady-state population equals the peak number of concurrent delta
// resolutions, so the cap is derived from GOMAXPROCS at startup: hosts with
// fewer CPUs cannot productively use more concurrent arenas, and sizing the
// list to the parallelism budget keeps the idle reserve proportional to the
// machine. The upper cap limits retained arenas to 256 MiB on large hosts.
var deltaArenaMaxRetained = min(8, max(2, runtime.GOMAXPROCS(0)))

// getDeltaArena retrieves a ping‑pong arena from the free-list or allocates
// a fresh one when the list is empty.
func getDeltaArena() *deltaArena {
	select {
	case arena := <-deltaArenaFreeList:
		return arena
	default:
		return &deltaArena{data: make([]byte, defaultDeltaArenaSize)}
	}
}

// prepareDeltaArenaForPool reports whether arena may be returned to the pool,
// restoring its full length in place when it may. Arenas that were grown
// beyond the default pool size during dynamic resizing are ineligible (and
// left untouched) so the pool is never polluted with oversized allocations.
//
// Split from putDeltaArena so the reset/discard decision is testable before
// ownership transfers to the pool: once Put runs, another goroutine may
// legitimately retrieve and mutate the arena, so no caller (test or
// otherwise) may inspect it afterwards.
func prepareDeltaArenaForPool(arena *deltaArena) bool {
	if cap(arena.data) > defaultDeltaArenaSize {
		return false
	}
	arena.data = arena.data[:cap(arena.data)]
	return true
}

// putDeltaArena returns arena to the free-list when it is pool-eligible (see
// prepareDeltaArenaForPool). If the free-list is full the arena is dropped for
// the GC. The arena must not be touched after this call.
func putDeltaArena(arena *deltaArena) {
	if !prepareDeltaArenaForPool(arena) {
		return
	}
	select {
	case deltaArenaFreeList <- arena:
	default:
	}
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
//     One arena twice that size is borrowed from the free-list and sliced
//     into two equal halves:
//
//     +----------------------+----------------------+
//     |  buffer A (input)    |  buffer B (output)   |
//     +----------------------+----------------------+
//
//     The first delta reads baseData in place – it is never copied into the
//     arena – and writes its result into buffer A.  The function then walks
//     deltaStack backwards (from the oldest delta down to the newest),
//     calling applyDeltaStreaming for each element:
//
//     current, out = baseData, bufferA
//     for delta := range reverse(deltaStack) {           // pseudo code
//     current = applyDeltaStreaming(delta, current, out) // writes into 'out'
//     out     = otherHalf(current)                   // ping-pong A<->B
//     }
//
//     Each hop writes into whichever half is not holding current – hence the
//     *ping-pong* terminology – guaranteeing that reads and writes never
//     overlap and that no further allocations or copies are required.
//
//     When the loop terminates the fully reconstructed object lives in the
//     slice currently referenced by 'current'.  A final copy is made into a
//     fresh slice so callers may retain the result after the arena has been
//     returned to the free-list.
//
// The function fails with an error when the chain is too deep, cyclic, the
// pack-file is corrupted, or any delta instruction is invalid.
// IMPORTANT: Because the slice is mutated in-place it MUST NOT be reused once
// resolution is complete.  applyDeltaStack treats the slice as read-only and
// does not retain it after the call returns.
func inflateDeltaChainStreaming(s *store, params inflationParams) ([]byte, ObjectType, error) {
	stack := make(deltaStack, 0, 16) // pre-size for typical chain depths
	base, baseType, err := walkUpDeltaChain(s, params, &stack, s.offCache)
	if err != nil {
		return nil, ObjBad, err
	}
	return applyDeltaStackCached(s.offCache, stack, base, baseType, params.maxObjectSize, false)
}

// inflateDeltaChainBorrowed is the entry point for consume-and-discard callers
// (cacheResult=false). It neither consults nor publishes to the offset cache.
// Multi-hop results are detached from the pooled arena before return.
func inflateDeltaChainBorrowed(s *store, params inflationParams) ([]byte, ObjectType, error) {
	stack := make(deltaStack, 0, 16)
	base, baseType, err := walkUpDeltaChain(s, params, &stack, nil)
	if err != nil {
		return nil, ObjBad, err
	}
	return applyDeltaStackCached(nil, stack, base, baseType, params.maxObjectSize, true)
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
	oc *offsetCache,
) ([]byte, ObjectType, error) {
	if stack == nil {
		return nil, ObjBad, errors.New("stack is nil")
	}

	currPack, currOff := params.p, params.off
	// currOID tracks the OID of the object at (currPack, currOff) when it is
	// known: at depth 0 it is the lookup target itself, and each ref-delta
	// hop names its base by OID. ofs-delta hops identify the base only by
	// byte offset, so the OID becomes unknown (zero) until a later ref-delta
	// re-establishes it. findCRCForObject uses the OID only for its midx
	// fallback, which requires the entry's offset to match — so a zero OID
	// degrades to a conservative skip, never a wrong CRC.
	currOID := params.oid
	for depth := 0; depth < params.ctx.maxDepth; depth++ {

		// Short-circuit: if any prior resolution materialized the object at
		// this offset, adopt it as the base immediately instead of climbing
		// further. Sibling blob versions share long chain tails, so in bulk
		// scans ~90% of hops land on an already-materialized offset. The
		// check also fires at depth 0: OID-keyed caches (delta window, ARC)
		// can miss objects that the offset cache still holds.
		if data, typ, ok := oc.get(currPack, currOff); ok {
			return data, typ, nil
		}

		typ, hdrLen, err := peekObjectType(currPack, currOff)
		if err != nil {
			return nil, ObjBad, err
		}

		if typ != ObjOfsDelta && typ != ObjRefDelta {
			_, baseData, err := readRawObject(currPack, currOff)
			if err != nil {
				return nil, ObjBad, err
			}
			// Verify the raw pack record before publishing it: store.get
			// serves offset-cache hits directly, so an unverified entry
			// here would let later direct reads of this object bypass the
			// CRC check that inflateFromPack would otherwise perform.
			// Mirrors inflateFromPackWithOptions: verify when the CRC is
			// known, proceed when the index has none. Reconstructed delta
			// hops published by applyDeltaStackCached are not affected —
			// pack CRCs cover raw records only, and delta-typed reads
			// never carried a CRC check. Note the midx CRC fallback only
			// fires when currOID is known (see the currOID declaration);
			// per-pack idx lookups are keyed purely by offset and always
			// apply.
			if s.VerifyCRC {
				if crc, ok := s.findCRCForObject(currPack, currOff, currOID); ok {
					if err := s.verifyCRCForPackObject(currPack, currOff, crc); err != nil {
						return nil, ObjBad, err
					}
				}
			}
			oc.add(currPack, currOff, baseData, typ)
			return baseData, typ, nil
		}

		*stack = append(*stack, deltaInfo{currPack, currOff, typ})

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
			currOID = baseOID
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
		back, err := readOfsDeltaOffset(currPack, pos)
		if err != nil {
			return nil, ObjBad, fmt.Errorf("read ofs-delta offset: %w", err)
		}
		currOff -= back
		// The base is identified only by byte offset; its OID is unknown
		// until a subsequent ref-delta hop names one.
		currOID = Hash{}
	}

	return nil, ObjBad, fmt.Errorf("delta chain too deep (max %d)", params.ctx.maxDepth)
}

// applyDeltaStackCached consumes the *same* deltaStack that was filled by
// walkUpDeltaChain. The slice is treated as read-only here; no mutations are
// performed after the walk-up phase has completed. The function resolves the
// chain using the established ping-pong arena strategy and, when oc is
// non-nil, records every materialized hop (intermediate and final) under its
// pack offset so later chains sharing the same tail can short-circuit in
// walkUpDeltaChain.
//
// borrowed preserves the historical call-site intent for consume-and-discard
// paths. For correctness, the final bytes are always detached from the pooled
// arena before the function returns.
//
// Ordering invariant: the stack is iterated from len(stack)-1 down to 0.
// walkUpDeltaChain pushes deltas in walk-up order (child first, base last),
// so iterating in reverse applies the oldest (closest-to-base) delta first
// and the newest (closest-to-target) delta last, which is the correct
// application order for reconstructing the final object.
func applyDeltaStackCached(
	oc *offsetCache,
	stack deltaStack,
	baseData []byte,
	baseType ObjectType,
	maxObjectSize uint64,
	borrowed bool, // retained for call-site intent; final bytes are always detached
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

	// Single-hop fast path (the majority of chains once the offset cache
	// short-circuits walk-up): apply the one delta into a fresh exact-size
	// buffer and return it directly — no arena, no detach copy.
	// applyDeltaStreaming enforces maxObjectSize before allocating the
	// exact-size buffer, so a hostile advertised target cannot force a
	// large allocation ahead of the limit check.
	if len(stack) == 1 {
		d := stack[0]
		final, err := applyDeltaStreaming(d.pack, d.offset, d.typ, baseData, nil, true, maxObjectSize)
		if err != nil {
			var tooSmall *errDeltaOutputTooSmall
			// allocExact never under-allocates, so errDeltaOutputTooSmall
			// here would indicate a logic error; handle size-limit checks
			// explicitly below either way.
			if errors.As(err, &tooSmall) && maxObjectSize > 0 && tooSmall.need > maxObjectSize {
				return nil, ObjBad, fmt.Errorf("%w: target=%d limit=%d", ErrDeltaTargetTooLarge, tooSmall.need, maxObjectSize)
			}
			return nil, ObjBad, err
		}
		if maxObjectSize > 0 && uint64(len(final)) > maxObjectSize {
			return nil, ObjBad, fmt.Errorf("%w: target=%d limit=%d", ErrDeltaTargetTooLarge, len(final), maxObjectSize)
		}
		if oc != nil {
			oc.add(d.pack, d.offset, final, baseType)
		}
		return final, baseType, nil
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

	// Let's ping-pong! Full slice expressions pin each half's capacity to
	// its own boundary: without them bufA's capacity would extend through
	// bufB (and bufB's through any arena tail), letting a later hop whose
	// target exceeds maxTarget write straight across the A/B boundary and
	// corrupt the buffer the next hop reads — instead of taking the
	// errDeltaOutputTooSmall resize path (which also enforces
	// maxObjectSize).
	bufA := arena.data[0:maxTarget:maxTarget]
	bufB := arena.data[maxTarget : maxTarget*2 : maxTarget*2]

	// The first application reads the base directly — applyDeltaStreaming
	// never writes its input, so copying baseData into the arena first was
	// pure overhead (one memmove per materialized object). usingA=false
	// makes the first hop write into bufA; the ping-pong then proceeds
	// normally and baseData is never written.
	current := baseData
	usingA := false

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

			result, err := applyDeltaStreaming(d.pack, d.offset, d.typ, current, out, false, maxObjectSize)
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
					bufA = arena.data[0:maxTarget:maxTarget]
					bufB = arena.data[maxTarget : maxTarget*2 : maxTarget*2]

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

		// Publish intermediate materializations (every hop except the final
		// target, which callers cache by OID) into the offset cache so that
		// later chains sharing this tail stop climbing here. The copy is
		// required because 'current' aliases the recycled ping-pong arena;
		// a memmove is far cheaper than the inflate+apply work it saves.
		// The eligibility check runs BEFORE the detach copy: offsetCache.add
		// rejects objects above maxCacheableSize, so copying first would
		// burn a guaranteed-discarded allocation at every oversized hop.
		if i > 0 && oc != nil && len(current) <= maxCacheableSize {
			detached := make([]byte, len(current))
			copy(detached, current)
			oc.add(d.pack, d.offset, detached, baseType)
		}
	}

	// Always copy final result out of the arena so the arena can be safely
	// recycled without invalidating returned bytes.
	final := make([]byte, len(current))
	copy(final, current)
	_ = borrowed // call-site signal is currently informational only.
	if len(stack) > 0 {
		oc.add(stack[0].pack, stack[0].offset, final, baseType)
	}
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
func readOfsDeltaOffset(pack *mmap.ReaderAt, pos int64) (uint64, error) {
	var buf [9]byte
	n, err := pack.ReadAt(buf[:], pos)
	if n == 0 {
		if err != nil {
			return 0, fmt.Errorf("read ofs-delta offset: %w", err)
		}
		return 0, fmt.Errorf("read ofs-delta offset: no bytes read")
	}

	offset := uint64(buf[0] & 0x7f)
	for i := 1; i < n; i++ {
		if buf[i-1]&0x80 == 0 {
			return offset, nil
		}
		offset++
		offset = (offset << 7) | uint64(buf[i]&0x7f)
	}

	// The last available byte still has its continuation bit set: either the
	// pack is truncated mid-varint, or the encoding exceeds the 9-byte bound
	// Git can ever produce. Returning the partial value would silently climb
	// to a garbage offset; the inline skip in applyDeltaStreaming rejects the
	// same input, and the two must stay in agreement.
	if buf[n-1]&0x80 != 0 {
		return 0, fmt.Errorf("read ofs-delta offset: truncated or over-long varint")
	}

	return offset, nil
}

// applyDeltaStreaming applies delta instructions to reconstruct an object.
// The method uses pre-allocated buffers and streaming decompression to minimize memory usage.
//
// The output buffer must be pre-allocated with sufficient capacity for the
// target object size (unless allocExact is set, in which case out is ignored
// and a fresh exact-size buffer is allocated after the advertised target has
// been validated against maxObjectSize).
//
// maxObjectSize bounds both the advertised target size and the delta payload
// itself; zero disables the bound. Enforcing it here before any allocation
// is sized from pack-controlled headers — means a corrupt or hostile pack
// cannot force a large allocation ahead of the limit check.
//
// applyDeltaStreaming returns an error if the delta instructions are malformed or reference
// data outside the base object bounds.
func applyDeltaStreaming(
	pack *mmap.ReaderAt,
	offset uint64,
	deltaType ObjectType,
	base []byte,
	out []byte, // Pre-allocated output buffer (ignored when allocExact)
	allocExact bool, // allocate a fresh exact-size output after reading the header
	maxObjectSize uint64, // reject targets/payloads beyond this bound; 0 disables
) ([]byte, error) {
	// Parse the pack object header ourselves: its size field is the
	// decompressed length of the delta payload (varint header + instruction
	// stream), which lets the whole payload be inflated in one shot into a
	// pooled scratch buffer. Instruction processing then runs over a flat
	// []byte with index arithmetic — no bufio layer, no per-command
	// ReadByte virtual calls, and (with the libdeflate backend) no
	// streaming inflater state at all.
	//
	// The header (and the ofs-delta base offset below) is read through the
	// public ReadAt API rather than mmapData so this file stays portable:
	// on mmap-backed platforms ReadAt is a bounds-checked copy of ≤32
	// bytes, and on fallback platforms (no mmapData symbol) it is real
	// file I/O of the same size.
	var hdrBuf [32]byte
	n, err := pack.ReadAt(hdrBuf[:], int64(offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if n == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	_, payloadSize, hdrLen := parseObjectHeaderUnsafe(hdrBuf[:n])
	if hdrLen <= 0 {
		return nil, ErrCannotParseObjectHeader
	}
	pos := int64(offset) + int64(hdrLen)

	// Skip the base object reference (hash or offset).
	switch deltaType {
	case ObjRefDelta:
		pos += 20
	case ObjOfsDelta:
		// Skip the variable-length backward offset (≤9 bytes; git never
		// emits more). A varint that does not terminate within the buffer
		// is rejected, exactly as readOfsDeltaOffset rejects it during
		// walk-up; the two parsers must stay in agreement.
		var ofsBuf [9]byte
		on, oerr := pack.ReadAt(ofsBuf[:], pos)
		if oerr != nil && !errors.Is(oerr, io.EOF) {
			return nil, oerr
		}
		j := 0
		for j < on && ofsBuf[j]&0x80 != 0 {
			j++
		}
		if j >= on {
			return nil, io.ErrUnexpectedEOF
		}
		pos += int64(j) + 1
	}

	// Bound the payload before sizing any allocation from it. payloadSize
	// comes straight from the pack header, so a malformed pack can
	// advertise an arbitrary value: unchecked, int(payloadSize) overflows
	// on 32-bit (and wraps negative for values above MaxInt on 64-bit),
	// and getDeltaScratch would allocate the full advertised amount.
	//
	// The bound must account for delta encoding overhead: the payload
	// holds the two size varints plus the instruction stream, so a valid
	// delta for a target at the limit is necessarily LARGER than the
	// target itself (a literal-heavy delta is ~target+target/127 bytes;
	// a pathological-but-valid stream of size-1 copies costs up to 8
	// bytes per output byte). 8×maxObjectSize+32 admits every payload
	// that can possibly reconstruct a within-limit target while still
	// capping scratch allocation at a small multiple of the configured
	// ceiling.
	if payloadSize > uint64(math.MaxInt) {
		return nil, fmt.Errorf("delta payload size %d exceeds addressable memory", payloadSize)
	}
	if maxObjectSize > 0 && maxObjectSize <= (math.MaxUint64-32)/8 {
		if bound := 8*maxObjectSize + 32; payloadSize > bound {
			return nil, fmt.Errorf("%w: payload=%d exceeds bound %d for limit=%d",
				ErrDeltaTargetTooLarge, payloadSize, bound, maxObjectSize)
		}
	}

	scratch := getDeltaScratch(int(payloadSize))
	defer putDeltaScratch(scratch)
	delta := scratch.buf[:payloadSize]
	if err := inflateExact(pack, pos, delta); err != nil {
		return nil, err
	}

	// Read the delta header, which specifies the base and target object sizes.
	baseSize, n := decodeVarInt(delta)
	if n <= 0 {
		return nil, errors.New("read delta base size: bad varint")
	}
	delta = delta[n:]
	if baseSize != uint64(len(base)) {
		return nil, fmt.Errorf("delta base size mismatch: header=%d actual=%d", baseSize, len(base))
	}

	targetSize, n := decodeVarInt(delta)
	if n <= 0 {
		return nil, errors.New("invalid delta target size")
	}
	delta = delta[n:]

	// Enforce the size limit on the advertised target BEFORE any output
	// buffer is allocated from it.
	if maxObjectSize > 0 && targetSize > maxObjectSize {
		return nil, fmt.Errorf("%w: target=%d limit=%d", ErrDeltaTargetTooLarge, targetSize, maxObjectSize)
	}

	if allocExact {
		// Caller asked for a fresh, exactly-sized output buffer. This lets
		// single-hop chains (the common case) bypass the ping-pong arena
		// and the detach copy: the result is returned directly.
		out = make([]byte, 0, targetSize)
	} else if uint64(cap(out)) < targetSize {
		// Verify that the output buffer is large enough for the target object.
		return nil, &errDeltaOutputTooSmall{need: targetSize, have: cap(out)}
	}

	// Initialize a zero-length slice to ensure no undefined content is present.
	out = out[:0]

	// Process the delta instructions, copying data from the base or inserting new data.
	i := 0
	for uint64(len(out)) < targetSize && i < len(delta) {
		cmd := delta[i]
		i++
		if cmd&0x80 != 0 {
			// Copy command: decode operand bytes indicated by the low 7 bits.
			var cpOff, cpSize int
			if cmd&0x01 != 0 {
				if i >= len(delta) {
					return nil, io.ErrUnexpectedEOF
				}
				cpOff = int(delta[i])
				i++
			}
			if cmd&0x02 != 0 {
				if i >= len(delta) {
					return nil, io.ErrUnexpectedEOF
				}
				cpOff |= int(delta[i]) << 8
				i++
			}
			if cmd&0x04 != 0 {
				if i >= len(delta) {
					return nil, io.ErrUnexpectedEOF
				}
				cpOff |= int(delta[i]) << 16
				i++
			}
			if cmd&0x08 != 0 {
				if i >= len(delta) {
					return nil, io.ErrUnexpectedEOF
				}
				cpOff |= int(delta[i]) << 24
				i++
			}
			if cmd&0x10 != 0 {
				if i >= len(delta) {
					return nil, io.ErrUnexpectedEOF
				}
				cpSize = int(delta[i])
				i++
			}
			if cmd&0x20 != 0 {
				if i >= len(delta) {
					return nil, io.ErrUnexpectedEOF
				}
				cpSize |= int(delta[i]) << 8
				i++
			}
			if cmd&0x40 != 0 {
				if i >= len(delta) {
					return nil, io.ErrUnexpectedEOF
				}
				cpSize |= int(delta[i]) << 16
				i++
			}
			if cpSize == 0 {
				cpSize = 0x10000
			}

			// Overflow-safe bounds check: cpOff can go negative on 32-bit
			// ints (4th operand byte ≥ 0x80), and cpOff+cpSize can wrap;
			// the subtraction form avoids both.
			if cpOff < 0 || cpSize > len(base) || cpOff > len(base)-cpSize {
				return nil, errors.New("copy beyond base bounds")
			}

			// Bound the write against the declared target BEFORE extending:
			// with allocExact the output capacity is exactly targetSize, so
			// an oversized command would panic on the slice extension
			// instead of returning an error.
			if uint64(cpSize) > targetSize-uint64(len(out)) {
				return nil, errors.New("delta copy exceeds declared target size")
			}

			// Extend the output slice and copy the specified segment from the base.
			outPos := len(out)
			out = out[:outPos+cpSize]
			copy(out[outPos:], base[cpOff:cpOff+cpSize])
		} else if cmd > 0 {
			// Insert command: literal bytes follow inline in the stream.
			size := int(cmd)
			if i+size > len(delta) {
				return nil, io.ErrUnexpectedEOF
			}
			if uint64(size) > targetSize-uint64(len(out)) {
				return nil, errors.New("delta insert exceeds declared target size")
			}
			outPos := len(out)
			out = out[:outPos+size]
			copy(out[outPos:], delta[i:i+size])
			i += size
		} else {
			return nil, errors.New("invalid delta command")
		}
	}

	if uint64(len(out)) != targetSize {
		return nil, fmt.Errorf("delta size mismatch: got %d, want %d", len(out), targetSize)
	}

	return out, nil
}

// deltaScratch is a pooled buffer for one-shot delta payload inflation.
type deltaScratch struct{ buf []byte }

var deltaScratchPool = sync.Pool{
	New: func() any { return &deltaScratch{buf: make([]byte, 64<<10)} },
}

// getDeltaScratch returns a pooled scratch whose buf has at least n bytes.
func getDeltaScratch(n int) *deltaScratch {
	s := deltaScratchPool.Get().(*deltaScratch)
	if cap(s.buf) < n {
		s.buf = make([]byte, n)
	}
	s.buf = s.buf[:cap(s.buf)]
	return s
}

// putDeltaScratch recycles a scratch buffer; oversized ones (>8 MiB) are
// dropped so a single huge delta cannot pin memory in the pool.
func putDeltaScratch(s *deltaScratch) {
	if cap(s.buf) > 8<<20 {
		return
	}
	deltaScratchPool.Put(s)
}

// decodeVarInt reads a base-128 varint from the front of b, returning the
// value and the number of bytes consumed (0 if b is truncated or the varint
// exceeds the 9-byte bound Git ever produces).
func decodeVarInt(b []byte) (uint64, int) {
	var v uint64
	var shift uint
	for i := 0; i < len(b) && i < 9; i++ {
		c := b[i]
		v |= uint64(c&0x7f) << shift
		if c&0x80 == 0 {
			return v, i + 1
		}
		shift += 7
	}
	return 0, 0
}
