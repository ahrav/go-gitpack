// pair_cache.go
//
// Bounded memo of computed diff results keyed by (oldOID, newOID).
//
// The same blob transition frequently recurs across a history walk: a file
// changed on a feature branch is re-observed through the merge commit's
// first-parent diff, and long-lived release branches replay identical
// transitions. Measurements on real repositories show ~1.5x redundancy at
// the pair level. Since computeAddedHunks is a pure function of the two blob
// contents, its result can be shared across those repeats.
package objstore

import (
	"sync"
	"unsafe"
)

// pairCacheShards spreads pair lookups across independent locks.
const pairCacheShards = 32

// defaultPairCacheBudget bounds the bytes retained across all shards of one
// scanner's pair cache. Entries are compacted copies (see add) and every
// component of an entry's footprint — text bytes, per-line string headers,
// per-hunk and per-entry overhead — is charged, so the accounted size tracks
// actual retention. Each scanner owns an independent cache, so processes that
// open many repositories concurrently should lower the budget via
// WithPairCacheBudget to bound aggregate growth.
const defaultPairCacheBudget = 128 << 20

// pairCacheHunkOverhead approximates the per-hunk cost beyond line bytes:
// the AddedHunk struct and its Lines slice header.
const pairCacheHunkOverhead = 64

// pairCacheLineOverhead is the per-line cost of a string header in a cached
// hunk's Lines slice. Every stored hunk carries one []string sized to its
// line count, so retention scales with the number of lines independently of
// how many text bytes they hold: a diff of empty or very short lines retains
// this much per line while contributing almost nothing to lineBytes. Charging
// it stops a blank-line-heavy diff from being admitted as near-free and
// keeps the accounted size within a small factor of the bytes retained.
const pairCacheLineOverhead = int(unsafe.Sizeof(""))

// pairCacheEntryOverhead approximates the fixed per-entry cost: the map key,
// entry struct, and bucket bookkeeping. Charging it keeps entries with no
// line bytes (diffs whose additions are empty) visible to eviction, so the
// map's cardinality stays bounded by the byte budget.
const pairCacheEntryOverhead = 128

// pairKey is the concatenation of the old and new blob OIDs.
type pairKey [2 * hashSize]byte

type pairCacheShard struct {
	mu   sync.Mutex
	m    map[pairKey]pairCacheEntry
	used int
}

type pairCacheEntry struct {
	hunks []AddedHunk
	size  int
}

// pairCache is safe for concurrent use. Entries are immutable once stored:
// readers receive the shared []AddedHunk and MUST NOT modify the hunks or
// their Lines. An entry's line bytes are owned by the entry unless they
// already span the whole new blob (see add), in which case they view the
// store's immutable object buffer. Eviction is approximate (map-order) like
// offsetCache. A zero budgetPerShard turns the memo off without changing what
// add hands back to callers.
type pairCache struct {
	shards         [pairCacheShards]pairCacheShard
	budgetPerShard int
}

func newPairCache() *pairCache {
	c := &pairCache{}
	for i := range c.shards {
		c.shards[i].m = make(map[pairKey]pairCacheEntry, 128)
	}
	// Route through setBudget so the default shares the exact rounding and
	// disable semantics of WithPairCacheBudget.
	c.setBudget(defaultPairCacheBudget)
	return c
}

// setBudget adjusts the total byte budget across all shards and enforces the
// new bound before returning: when the budget drops, retained entries are
// evicted until every shard fits.
//
// A budget <= 0 disables the cache: existing entries are dropped and later
// adds store nothing (gets simply miss). Disabling does not change what a
// caller receives — add still compacts and returns the owned hunks, because
// the aliasing views computeAddedHunks produces would otherwise pin a whole
// blob per delivered hunk whether or not the memo is on.
//
// Enforcement here is eager rather than deferred to add because add rejects
// any entry costing more than a quarter of the per-shard budget. After a large
// reduction that gate can reject every subsequent entry, so add's eviction loop
// would never run and a shard could stay above its configured budget for the
// life of the cache — the one thing a memory bound must not do.
//
// budgetPerShard is written without synchronization, so setBudget must run
// before the cache is visible to concurrent readers and writers —
// WithPairCacheBudget satisfies this by running during scanner construction.
// Concurrent callers must synchronize externally.
func (c *pairCache) setBudget(total int) {
	if c == nil {
		return
	}
	per := total / pairCacheShards
	if total <= 0 {
		per = 0
	} else if per == 0 {
		// A positive budget too small to divide across shards still admits
		// the smallest entries rather than silently disabling the cache.
		per = 1
	}
	c.budgetPerShard = per
	if per == 0 {
		// Replacing the maps is cheaper than evicting key by key and drops
		// the buckets themselves, not just the entries.
		c.clear()
		return
	}
	c.evictToBudget()
}

// evictToBudget drops entries from every over-budget shard until it fits.
// Victim choice is map-order like add's eviction, which is what the cache's
// approximate-replacement contract already promises.
func (c *pairCache) evictToBudget() {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		for key, v := range s.m {
			if s.used <= c.budgetPerShard {
				break
			}
			delete(s.m, key)
			s.used -= v.size
		}
		s.mu.Unlock()
	}
}

func makePairKey(oldOID, newOID Hash) pairKey {
	var k pairKey
	copy(k[:hashSize], oldOID[:])
	copy(k[hashSize:], newOID[:])
	return k
}

func (c *pairCache) shard(k *pairKey) *pairCacheShard {
	// Mix one byte from each OID: additions carry a zero old OID and
	// deletions a zero new OID, so either byte alone would funnel an
	// entire class of pairs into shard 0.
	return &c.shards[int(k[0]^k[hashSize])&(pairCacheShards-1)]
}

func (c *pairCache) get(k pairKey) ([]AddedHunk, bool) {
	s := c.shard(&k)
	s.mu.Lock()
	e, ok := s.m[k]
	s.mu.Unlock()
	return e.hunks, ok
}

// clear drops every cached entry, releasing the retained hunk lines — and the
// whole-blob buffers the aliasing entries view — to the GC. The cache remains
// usable afterwards.
func (c *pairCache) clear() {
	if c == nil {
		return
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.m = make(map[pairKey]pairCacheEntry, 128)
		s.used = 0
		s.mu.Unlock()
	}
}

// add stores hunks under k and returns the slice that callers must hand out
// to consumers.
//
// Lines produced by computeAddedHunks are zero-copy views (btostr) into the
// entire decompressed new blob, so anything holding one of those hunks keeps
// that whole blob alive. Compacting into a freshly allocated buffer bounds
// retention by the line bytes actually carried, which is what keeps every
// in-flight HunkAddition from pinning one distinct blob apiece.
//
// The accounted size is text bytes plus the per-line string headers plus the
// per-hunk and per-entry overheads. The header term is not a rounding
// allowance: a hunk's Lines slice costs pairCacheLineOverhead per line no
// matter how short the lines are, so a diff of many empty lines retains
// mostly headers and would otherwise be admitted as near-free.
//
// Compaction is skipped when the added lines already cover the whole new
// blob: the copy would then retain the same bytes it aliases while costing a
// memcpy of up to MaxDiffSize. Both qualifying cases follow from
// computeAddedHunks' contract rather than from a heuristic — a zero old OID
// (a file addition) makes every line of the new blob an addition, and a
// binary result carries the whole new blob as its single line. Blob buffers
// are exact-sized allocations on every path that produces one — readRawObject
// and applyDeltaStackCached return a slice whose capacity is the object size,
// and readLooseObject trims the spare capacity io.ReadAll leaves — so aliasing
// one retains precisely the bytes the hunk reports.
//
// Compaction happens before any admission decision, so a rejected or disabled
// cache still returns hunks that own their bytes. Callers deliver the returned
// slice unconditionally; making the copy contingent on admission would let a
// zero budget — the setting chosen to cap memory — pin one whole blob per
// in-flight hunk instead.
func (c *pairCache) add(k pairKey, hunks []AddedHunk) []AddedHunk {
	lineBytes := 0
	lineCount := 0
	for i := range hunks {
		lineCount += len(hunks[i].Lines)
		for _, l := range hunks[i].Lines {
			lineBytes += len(l)
		}
	}
	size := lineBytes + lineCount*pairCacheLineOverhead +
		len(hunks)*pairCacheHunkOverhead + pairCacheEntryOverhead

	stored := hunks
	if !coversWholeNewBlob(&k, hunks) {
		stored = compactHunks(hunks, lineBytes)
	}

	// A disabled cache stores nothing. One giant diff must not evict a whole
	// shard, so it is not stored either — but both are still compacted above
	// and returned: the largest diffs are exactly the ones whose aliasing
	// views pin the most memory per delivered hunk. size is at least
	// pairCacheEntryOverhead, so a zero budget rejects every entry here even
	// before the explicit guard, which keeps empty-hunk transitions (nil
	// results from deletions and mode-only changes) from growing the map
	// uncharged.
	if c.budgetPerShard <= 0 || size > c.budgetPerShard/4 {
		return stored
	}

	s := c.shard(&k)
	s.mu.Lock()
	if old, ok := s.m[k]; ok {
		s.used -= old.size
	}
	s.m[k] = pairCacheEntry{hunks: stored, size: size}
	s.used += size
	if s.used > c.budgetPerShard {
		for key, v := range s.m {
			if key == k {
				continue
			}
			delete(s.m, key)
			s.used -= v.size
			if s.used <= c.budgetPerShard {
				break
			}
		}
	}
	s.mu.Unlock()
	return stored
}

// coversWholeNewBlob reports whether hunks already carry (essentially) every
// byte of the new blob their lines view, which makes compaction a copy that
// retains what it aliases.
//
// A zero old OID marks a file addition, whose added lines are the whole new
// blob minus its line terminators; a binary result is a single hunk holding
// the new blob verbatim.
func coversWholeNewBlob(k *pairKey, hunks []AddedHunk) bool {
	if Hash(k[:hashSize]).IsZero() {
		return true
	}
	return len(hunks) == 1 && hunks[0].IsBinary
}

// compactHunks deep-copies hunks so that no line aliases its source blob.
// All line bytes share one backing buffer sized to lineBytes, so the copy
// costs a single allocation for the text plus the slice headers.
func compactHunks(hunks []AddedHunk, lineBytes int) []AddedHunk {
	if len(hunks) == 0 {
		return nil
	}
	// buf never grows past its capacity, so string views into it stay valid.
	buf := make([]byte, 0, lineBytes)
	out := make([]AddedHunk, len(hunks))
	for i := range hunks {
		src := &hunks[i]
		lines := make([]string, len(src.Lines))
		for j, l := range src.Lines {
			if len(l) == 0 {
				continue
			}
			start := len(buf)
			buf = append(buf, l...)
			lines[j] = btostr(buf[start:])
		}
		out[i] = AddedHunk{
			Lines:     lines,
			StartLine: src.StartLine,
			IsBinary:  src.IsBinary,
		}
	}
	return out
}
