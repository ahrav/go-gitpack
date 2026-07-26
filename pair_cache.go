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

// pairCacheBudget bounds the bytes retained across all shards. Entries are
// compacted copies (see add) and every component of an entry's footprint —
// text bytes, per-line string headers, per-hunk and per-entry overhead — is
// charged, so the accounted size tracks actual retention.
const pairCacheBudget = 128 << 20

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
// offsetCache.
type pairCache struct {
	shards         [pairCacheShards]pairCacheShard
	budgetPerShard int
}

func newPairCache() *pairCache {
	c := &pairCache{budgetPerShard: pairCacheBudget / pairCacheShards}
	for i := range c.shards {
		c.shards[i].m = make(map[pairKey]pairCacheEntry, 128)
	}
	return c
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

	// One giant diff must not evict a whole shard, so it is not stored — but
	// it is still compacted above and returned: the largest diffs are exactly
	// the ones whose aliasing views pin the most memory per delivered hunk.
	if size > c.budgetPerShard/4 {
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
