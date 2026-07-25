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

import "sync"

// pairCacheShards spreads pair lookups across independent locks.
const pairCacheShards = 32

// pairCacheBudget bounds the bytes retained across all shards. Entries are
// compacted copies (see add), so the accounted size tracks actual retention.
const pairCacheBudget = 128 << 20

// pairCacheHunkOverhead approximates the per-hunk cost beyond line bytes:
// the AddedHunk struct and its Lines slice header.
const pairCacheHunkOverhead = 64

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
// their Lines. Eviction is approximate (map-order) like offsetCache.
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

// add stores a compacted deep copy of hunks under k.
//
// Lines produced by computeAddedHunks are zero-copy views (btostr) into the
// entire decompressed new blob, so storing them verbatim would pin one full
// blob per entry while the budget accounting saw only line lengths.
// Compacting into a freshly allocated buffer makes the accounted size equal
// the retained size and lets the source blob be collected.
func (c *pairCache) add(k pairKey, hunks []AddedHunk) {
	lineBytes := 0
	for i := range hunks {
		for _, l := range hunks[i].Lines {
			lineBytes += len(l)
		}
	}
	size := lineBytes + len(hunks)*pairCacheHunkOverhead + pairCacheEntryOverhead
	if size > c.budgetPerShard/4 {
		return // one giant diff must not evict a whole shard
	}

	stored := compactHunks(hunks, lineBytes)

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
