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
	"strings"
	"sync"
)

// pairCacheShards spreads pair lookups across independent locks.
const pairCacheShards = 32

// defaultPairCacheBudget bounds retained hunk bytes (approximate, line lengths).
const defaultPairCacheBudget = 128 << 20

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
	return newPairCacheWithBudget(defaultPairCacheBudget)
}

func newPairCacheWithBudget(budget int) *pairCache {
	c := &pairCache{}
	c.setBudget(budget)
	for i := range c.shards {
		c.shards[i].m = make(map[pairKey]pairCacheEntry, 128)
	}
	return c
}

func (c *pairCache) setBudget(budget int) {
	if budget < 0 {
		budget = 0
	}
	c.budgetPerShard = budget / pairCacheShards
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		if s.m != nil {
			for key, v := range s.m {
				if s.used <= c.budgetPerShard {
					break
				}
				delete(s.m, key)
				s.used -= v.size
			}
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
	return &c.shards[int(k[0])&(pairCacheShards-1)]
}

func (c *pairCache) get(k pairKey) ([]AddedHunk, bool) {
	s := c.shard(&k)
	s.mu.Lock()
	e, ok := s.m[k]
	s.mu.Unlock()
	return e.hunks, ok
}

func (c *pairCache) add(k pairKey, hunks []AddedHunk) {
	if c.budgetPerShard <= 0 {
		return
	}
	size := estimateAddedHunksSize(hunks)
	if size > c.budgetPerShard/4 {
		return // one giant diff must not evict a whole shard
	}
	cached := cloneAddedHunksForCache(hunks)
	s := c.shard(&k)
	s.mu.Lock()
	if old, ok := s.m[k]; ok {
		s.used -= old.size
	}
	s.m[k] = pairCacheEntry{hunks: cached, size: size}
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

func estimateAddedHunksSize(hunks []AddedHunk) int {
	size := 96 // map entry, key, and slice header overhead, approximate
	for i := range hunks {
		for _, l := range hunks[i].Lines {
			size += len(l)
		}
		size += 64 + len(hunks[i].Lines)*16 // struct + slice/string headers, approximate
	}
	return size
}

func cloneAddedHunksForCache(hunks []AddedHunk) []AddedHunk {
	if len(hunks) == 0 {
		return nil
	}
	totalLineBytes := 0
	totalLines := 0
	for i := range hunks {
		totalLines += len(hunks[i].Lines)
		for _, line := range hunks[i].Lines {
			totalLineBytes += len(line)
		}
	}
	starts := make([]int, 0, totalLines)
	var packed strings.Builder
	packed.Grow(totalLineBytes)
	for i := range hunks {
		for _, line := range hunks[i].Lines {
			start := packed.Len()
			packed.WriteString(line)
			starts = append(starts, start)
		}
	}
	backing := packed.String()

	cached := make([]AddedHunk, len(hunks))
	startIdx := 0
	for i := range hunks {
		cached[i] = hunks[i]
		if len(hunks[i].Lines) == 0 {
			continue
		}
		lines := make([]string, len(hunks[i].Lines))
		for j := range hunks[i].Lines {
			start := starts[startIdx]
			lines[j] = backing[start : start+len(hunks[i].Lines[j])]
			startIdx++
		}
		cached[i].Lines = lines
	}
	return cached
}
