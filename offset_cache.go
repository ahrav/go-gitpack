// offset_cache.go
//
// Bounded, sharded cache of materialized pack objects keyed by (pack,
// offset) rather than OID.
//
// Why offsets and not OIDs: ofs-delta objects reference their base by a
// backward byte offset inside the same pack, so during delta-chain walk-up
// the base's OID is unknown without a reverse-index lookup. The OID-keyed
// delta window therefore never intercepts intermediate chain hops, and
// measurements on real repositories show ~90% of walk-up hops revisit an
// offset that was already materialized moments earlier (sibling versions of
// a file share long chain tails). Keying by offset lets walkUpDeltaChain
// stop climbing the instant it reaches any previously materialized object.
package objstore

import (
	"sync"

	"golang.org/x/exp/mmap"
)

// offsetCacheShards must be a power of two; offsets are distributed by their
// low bits (pack entries are byte-aligned, so low bits are well mixed).
const offsetCacheShards = 32

// defaultOffsetCacheBudget bounds the total bytes retained across all shards.
const defaultOffsetCacheBudget = 256 << 20

// offCacheKey identifies a pack-local byte offset. The pack pointer
// disambiguates offsets across multiple mapped packfiles.
type offCacheKey struct {
	pack *mmap.ReaderAt
	off  uint64
}

type offsetCacheShard struct {
	mu   sync.Mutex
	m    map[offCacheKey]cachedObj
	used int
}

// offsetCache is safe for concurrent use. Eviction is approximate: when a
// shard exceeds its budget slice, arbitrary entries (Go map iteration order)
// are dropped until it fits. Scan workloads touch each chain tail in tight
// temporal clusters, so precise LRU buys little over random replacement here.
type offsetCache struct {
	shards         [offsetCacheShards]offsetCacheShard
	budgetPerShard int
}

func newOffsetCache() *offsetCache {
	return newOffsetCacheWithBudget(defaultOffsetCacheBudget)
}

func newOffsetCacheWithBudget(budget int) *offsetCache {
	c := &offsetCache{}
	c.setBudget(budget)
	for i := range c.shards {
		c.shards[i].m = make(map[offCacheKey]cachedObj, 256)
	}
	return c
}

func (c *offsetCache) setBudget(budget int) {
	if c == nil {
		return
	}
	if budget < 0 {
		budget = 0
	}
	c.budgetPerShard = budget / offsetCacheShards
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		if s.m != nil {
			for key, v := range s.m {
				if s.used <= c.budgetPerShard {
					break
				}
				delete(s.m, key)
				s.used -= len(v.data)
			}
		}
		s.mu.Unlock()
	}
}

func (c *offsetCache) shard(off uint64) *offsetCacheShard {
	return &c.shards[off&(offsetCacheShards-1)]
}

// get returns the materialized object stored at (pack, off), if present.
// The returned slice is shared and MUST NOT be mutated.
// A nil receiver reports a miss so contexts without a store can share code.
func (c *offsetCache) get(pack *mmap.ReaderAt, off uint64) ([]byte, ObjectType, bool) {
	if c == nil {
		return nil, ObjBad, false
	}
	s := c.shard(off)
	s.mu.Lock()
	obj, ok := s.m[offCacheKey{pack, off}]
	s.mu.Unlock()
	if !ok {
		return nil, ObjBad, false
	}
	return obj.data, obj.typ, true
}

// add stores a materialized object under (pack, off). The cache takes shared
// ownership of data; callers must treat it as immutable afterwards.
func (c *offsetCache) add(pack *mmap.ReaderAt, off uint64, data []byte, typ ObjectType) {
	if !c.canAdd(len(data)) {
		return
	}
	s := c.shard(off)
	key := offCacheKey{pack, off}
	s.mu.Lock()
	if old, ok := s.m[key]; ok {
		s.used -= len(old.data)
	}
	s.m[key] = cachedObj{data: data, typ: typ}
	s.used += len(data)
	if s.used > c.budgetPerShard {
		for k, v := range s.m {
			if k == key {
				continue
			}
			delete(s.m, k)
			s.used -= len(v.data)
			if s.used <= c.budgetPerShard {
				break
			}
		}
	}
	s.mu.Unlock()
}

func (c *offsetCache) canAdd(size int) bool {
	return c != nil && c.budgetPerShard > 0 && size <= maxCacheableSize && size <= c.budgetPerShard
}
