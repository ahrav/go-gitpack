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

// defaultOffsetCacheBudget bounds the total bytes retained across all shards
// of one store's offset cache. Each open store owns an independent cache, so
// processes that open many stores concurrently should lower the budget via
// WithOffsetCacheBudget to bound aggregate growth.
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

	// _ pads each shard to its own cache line (and covers the adjacent-line
	// prefetcher). Without padding, three 24-byte shards share every 64-byte
	// line, so mutex traffic on distinct shards bounces the same lines and
	// defeats the contention reduction the sharding exists to provide.
	_ [128 - 24]byte
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
	c := &offsetCache{budgetPerShard: defaultOffsetCacheBudget / offsetCacheShards}
	for i := range c.shards {
		c.shards[i].m = make(map[offCacheKey]cachedObj, 256)
	}
	return c
}

// setBudget adjusts the total byte budget across all shards. A budget <= 0
// disables the cache: existing entries are dropped and later adds become
// no-ops (gets simply miss). Safe for concurrent use, though it is intended
// to be called once right after the owning store is opened.
func (c *offsetCache) setBudget(total int) {
	if c == nil {
		return
	}
	per := total / offsetCacheShards
	if total <= 0 {
		per = 0
	} else if per == 0 {
		per = 1
	}
	c.budgetPerShard = per
	if per == 0 {
		c.clear()
	}
}

// clear drops every cached entry, releasing the retained object bytes to the
// GC. The cache remains usable afterwards (unless the budget is zero).
func (c *offsetCache) clear() {
	if c == nil {
		return
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.m = make(map[offCacheKey]cachedObj)
		s.used = 0
		s.mu.Unlock()
	}
}

func (c *offsetCache) shard(off uint64) *offsetCacheShard {
	return &c.shards[off&(offsetCacheShards-1)]
}

func (c *offsetCache) enabled() bool {
	return c != nil && c.budgetPerShard > 0
}

func (c *offsetCache) admits(size int) bool {
	return c.enabled() && size <= maxCacheableSize && size <= c.budgetPerShard
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
//
// Entries larger than the per-shard budget are rejected outright: the
// eviction loop below never removes the just-added key, so admitting one
// would pin the shard above its configured budget indefinitely (up to
// maxCacheableSize × offsetCacheShards process-wide, defeating small
// WithOffsetCacheBudget settings on memory-constrained scanners).
func (c *offsetCache) add(pack *mmap.ReaderAt, off uint64, data []byte, typ ObjectType) {
	if !c.admits(len(data)) {
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
