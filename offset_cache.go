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
	"cmp"
	"os"
	"slices"
	"strconv"
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

// offsetCacheDefaultBudget is defaultOffsetCacheBudget unless overridden by
// the GOGITPACK_OFFSET_CACHE_BUDGET environment variable (bytes; a value
// <= 0 disables the cache). The override lets operators bound or disable
// every store's cache fleet-wide without a code change in the embedding
// application: each open store retains up to this budget until Close, so
// processes opening many repositories under fixed memory limits need a
// no-rebuild control. Read once at process start; malformed values fall
// back to the compiled default.
var offsetCacheDefaultBudget = func() int {
	if v := os.Getenv("GOGITPACK_OFFSET_CACHE_BUDGET"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultOffsetCacheBudget
}()

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
	c := &offsetCache{}
	for i := range c.shards {
		c.shards[i].m = make(map[offCacheKey]cachedObj, 256)
	}
	// Route through setBudget so the environment override shares the
	// exact rounding and disable semantics of WithOffsetCacheBudget.
	c.setBudget(offsetCacheDefaultBudget)
	return c
}

// setBudget adjusts the total byte budget across all shards. A budget <= 0
// disables the cache: existing entries are dropped and later adds become
// no-ops (gets simply miss). budgetPerShard is written without
// synchronization, so setBudget must run before the cache is visible to
// concurrent readers and writers — WithOffsetCacheBudget satisfies this by
// running during store construction. Concurrent callers must synchronize
// externally.
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
// Accounting uses len(data) and relies on every producer passing an
// exactly-sized allocation (len == cap): readRawObject, allocExact, and the
// detach copies in applyDeltaStackCached all allocate exact. A producer
// passing a trimmed slice with excess capacity would silently under-account
// the bytes actually retained.
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

// Whale objects: the largest pack entries, which the offset cache refuses
// (maxCacheableSize) and which inflate in tens of milliseconds each. A
// history walk discovers them in commit order, so when one surfaces near the
// end of the walk a single worker inflates it while every other worker has
// run out of work, and that inflation becomes the scan's tail. The prefetch
// below starts those inflations at scan start, biggest first, so they overlap
// with the rest of the walk, and parks the results where store.get finds
// them on the miss path.

// whaleMinCompressedBytes is the compressed pack-entry size at which an
// object is prefetched. An entry of this size holds several MiB of output
// and inflates in milliseconds, which is the scale at which a late discovery
// is visible in the scan's wall time.
const whaleMinCompressedBytes = 2 << 20

// whalePrefetchBudget bounds the inflated bytes the prefetch retains at once.
// Candidates beyond the budget are left to the normal miss path.
const whalePrefetchBudget = 256 << 20

// whaleCandidate names one pack entry selected for prefetch.
type whaleCandidate struct {
	pack       *mmap.ReaderAt
	off        uint64
	compressed uint64
}

// whaleEntry is one prefetched object. done closes once data and typ are
// final; a reader that arrives first waits on it rather than inflating the
// same object a second time.
type whaleEntry struct {
	done chan struct{}
	data []byte
	typ  ObjectType
	err  error
}

// whaleCache holds prefetched whale objects for the duration of one scan.
type whaleCache struct {
	mu sync.Mutex
	m  map[offCacheKey]*whaleEntry
}

// get returns the prefetched object at (pack, off), waiting for an inflation
// in progress. stop aborts the wait.
func (c *whaleCache) get(pack *mmap.ReaderAt, off uint64, stop <-chan struct{}) ([]byte, ObjectType, bool) {
	if c == nil {
		return nil, ObjBad, false
	}
	c.mu.Lock()
	e := c.m[offCacheKey{pack, off}]
	c.mu.Unlock()
	if e == nil {
		return nil, ObjBad, false
	}
	select {
	case <-e.done:
	case <-stop:
		return nil, ObjBad, false
	}
	if e.err != nil {
		return nil, ObjBad, false
	}
	return e.data, e.typ, true
}

// whaleCandidates lists the pack entries whose compressed size is at least
// whaleMinCompressedBytes, largest first. Compressed size is the distance to
// the next entry in pack order, so the scan touches only the index.
func (s *store) whaleCandidates() []whaleCandidate {
	var out []whaleCandidate
	for _, f := range s.packs {
		offs := f.sortedOffsets
		if len(offs) == 0 {
			continue
		}
		// The pack ends with a 20-byte trailer checksum.
		end := uint64(f.pack.Len())
		if end >= 20 {
			end -= 20
		}
		for i, off := range offs {
			next := end
			if i+1 < len(offs) {
				next = offs[i+1]
			}
			if next <= off {
				continue
			}
			if size := next - off; size >= whaleMinCompressedBytes {
				out = append(out, whaleCandidate{pack: f.pack, off: off, compressed: size})
			}
		}
	}
	slices.SortFunc(out, func(a, b whaleCandidate) int {
		return cmp.Compare(b.compressed, a.compressed)
	})
	return out
}

// prefetchWhales inflates the pack's whale blobs in the background, largest
// first, until the budget is spent or stop closes. The returned wait function
// blocks until the prefetch goroutine has exited; callers invoke it before
// releasing the cache so no inflation outlives the scan.
func (s *store) prefetchWhales(stop <-chan struct{}) (wait func()) {
	cands := s.whaleCandidates()
	if len(cands) == 0 {
		return func() {}
	}
	c := &whaleCache{m: make(map[offCacheKey]*whaleEntry, len(cands))}
	var budget uint64 = whalePrefetchBudget
	var selected []whaleCandidate
	for _, cand := range cands {
		typ, hdrLen, err := peekObjectType(cand.pack, cand.off)
		if err != nil || typ != ObjBlob || hdrLen <= 0 {
			continue
		}
		var hdr [32]byte
		n, _ := cand.pack.ReadAt(hdr[:], int64(cand.off))
		_, size, _ := parseObjectHeaderUnsafe(hdr[:n])
		if size > budget {
			continue
		}
		budget -= size
		c.m[offCacheKey{cand.pack, cand.off}] = &whaleEntry{done: make(chan struct{})}
		selected = append(selected, cand)
	}
	if len(selected) == 0 {
		return func() {}
	}
	s.whales.Store(c)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, cand := range selected {
			e := c.m[offCacheKey{cand.pack, cand.off}]
			select {
			case <-stop:
				e.err = errScanAborted
				close(e.done)
				continue
			default:
			}
			_, data, err := readRawObject(cand.pack, cand.off)
			if err == nil {
				err = s.verifyPackObjectCRCIfEnabled(cand.pack, cand.off, Hash{})
			}
			e.data, e.typ, e.err = data, ObjBlob, err
			close(e.done)
		}
	}()
	return func() {
		<-done
		s.whales.Store((*whaleCache)(nil))
	}
}
