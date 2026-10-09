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
	"math/bits"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

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

// offsetCacheShardData holds one shard's state. It is embedded in
// offsetCacheShard so the padding there can be derived from its size on
// every architecture.
type offsetCacheShardData struct {
	mu   sync.Mutex
	m    offTable
	used int
}

type offsetCacheShard struct {
	offsetCacheShardData

	// _ pads each shard to 128 bytes: two cache lines, which also covers the
	// adjacent-line prefetcher. Without padding, shards share 64-byte lines,
	// so mutex traffic on distinct shards bounces the same lines and defeats
	// the contention reduction the sharding exists to provide. The array
	// length is a compile-time constant on every target, and a data struct
	// larger than 128 bytes fails to compile here.
	// TestOffsetCacheShardFillsWholeCacheLines pins the total at 128.
	_ [128 - unsafe.Sizeof(offsetCacheShardData{})]byte
}

// offTable is an open-addressing hash table from pack offset to cached
// object, specialized for this cache: one probe reads the key and the value
// from adjacent arrays, so a lookup costs about one cache miss where the
// generic map's control groups and 48-byte slots cost two or three. perf
// attributed 4% of scan cycles to map probes at IPC 0.14 before this table.
//
// keys holds off+1 so zero marks an empty slot. Two packs may share an
// offset, so a key match also compares the pack handle and a mismatch is
// treated as a collision. Deletion uses backward-shift so no tombstones
// accumulate; the load factor stays at or below one half.
type offTable struct {
	keys []uint64
	vals []offValue
	n    int
	rng  uint64
}

type offValue struct {
	pack *mmap.ReaderAt
	data []byte
	typ  ObjectType
}

const offTableMinSlots = 256

func (t *offTable) slot(off uint64) int {
	return int((off * 0x9E3779B97F4A7C15) >> (64 - bits.Len(uint(len(t.keys))-1)))
}

func (t *offTable) find(pack *mmap.ReaderAt, off uint64) int {
	if len(t.keys) == 0 {
		return -1
	}
	mask := len(t.keys) - 1
	i := t.slot(off)
	for {
		k := t.keys[i]
		if k == 0 {
			return -1
		}
		if k == off+1 && t.vals[i].pack == pack {
			return i
		}
		i = (i + 1) & mask
	}
}

// get returns the cached object at (pack, off).
func (t *offTable) get(pack *mmap.ReaderAt, off uint64) (offValue, bool) {
	i := t.find(pack, off)
	if i < 0 {
		return offValue{}, false
	}
	return t.vals[i], true
}

// put inserts or replaces (pack, off) and returns the replaced value.
func (t *offTable) put(pack *mmap.ReaderAt, off uint64, v offValue) (old offValue, replaced bool) {
	if i := t.find(pack, off); i >= 0 {
		old = t.vals[i]
		t.vals[i] = v
		return old, true
	}
	if 2*(t.n+1) > len(t.keys) {
		t.grow()
	}
	mask := len(t.keys) - 1
	i := t.slot(off)
	for t.keys[i] != 0 {
		i = (i + 1) & mask
	}
	t.keys[i] = off + 1
	t.vals[i] = v
	t.n++
	return offValue{}, false
}

func (t *offTable) grow() {
	size := offTableMinSlots
	for size < 4*(t.n+1) {
		size <<= 1
	}
	oldKeys, oldVals := t.keys, t.vals
	t.keys = make([]uint64, size)
	t.vals = make([]offValue, size)
	mask := size - 1
	for j, k := range oldKeys {
		if k == 0 {
			continue
		}
		i := t.slot(k - 1)
		for t.keys[i] != 0 {
			i = (i + 1) & mask
		}
		t.keys[i] = k
		t.vals[i] = oldVals[j]
	}
}

// remove deletes slot i with backward shift, keeping every probe chain
// intact.
func (t *offTable) remove(i int) {
	mask := len(t.keys) - 1
	j := i
	for {
		j = (j + 1) & mask
		k := t.keys[j]
		if k == 0 {
			break
		}
		home := t.slot(k - 1)
		// Slot j may move to i when its home position does not lie in the
		// cyclic range (i, j].
		if (i <= j && (home <= i || home > j)) || (i > j && home <= i && home > j) {
			t.keys[i] = k
			t.vals[i] = t.vals[j]
			i = j
		}
	}
	t.keys[i] = 0
	t.vals[i] = offValue{}
	t.n--
}

// evictOne removes an arbitrary entry other than keep and returns it.
// Victim choice is pseudo-random, which the cache's approximate-replacement
// contract already promises; random replacement measured better here than
// FIFO or CLOCK because the walk's repeats are long-range.
func (t *offTable) evictOne(keepPack *mmap.ReaderAt, keepOff uint64) (offValue, bool) {
	if t.n == 0 || (t.n == 1 && t.find(keepPack, keepOff) >= 0) {
		return offValue{}, false
	}
	mask := len(t.keys) - 1
	t.rng ^= t.rng << 13
	t.rng ^= t.rng >> 7
	t.rng ^= t.rng << 17
	i := int(t.rng) & mask
	for {
		if k := t.keys[i]; k != 0 && !(k == keepOff+1 && t.vals[i].pack == keepPack) {
			v := t.vals[i]
			t.remove(i)
			return v, true
		}
		i = (i + 1) & mask
	}
}

func (t *offTable) reset() {
	t.keys, t.vals, t.n = nil, nil, 0
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
		c.shards[i].m.rng = 0x9E3779B97F4A7C15 ^ uint64(i+1)
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
// concurrent readers and writers: WithOffsetCacheBudget satisfies this by
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
		s.m.reset()
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
	v, ok := s.m.get(pack, off)
	s.mu.Unlock()
	if !ok {
		return nil, ObjBad, false
	}
	return v.data, v.typ, true
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
	s.mu.Lock()
	if old, replaced := s.m.put(pack, off, offValue{pack: pack, data: data, typ: typ}); replaced {
		s.used -= len(old.data)
	}
	s.used += len(data)
	for s.used > c.budgetPerShard {
		v, ok := s.m.evictOne(pack, off)
		if !ok {
			break
		}
		s.used -= len(v.data)
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

// whaleEntry is one prefetched object. Whoever sets claimed first, the
// prefetch goroutine or a reader, inflates the entry; done closes once data,
// typ, and err are final. A reader therefore waits only on an inflation that
// is already running and inflates an unstarted entry itself instead of
// queueing behind the larger whales the prefetch handles first.
type whaleEntry struct {
	claimed atomic.Bool
	done    chan struct{}
	data    []byte
	typ     ObjectType
	err     error
}

// whaleCache holds prefetched whale objects for the duration of one scan. m
// is complete before the cache is published through store.whales and is
// read-only afterwards, so lookups take no lock.
type whaleCache struct {
	m map[offCacheKey]*whaleEntry
	// order lists m's keys in prefetch order, largest first.
	order []offCacheKey
	// load materializes one whale.
	load func(pack *mmap.ReaderAt, off uint64) ([]byte, error)
}

// get returns the prefetched object at (pack, off). It inflates an entry no
// one has claimed and waits for one in progress; stop aborts the wait.
func (c *whaleCache) get(pack *mmap.ReaderAt, off uint64, stop <-chan struct{}) ([]byte, ObjectType, bool) {
	if c == nil {
		return nil, ObjBad, false
	}
	key := offCacheKey{pack, off}
	e := c.m[key]
	if e == nil {
		return nil, ObjBad, false
	}
	if e.claimed.CompareAndSwap(false, true) {
		c.fill(key, e)
	} else {
		select {
		case <-e.done:
		case <-stop:
			return nil, ObjBad, false
		}
	}
	if e.err != nil {
		return nil, ObjBad, false
	}
	return e.data, e.typ, true
}

// run inflates, in prefetch order, every entry no reader has claimed. Once
// stop closes, the remaining unclaimed entries are marked failed so their
// readers fall back to the normal miss path.
func (c *whaleCache) run(stop <-chan struct{}) {
	for _, key := range c.order {
		e := c.m[key]
		if !e.claimed.CompareAndSwap(false, true) {
			continue
		}
		select {
		case <-stop:
			e.err = errScanAborted
			close(e.done)
			continue
		default:
		}
		c.fill(key, e)
	}
}

// fill inflates e and publishes the result; the caller holds e's claim.
func (c *whaleCache) fill(key offCacheKey, e *whaleEntry) {
	e.data, e.err = c.load(key.pack, key.off)
	e.typ = ObjBlob
	close(e.done)
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

// whalePrefetch coordinates the scans that share store.whales. The first
// scan publishes the cache and starts the prefetch goroutine, concurrent
// scans join it, and the last scan to finish stops the goroutine and clears
// the cache. mu guards every field.
type whalePrefetch struct {
	mu   sync.Mutex
	refs int
	// stop closes when the last scan releases the cache.
	stop chan struct{}
	// done closes when the prefetch goroutine has exited.
	done chan struct{}
}

// prefetchWhales starts, or joins, the store's background whale prefetch.
// The returned wait function releases this scan's share once, so repeated
// calls are no-ops; the last release stops the goroutine and blocks until
// it has exited. A disabled offset cache skips the prefetch.
func (s *store) prefetchWhales() (wait func()) {
	if !s.offCache.enabled() {
		return func() {}
	}
	p := &s.whalePrefetch
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.refs > 0 {
		p.refs++
		return sync.OnceFunc(s.releaseWhales)
	}
	c := s.newWhaleCache()
	if c == nil {
		return func() {}
	}
	p.refs = 1
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	s.whales.Store(c)
	go func(stop, done chan struct{}) {
		defer close(done)
		c.run(stop)
	}(p.stop, p.done)
	return sync.OnceFunc(s.releaseWhales)
}

// releaseWhales drops one scan's share of the whale cache. The last release
// stops the prefetch goroutine, waits for it to exit, and clears store.whales
// while holding the lock, so a scan starting in that window publishes its
// cache only after the old one is gone.
func (s *store) releaseWhales() {
	p := &s.whalePrefetch
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refs--
	if p.refs > 0 {
		return
	}
	close(p.stop)
	<-p.done
	p.stop, p.done = nil, nil
	s.whales.Store((*whaleCache)(nil))
}

func (s *store) newWhaleCache() *whaleCache {
	cands := s.whaleCandidates()
	if len(cands) == 0 {
		return nil
	}
	c := &whaleCache{
		m: make(map[offCacheKey]*whaleEntry, len(cands)),
		load: func(pack *mmap.ReaderAt, off uint64) ([]byte, error) {
			_, data, err := readRawObject(pack, off)
			if err == nil {
				err = s.verifyPackObjectCRCIfEnabled(pack, off, Hash{})
			}
			return data, err
		},
	}
	var budget uint64 = whalePrefetchBudget
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
		key := offCacheKey{cand.pack, cand.off}
		c.m[key] = &whaleEntry{done: make(chan struct{})}
		c.order = append(c.order, key)
	}
	if len(c.order) == 0 {
		return nil
	}
	return c
}
