// pair_cache_test.go verifies the pairCache memo used by the hunk-diff
// pipeline: entries must not alias caller-owned blob buffers (so the byte
// budget reflects real retention), shard selection must spread structurally
// zero OIDs, empty results must still be charged to the budget, and eviction
// must keep every shard within its budget.
package objstore

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// testPairHash returns a Hash whose first byte is b, for crafting keys.
func testPairHash(b byte) Hash {
	var h Hash
	h[0] = b
	h[hashSize-1] = 1 // never the zero Hash
	return h
}

// TestPairCacheAdd_CopiesLineData proves that cached hunk lines do not alias
// the source buffer they were tokenized from. computeAddedHunks produces
// lines via btostr views into the whole decompressed blob; caching those
// views verbatim would pin one full blob per entry while the budget saw only
// line lengths.
func TestPairCacheAdd_CopiesLineData(t *testing.T) {
	backing := make([]byte, 1<<20)
	for i := range backing {
		backing[i] = byte('a' + i%26)
	}

	// Simulate tokenize: short zero-copy views into a large blob buffer.
	lineA := btostr(backing[100:120])
	lineB := btostr(backing[5000:5040])
	hunks := []AddedHunk{{Lines: []string{lineA, "", lineB}, StartLine: 3}}

	c := newPairCache()
	k := makePairKey(testPairHash(1), testPairHash(2))
	c.add(k, hunks)

	got, ok := c.get(k)
	require.True(t, ok)
	require.Len(t, got, 1)
	require.Equal(t, []string{lineA, "", lineB}, got[0].Lines)
	require.Equal(t, uint32(3), got[0].StartLine)

	base := uintptr(unsafe.Pointer(unsafe.SliceData(backing)))
	end := base + uintptr(len(backing))
	for _, h := range got {
		for i, l := range h.Lines {
			if len(l) == 0 {
				continue
			}
			p := uintptr(unsafe.Pointer(unsafe.StringData(l)))
			require.False(t, p >= base && p < end,
				"cached line %d aliases the source blob buffer; entries must own their bytes", i)
		}
	}
	runtime.KeepAlive(backing)
}

// TestPairCacheShard_SpreadsZeroOldOID proves shard selection does not
// collapse when the old OID is the zero Hash. Every file addition carries a
// zero old OID (walkDiff's handleAdd), so a shard function keyed only on the
// old OID's bytes would funnel the entire addition class into one shard,
// serializing all workers on one mutex and one budget slice.
func TestPairCacheShard_SpreadsZeroOldOID(t *testing.T) {
	c := newPairCache()

	shards := make(map[*pairCacheShard]struct{})
	for i := range 64 {
		newOID := testPairHash(byte(i))
		k := makePairKey(Hash{}, newOID)
		shards[c.shard(&k)] = struct{}{}
	}
	require.Greater(t, len(shards), 1,
		"all zero-old-OID (addition) keys map to a single shard")
}

// TestPairCacheAdd_EmptyResultCountsTowardBudget proves that entries with no
// hunk lines still charge the shard budget. A zero-cost entry is invisible
// to eviction, so distinct empty results (files whose diff adds no lines)
// would grow the map without bound.
func TestPairCacheAdd_EmptyResultCountsTowardBudget(t *testing.T) {
	c := newPairCache()
	k := makePairKey(testPairHash(7), testPairHash(9))
	c.add(k, nil)

	_, ok := c.get(k)
	require.True(t, ok, "empty results are cacheable")

	s := c.shard(&k)
	s.mu.Lock()
	used := s.used
	s.mu.Unlock()
	require.Positive(t, used,
		"an empty entry must be charged to the budget so eviction can see it")
}

// TestPairCacheAdd_EvictionKeepsShardWithinBudget proves the shard budget is
// enforced: inserting more bytes than one shard's budget evicts older
// entries rather than growing without bound.
func TestPairCacheAdd_EvictionKeepsShardWithinBudget(t *testing.T) {
	c := &pairCache{budgetPerShard: 8 << 10}
	for i := range c.shards {
		c.shards[i].m = make(map[pairKey]pairCacheEntry, 8)
	}

	line := string(make([]byte, 512))
	for i := range 200 {
		var oldOID, newOID Hash
		oldOID[1] = byte(i)
		newOID[1] = byte(i >> 8)
		newOID[2] = byte(i)
		k := makePairKey(oldOID, newOID)
		c.add(k, []AddedHunk{{Lines: []string{line}, StartLine: 1}})
	}

	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		used := s.used
		s.mu.Unlock()
		require.LessOrEqual(t, used, c.budgetPerShard,
			"shard %d exceeds its byte budget", i)
	}
}

// TestPairCacheAdd_OversizedEntryRejected proves a single giant diff is not
// admitted: caching it would evict an entire shard for one entry.
func TestPairCacheAdd_OversizedEntryRejected(t *testing.T) {
	c := &pairCache{budgetPerShard: 4 << 10}
	for i := range c.shards {
		c.shards[i].m = make(map[pairKey]pairCacheEntry, 8)
	}

	huge := string(make([]byte, 2<<10))
	k := makePairKey(testPairHash(3), testPairHash(4))
	c.add(k, []AddedHunk{{Lines: []string{huge}, StartLine: 1}})

	_, ok := c.get(k)
	require.False(t, ok, "entries larger than budgetPerShard/4 must be rejected")
}
