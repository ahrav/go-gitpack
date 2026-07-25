// pair_cache_test.go verifies the pairCache memo used by the hunk-diff
// pipeline: entries must not alias caller-owned blob buffers (so the byte
// budget reflects real retention), what add returns must be safe to deliver to
// consumers, shard selection must spread structurally zero OIDs, empty results
// must still be charged to the budget, and eviction must keep every shard
// within its budget.
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

// aliasesBuffer reports whether s views memory inside buf. Empty strings carry
// no data pointer worth checking.
func aliasesBuffer(s string, buf []byte) bool {
	if len(s) == 0 || len(buf) == 0 {
		return false
	}
	base := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
	p := uintptr(unsafe.Pointer(unsafe.StringData(s)))
	return p >= base && p < base+uintptr(len(buf))
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

	for _, h := range got {
		for i, l := range h.Lines {
			require.False(t, aliasesBuffer(l, backing),
				"cached line %d aliases the source blob buffer; entries must own their bytes", i)
		}
	}
	runtime.KeepAlive(backing)
}

// TestPairCacheAdd_ReturnsStoredHunks proves add hands back exactly the slice
// it stored. Callers deliver the returned slice to consumers, so this is what
// keeps an in-flight hunk from retaining the whole decompressed blob its lines
// were tokenized from.
func TestPairCacheAdd_ReturnsStoredHunks(t *testing.T) {
	backing := make([]byte, 1<<20)
	for i := range backing {
		backing[i] = byte('a' + i%26)
	}
	hunks := []AddedHunk{{Lines: []string{btostr(backing[64:96])}, StartLine: 1}}

	c := newPairCache()
	k := makePairKey(testPairHash(1), testPairHash(2))
	returned := c.add(k, hunks)

	stored, ok := c.get(k)
	require.True(t, ok)
	require.Equal(t, unsafe.SliceData(stored), unsafe.SliceData(returned),
		"add must return the stored slice so deliveries and cache hits share one copy")
	require.False(t, aliasesBuffer(returned[0].Lines[0], backing),
		"returned line aliases the source blob buffer")
	runtime.KeepAlive(backing)
}

// TestPairCacheAdd_OversizedEntryReturnsOwnedLines proves that an entry too
// large to cache is still compacted before delivery. The largest diffs are
// precisely the ones whose aliasing views pin the most memory per delivered
// hunk, so skipping the copy on this path would leave the retention problem in
// place for the worst cases.
func TestPairCacheAdd_OversizedEntryReturnsOwnedLines(t *testing.T) {
	c := &pairCache{budgetPerShard: 4 << 10}
	for i := range c.shards {
		c.shards[i].m = make(map[pairKey]pairCacheEntry, 8)
	}

	backing := make([]byte, 8<<10)
	huge := btostr(backing[:2<<10])
	k := makePairKey(testPairHash(3), testPairHash(4))
	returned := c.add(k, []AddedHunk{{Lines: []string{huge}, StartLine: 1}})

	_, ok := c.get(k)
	require.False(t, ok, "precondition: the entry is too large to be stored")
	require.Len(t, returned, 1)
	require.Equal(t, huge, returned[0].Lines[0])
	require.False(t, aliasesBuffer(returned[0].Lines[0], backing),
		"a rejected entry must still be compacted before it is delivered")
	runtime.KeepAlive(backing)
}

// TestPairCacheAdd_WholeBlobResultSkipsCopy pins the deliberate exemption from
// compaction: when the added lines already span the whole new blob, copying
// them retains the same bytes it aliases and only adds a memcpy of up to
// MaxDiffSize. A file addition (zero old OID) and a binary result are the two
// cases computeAddedHunks guarantees this for.
func TestPairCacheAdd_WholeBlobResultSkipsCopy(t *testing.T) {
	backing := make([]byte, 1<<10)
	for i := range backing {
		backing[i] = byte('a' + i%26)
	}
	whole := btostr(backing)

	tests := []struct {
		name  string
		key   pairKey
		hunks []AddedHunk
	}{
		{
			name:  "FileAddition",
			key:   makePairKey(Hash{}, testPairHash(5)),
			hunks: []AddedHunk{{Lines: []string{whole}, StartLine: 1}},
		},
		{
			name:  "BinaryResult",
			key:   makePairKey(testPairHash(6), testPairHash(7)),
			hunks: []AddedHunk{{Lines: []string{whole}, StartLine: 1, IsBinary: true}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newPairCache()
			returned := c.add(tt.key, tt.hunks)
			require.Len(t, returned, 1)
			require.True(t, aliasesBuffer(returned[0].Lines[0], backing),
				"whole-blob lines must be handed out as-is rather than copied")
		})
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
