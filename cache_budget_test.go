// cache_budget_test.go verifies pairCache.setBudget, the knob behind
// WithPairCacheBudget. The offset cache's equivalent is covered in
// offset_cache_test.go; retention, eviction, and accounting of pair entries at
// a fixed budget are covered in pair_cache_test.go. What is unique here is the
// budget transition itself: disabling must drop what is held and stop storing
// while still handing callers hunks that own their bytes, shrinking must evict
// down to the new bound, and a positive budget too small to divide across
// shards must not silently disable the memo.
package objstore

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPairCacheSetBudget_ZeroDisablesButStillCompacts proves a zero budget
// stops storage without changing what callers receive. streamBlobPairHunks
// delivers whatever add returns, so if the copy were contingent on admission
// the setting chosen to cap memory would instead pin one whole decompressed
// blob per in-flight hunk.
func TestPairCacheSetBudget_ZeroDisablesButStillCompacts(t *testing.T) {
	t.Parallel()

	backing := make([]byte, 1<<20)
	for i := range backing {
		backing[i] = byte('a' + i%26)
	}
	line := btostr(backing[100:132])

	c := newPairCache()
	c.setBudget(0)
	require.Zero(t, c.budgetPerShard, "a zero budget must leave no per-shard allowance")

	k := makePairKey(testPairHash(1), testPairHash(2))
	returned := c.add(k, []AddedHunk{{Lines: []string{line}, StartLine: 1}})

	_, ok := c.get(k)
	require.False(t, ok, "a disabled cache must not store the entry")
	require.Len(t, returned, 1)
	require.Equal(t, line, returned[0].Lines[0], "delivered content must be unchanged")
	require.False(t, aliasesBuffer(returned[0].Lines[0], backing),
		"a disabled cache must still compact: the returned line aliases the source blob")
	require.Zero(t, c.shard(&k).used, "a rejected entry must not be accounted")
	runtime.KeepAlive(backing)
}

// TestPairCacheSetBudget_NegativeDisables proves a negative budget is treated
// as disabled rather than producing a negative per-shard allowance, which
// integer division would otherwise yield.
func TestPairCacheSetBudget_NegativeDisables(t *testing.T) {
	t.Parallel()

	c := newPairCache()
	c.setBudget(-1 << 20)
	require.Zero(t, c.budgetPerShard)

	k := makePairKey(testPairHash(3), testPairHash(4))
	c.add(k, []AddedHunk{{Lines: []string{"line"}, StartLine: 1}})
	_, ok := c.get(k)
	require.False(t, ok, "a negative budget must disable storage")
}

// TestPairCacheSetBudget_DisableDropsHeldEntries proves shrinking to zero
// releases what is already retained. Without this a scanner configured down
// after a warm walk would keep the old bytes alive for the rest of its life.
func TestPairCacheSetBudget_DisableDropsHeldEntries(t *testing.T) {
	t.Parallel()

	c := newPairCache()
	k := makePairKey(testPairHash(5), testPairHash(6))
	c.add(k, []AddedHunk{{Lines: []string{"retained"}, StartLine: 1}})
	_, ok := c.get(k)
	require.True(t, ok, "precondition: the entry is stored at the default budget")

	c.setBudget(0)
	_, ok = c.get(k)
	require.False(t, ok, "disabling must drop entries already held")
	for i := range c.shards {
		require.Zero(t, c.shards[i].used, "shard %d retained usage after disable", i)
	}
}

// TestPairCacheSetBudget_TinyPositiveBudgetStaysEnabled proves a positive
// budget smaller than pairCacheShards rounds up to one byte per shard instead
// of dividing to zero. Rounding down would turn a caller's request to shrink
// the memo into a silent request to disable it, and "enabled" and "disabled"
// differ in more than degree: only the latter recomputes every pair.
func TestPairCacheSetBudget_TinyPositiveBudgetStaysEnabled(t *testing.T) {
	t.Parallel()

	c := newPairCache()
	c.setBudget(1)
	require.Equal(t, 1, c.budgetPerShard,
		"a positive budget must not round down to a disabled cache")

	// One byte per shard admits nothing (every entry carries at least
	// pairCacheEntryOverhead), but the memo is on rather than off.
	k := makePairKey(testPairHash(7), testPairHash(8))
	returned := c.add(k, []AddedHunk{{Lines: []string{"line"}, StartLine: 1}})
	require.Len(t, returned, 1)
	_, ok := c.get(k)
	require.False(t, ok, "a 1-byte shard budget cannot hold an entry")
}

// TestPairCacheSetBudget_ShrinkEvictsToNewBound proves a reduction takes effect
// before setBudget returns. Deferring to add is not sound: add rejects entries
// costing more than a quarter of the per-shard budget, so after a large
// reduction that gate can reject every later entry and the eviction loop inside
// add would never run — leaving the shard above its configured bound for the
// life of the cache.
func TestPairCacheSetBudget_ShrinkEvictsToNewBound(t *testing.T) {
	t.Parallel()

	c := newPairCache()
	// All keys land in shard 0: shard selection mixes k[0]^k[hashSize], so
	// equal first bytes on both OIDs select shard 0 regardless of the rest.
	key := func(i int) pairKey {
		return makePairKey(testPairHash(byte(i)), testPairHash(byte(i)))
	}
	k0 := key(0)
	require.Same(t, &c.shards[0], c.shard(&k0), "precondition: keys map to shard 0")

	for i := range 32 {
		c.add(key(i), []AddedHunk{{Lines: []string{"0123456789abcdef"}, StartLine: 1}})
	}
	require.Positive(t, c.shards[0].used, "precondition: shard 0 holds entries")

	const total = pairCacheShards * 512 // 512 bytes per shard
	c.setBudget(total)
	require.Equal(t, 512, c.budgetPerShard, "the new bound must be recorded")
	require.LessOrEqual(t, c.shards[0].used, c.budgetPerShard,
		"shrinking the budget must evict down to the new per-shard bound")

	// Accounting must survive eviction: used has to match what is still held,
	// or a later add would evict against a corrupted total.
	sum := 0
	for _, v := range c.shards[0].m {
		sum += v.size
	}
	require.Equal(t, sum, c.shards[0].used,
		"used must equal the sum of surviving entry sizes after eviction")
}

// TestPairCacheSetBudget_ReEnableStores proves the cache is usable again after
// being disabled, so the map replacement in clear does not leave a nil map that
// a later add would panic on.
func TestPairCacheSetBudget_ReEnableStores(t *testing.T) {
	t.Parallel()

	c := newPairCache()
	c.setBudget(0)
	c.setBudget(defaultPairCacheBudget)

	k := makePairKey(testPairHash(9), testPairHash(10))
	c.add(k, []AddedHunk{{Lines: []string{"line"}, StartLine: 1}})
	got, ok := c.get(k)
	require.True(t, ok, "a re-enabled cache must store again")
	require.Equal(t, "line", got[0].Lines[0])
}

// TestWithPairCacheBudget_AppliesToScanner proves the option reaches the memo
// the scanner actually consults, not a replacement cache left unreferenced.
func TestWithPairCacheBudget_AppliesToScanner(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	hs, err := NewHistoryScanner(dir, WithPairCacheBudget(0))
	require.NoError(t, err)
	defer hs.Close()

	require.NotNil(t, hs.pairs, "the option must not detach the scanner's memo")
	require.Zero(t, hs.pairs.budgetPerShard, "the option must disable the scanner's memo")

	hs2, err := NewHistoryScanner(dir, WithPairCacheBudget(pairCacheShards<<10))
	require.NoError(t, err)
	defer hs2.Close()
	require.Equal(t, 1<<10, hs2.pairs.budgetPerShard)
}
