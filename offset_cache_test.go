package objstore

import "testing"

// TestOffsetCacheBudgetRejectsOversizedEntries verifies that add honors the
// configured per-shard budget: an entry larger than budgetPerShard must be
// rejected before insertion, because the eviction loop never removes the
// just-added key and would otherwise pin the shard above its budget (up to
// maxCacheableSize per shard regardless of the configured bound).
func TestOffsetCacheBudgetRejectsOversizedEntries(t *testing.T) {
	t.Parallel()

	c := newOffsetCache()
	c.setBudget(offsetCacheShards) // 1 byte per shard

	big := make([]byte, 1024)
	if c.admits(len(big)) {
		t.Fatal("entry larger than the per-shard budget must not be admitted")
	}
	c.add(nil, 0, big, ObjBlob)
	if _, _, ok := c.get(nil, 0); ok {
		t.Fatal("entry larger than the per-shard budget must be rejected")
	}
	if used := c.shards[0].used; used != 0 {
		t.Fatalf("rejected entry must not be accounted: used=%d", used)
	}

	// Entries within the per-shard budget are still admitted.
	c.setBudget(offsetCacheShards << 10) // 1 KiB per shard
	small := make([]byte, 512)
	if !c.admits(len(small)) {
		t.Fatal("entry within the per-shard budget must be admitted")
	}
	c.add(nil, 8, small, ObjBlob)
	if _, _, ok := c.get(nil, 8); !ok {
		t.Fatal("entry within the per-shard budget must be admitted")
	}

	c.setBudget(0)
	if c.enabled() || c.admits(0) {
		t.Fatal("zero budget must disable cache admission")
	}
}

// TestOffsetCacheAddStaysWithinBudget verifies that after any sequence of
// admitted inserts, a shard's accounted bytes never exceed its budget.
func TestOffsetCacheAddStaysWithinBudget(t *testing.T) {
	t.Parallel()

	c := newOffsetCache()
	c.setBudget(offsetCacheShards * 1024) // 1 KiB per shard

	// All offsets map to shard 0 (multiples of offsetCacheShards).
	for i := range 64 {
		entry := make([]byte, 256)
		c.add(nil, uint64(i*offsetCacheShards), entry, ObjBlob)
		if used := c.shards[0].used; used > c.budgetPerShard {
			t.Fatalf("shard exceeded budget after insert %d: used=%d budget=%d",
				i, used, c.budgetPerShard)
		}
	}
}
