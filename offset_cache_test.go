package objstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/mmap"
)

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

// TestWhaleCandidatesAndPrefetch builds a repository with one blob whose
// compressed size exceeds whaleMinCompressedBytes and checks that the scan
// prefetch selects it, that store.get serves the prefetched bytes, and that a
// stopped prefetch leaves the normal path intact.
func TestWhaleCandidatesAndPrefetch(t *testing.T) {
	requireGit(t)
	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")

	// Incompressible content so the compressed entry stays above the
	// threshold; a small text file keeps a second, ordinary object around.
	big := make([]byte, whaleMinCompressedBytes+1<<20)
	rng := uint64(0x9E3779B97F4A7C15)
	for i := range big {
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		big[i] = byte(rng)
	}
	require.NoError(t, os.WriteFile(filepath.Join(repo, "big.bin"), big, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "small.txt"), []byte("hello\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add", "--quiet")
	runGit(t, repo, "repack", "-a", "-d", "-q")

	s, err := open(filepath.Join(repo, ".git", "objects", "pack"))
	require.NoError(t, err)
	defer s.Close()

	cands := s.whaleCandidates()
	require.Len(t, cands, 1, "exactly one entry exceeds the compressed threshold")
	require.GreaterOrEqual(t, cands[0].compressed, uint64(whaleMinCompressedBytes))

	bigOID := calculateHash(ObjBlob, big)
	pack, off, ok := s.findPackedObject(bigOID)
	require.True(t, ok)
	require.Equal(t, cands[0].off, off)
	require.Same(t, cands[0].pack, pack)

	stop := make(chan struct{})
	wait := s.prefetchWhales(stop)
	w := s.whales.Load()
	require.NotNil(t, w, "prefetch registers a whale cache")
	data, typ, found := w.get(pack, off, nil)
	require.True(t, found)
	require.Equal(t, ObjBlob, typ)
	require.Equal(t, big, data)

	// store.get hands out the prefetched bytes without a second inflation.
	got, typ, err := s.get(bigOID)
	require.NoError(t, err)
	require.Equal(t, ObjBlob, typ)
	require.Equal(t, big, got)
	wait()
	require.Nil(t, s.whales.Load(), "wait releases the whale cache")

	// A stop before the prefetch runs marks the entry failed; store.get then
	// materializes normally.
	close(stop)
	wait = s.prefetchWhales(stop)
	wait()
	got, _, err = s.get(bigOID)
	require.NoError(t, err)
	require.Equal(t, big, got)
}

func TestWhaleCandidatesEmptyForSmallPacks(t *testing.T) {
	requireGit(t)
	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "a.txt"), []byte("a\n"), 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "add", "--quiet")
	runGit(t, repo, "repack", "-a", "-d", "-q")

	s, err := open(filepath.Join(repo, ".git", "objects", "pack"))
	require.NoError(t, err)
	defer s.Close()
	require.Empty(t, s.whaleCandidates())
	wait := s.prefetchWhales(make(chan struct{}))
	wait()
	require.Nil(t, s.whales.Load())
}

// TestOffTableMatchesMapOracle drives the open-addressing table with random
// puts, gets, and evictions across growth and backward-shift deletions and
// checks every answer against a plain map.
func TestOffTableMatchesMapOracle(t *testing.T) {
	t.Parallel()
	packA := &mmap.ReaderAt{}
	packB := &mmap.ReaderAt{}
	var tbl offTable
	tbl.rng = 0x1234567
	oracle := map[offCacheKey][]byte{}
	rng := uint64(42)
	next := func() uint64 {
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		return rng
	}
	for step := 0; step < 200000; step++ {
		pack := packA
		if next()%3 == 0 {
			pack = packB
		}
		off := next() % 4096 // dense offsets force collisions and shared offsets across packs
		key := offCacheKey{pack, off}
		switch next() % 4 {
		case 0, 1:
			data := []byte{byte(off), byte(step)}
			_, replaced := tbl.put(pack, off, offValue{pack: pack, data: data, typ: ObjBlob})
			_, had := oracle[key]
			require.Equal(t, had, replaced, "step %d replace flag", step)
			oracle[key] = data
		case 2:
			v, ok := tbl.get(pack, off)
			want, had := oracle[key]
			require.Equal(t, had, ok, "step %d presence", step)
			if ok {
				require.Equal(t, want, v.data, "step %d value", step)
				require.Same(t, pack, v.pack)
			}
		case 3:
			if len(oracle) == 0 {
				continue
			}
			v, ok := tbl.evictOne(pack, off)
			if !ok {
				// Only the protected key remains.
				require.LessOrEqual(t, len(oracle), 1)
				continue
			}
			victim := offCacheKey{v.pack, 0}
			found := false
			for k, d := range oracle {
				if k.pack == v.pack && string(d) == string(v.data) {
					if got, ok := tbl.get(k.pack, k.off); !ok || string(got.data) != string(d) {
						victim = k
						found = true
						break
					}
				}
			}
			require.True(t, found, "step %d: evicted value must have been present and now absent", step)
			require.False(t, victim == key, "step %d: the protected key must not be evicted", step)
			delete(oracle, victim)
		}
		require.Equal(t, len(oracle), tbl.n, "step %d count", step)
	}
	// Every surviving key is still reachable after all the shifting.
	for k, d := range oracle {
		v, ok := tbl.get(k.pack, k.off)
		require.True(t, ok)
		require.Equal(t, d, v.data)
	}
	require.LessOrEqual(t, 2*tbl.n, len(tbl.keys), "load factor stays at or below one half")
}
