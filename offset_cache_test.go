package objstore

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

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

// openWhaleStore opens a store over a one-commit repository holding one blob
// whose compressed entry exceeds whaleMinCompressedBytes and one small text
// file, and returns the store with the large blob's content.
func openWhaleStore(t *testing.T) (*store, []byte) {
	t.Helper()
	repo, big := buildWhaleRepo(t)
	s, err := open(filepath.Join(repo, ".git", "objects", "pack"))
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s, big
}

// buildWhaleRepo creates the one-commit repository behind openWhaleStore and
// returns its work tree with the large blob's content.
func buildWhaleRepo(t *testing.T) (string, []byte) {
	t.Helper()
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
	return repo, big
}

// TestWhalePrefetchCoversDeltifiedBlob pins that a multi-MB blob stored as a
// delta of its previous version is a whale too: both versions of the big
// file are candidates, the deltified one is sized through its chain, and
// the prefetch delivers its bytes with the blob type.
func TestWhalePrefetchCoversDeltifiedBlob(t *testing.T) {
	requireGit(t)
	repo := t.TempDir()
	runGit(t, repo, "init", "--quiet")
	// Three times the whale threshold of incompressible bytes; the second
	// version rewrites the middle third, so the delta between them is
	// itself above the threshold.
	big := make([]byte, 3*whaleMinCompressedBytes)
	rng := uint64(0x9E3779B97F4A7C15)
	for i := range big {
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		big[i] = byte(rng)
	}
	require.NoError(t, os.WriteFile(filepath.Join(repo, "big.bin"), big, 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "v1", "--quiet")
	// The second version shares most bytes with the first, so the repack
	// stores one of them as a delta of the other, and the delta is still
	// megabytes because the changed region is incompressible.
	big2 := append([]byte(nil), big...)
	for i := len(big2) / 3; i < 2*len(big2)/3; i++ {
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		big2[i] = byte(rng)
	}
	require.NoError(t, os.WriteFile(filepath.Join(repo, "big.bin"), big2, 0o644))
	runGit(t, repo, "add", "-A")
	runGit(t, repo, "commit", "-m", "v2", "--quiet")
	runGit(t, repo, "repack", "-a", "-d", "-q", "--window=10", "--depth=10")

	s, err := open(filepath.Join(repo, ".git", "objects", "pack"))
	require.NoError(t, err)
	defer s.Close()

	oid1, oid2 := calculateHash(ObjBlob, big), calculateHash(ObjBlob, big2)
	deltified := 0
	for _, oid := range []Hash{oid1, oid2} {
		p, off, ok := s.findPackedObject(oid)
		require.True(t, ok)
		typ, _, err := peekObjectType(p, off)
		require.NoError(t, err)
		if typ == ObjOfsDelta || typ == ObjRefDelta {
			deltified++
		}
		size, root, ok := s.whaleSizeAt(p, off)
		require.True(t, ok)
		require.Equal(t, ObjBlob, root)
		require.GreaterOrEqual(t, size, uint64(len(big)))
	}
	require.Equal(t, 1, deltified, "the repack stores one version as a delta of the other")

	c := s.newWhaleCache()
	require.NotNil(t, c)
	require.Len(t, c.order, 2, "both versions are prefetched")
	for _, want := range [][]byte{big, big2} {
		p, off, _ := s.findPackedObject(calculateHash(ObjBlob, want))
		got, typ, ok := c.get(p, off, nil)
		require.True(t, ok)
		require.Equal(t, ObjBlob, typ)
		require.Equal(t, want, got)
	}
}

// TestDedupScanPrefetchesWhales pins that the dedup pipeline runs the whale
// prefetch for the duration of its scan and releases it on return: a
// multi-MB blob's inflation then starts at scan start instead of when the
// tree stage reaches its commit, and the scanner holds no whale bytes
// between scans.
func TestDedupScanPrefetchesWhales(t *testing.T) {
	repo, big := buildWhaleRepo(t)
	hs, err := NewHistoryScanner(filepath.Join(repo, ".git"), WithHunkLineDedup(true))
	require.NoError(t, err)
	defer hs.Close()
	require.NotEmpty(t, hs.store.whaleCandidates())

	var sawPrefetch, sawBig atomic.Bool
	require.NoError(t, hs.DiffHistoryHunksFunc(func(h HunkAddition) error {
		if hs.store.whales.Load() != nil {
			sawPrefetch.Store(true)
		}
		if h.IsBinary() && len(h.Lines()) == 1 && h.Lines()[0] == string(big) {
			sawBig.Store(true)
		}
		return nil
	}))
	require.True(t, sawPrefetch.Load(), "whale prefetch was not active during the dedup scan")
	require.True(t, sawBig.Load(), "the whale blob was not delivered")
	require.Nil(t, hs.store.whales.Load(), "whale prefetch outlived the scan")
}

// TestWhalePrefetchSkippedWhenOffsetCacheDisabled pins that a disabled offset
// cache also disables the prefetch: store.get reads prefetched whales only on
// its offset-cache path, so with the cache off a prefetch would retain up to
// whalePrefetchBudget bytes that no read consumes.
func TestWhalePrefetchSkippedWhenOffsetCacheDisabled(t *testing.T) {
	s, big := openWhaleStore(t)
	s.offCache.setBudget(0)
	require.NotEmpty(t, s.whaleCandidates())

	wait := s.prefetchWhales()
	defer wait()
	require.Nil(t, s.whales.Load(), "a disabled offset cache must not start a prefetch")

	got, typ, err := s.get(calculateHash(ObjBlob, big))
	require.NoError(t, err)
	require.Equal(t, ObjBlob, typ)
	require.Equal(t, big, got)
}

// TestWhaleGetInflatesUnstartedEntry pins that a reader waits only for an
// inflation already in progress. The prefetch inflates one entry at a time,
// so a reader that waited on every unstarted entry would queue behind all
// larger whales and serialize work the scan workers would otherwise do in
// parallel.
func TestWhaleGetInflatesUnstartedEntry(t *testing.T) {
	t.Parallel()
	packA, packB := &mmap.ReaderAt{}, &mmap.ReaderAt{}
	keyA, keyB := offCacheKey{packA, 12}, offCacheKey{packB, 12}

	entered := make(chan struct{})
	release := make(chan struct{})
	releaseA := sync.OnceFunc(func() { close(release) })
	var loadsB atomic.Int32
	c := &whaleCache{
		m: map[offCacheKey]*whaleEntry{
			keyA: {done: make(chan struct{})},
			keyB: {done: make(chan struct{})},
		},
		order: []offCacheKey{keyA, keyB},
		load: func(pack *mmap.ReaderAt, _ uint64) ([]byte, ObjectType, error) {
			if pack == packA {
				close(entered)
				<-release
				return []byte("a"), ObjBlob, nil
			}
			loadsB.Add(1)
			return []byte("b"), ObjBlob, nil
		},
	}
	stop := make(chan struct{})
	ran := make(chan struct{})
	go func() {
		defer close(ran)
		c.run(stop)
	}()
	defer func() {
		releaseA()
		<-ran
	}()
	<-entered

	got := make(chan []byte, 1)
	go func() {
		data, _, ok := c.get(packB, 12, nil)
		if !ok {
			data = nil
		}
		got <- data
	}()
	select {
	case data := <-got:
		require.Equal(t, []byte("b"), data)
	case <-time.After(5 * time.Second):
		t.Fatal("get on an unstarted whale waited behind the prefetch of a larger one")
	}

	// The prefetch skips the entry the reader already inflated.
	releaseA()
	<-ran
	require.Equal(t, int32(1), loadsB.Load(), "each whale inflates exactly once")
	data, _, ok := c.get(packA, 12, nil)
	require.True(t, ok)
	require.Equal(t, []byte("a"), data)
}

// TestWhaleCandidatesAndPrefetch builds a repository with one blob whose
// compressed size exceeds whaleMinCompressedBytes and checks that the scan
// prefetch selects it, that store.get serves the prefetched bytes, and that a
// stopped prefetch leaves the normal path intact.
func TestWhaleCandidatesAndPrefetch(t *testing.T) {
	s, big := openWhaleStore(t)

	cands := s.whaleCandidates()
	require.Len(t, cands, 1, "exactly one entry exceeds the compressed threshold")
	require.GreaterOrEqual(t, cands[0].compressed, uint64(whaleMinCompressedBytes))

	bigOID := calculateHash(ObjBlob, big)
	pack, off, ok := s.findPackedObject(bigOID)
	require.True(t, ok)
	require.Equal(t, cands[0].off, off)
	require.Same(t, cands[0].pack, pack)

	wait := s.prefetchWhales()
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
	stop := make(chan struct{})
	close(stop)
	c := s.newWhaleCache()
	require.NotNil(t, c)
	c.run(stop)
	_, _, found = c.get(pack, off, nil)
	require.False(t, found, "a stopped prefetch marks unstarted entries failed")
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
	wait := s.prefetchWhales()
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

// TestWhalePrefetchSharedAcrossConcurrentScans pins that concurrent scans on
// one store share a single whale cache: the second scan joins the published
// cache, the first scan to finish leaves it in place for the scan still
// running, and the last scan to finish releases it.
func TestWhalePrefetchSharedAcrossConcurrentScans(t *testing.T) {
	s, big := openWhaleStore(t)
	bigOID := calculateHash(ObjBlob, big)

	waitA := s.prefetchWhales()
	c := s.whales.Load()
	require.NotNil(t, c, "the first scan publishes the whale cache")

	waitB := s.prefetchWhales()
	require.Same(t, c, s.whales.Load(), "a concurrent scan joins the published cache")

	waitA()
	require.Same(t, c, s.whales.Load(), "the first scan to finish leaves the cache to the running scan")
	got, typ, err := s.get(bigOID)
	require.NoError(t, err)
	require.Equal(t, ObjBlob, typ)
	require.Equal(t, big, got)

	waitB()
	require.Nil(t, s.whales.Load(), "the last scan releases the cache")
}

// TestOffsetCacheShardFillsWholeCacheLines pins the shard size to a multiple
// of the 64-byte cache line, so adjacent shards in the shards array never
// share a line and mutex traffic on one shard stays off its neighbors.
func TestOffsetCacheShardFillsWholeCacheLines(t *testing.T) {
	size := unsafe.Sizeof(offsetCacheShard{})
	require.Zero(t, size%64, "offsetCacheShard is %d bytes; adjust its padding to a multiple of 64", size)
	require.Equal(t, uintptr(128), size, "offsetCacheShard spans two cache lines")
}

// TestWhalePrefetchReleaseIsIdempotent pins that calling one scan's wait
// function twice releases that scan's share once: the second call is a no-op
// rather than a second decrement that would close an already-closed channel.
func TestWhalePrefetchReleaseIsIdempotent(t *testing.T) {
	s, _ := openWhaleStore(t)
	waitA := s.prefetchWhales()
	waitB := s.prefetchWhales()
	waitA()
	waitA()
	require.NotNil(t, s.whales.Load(), "a repeated release by one scan leaves the other scan's share intact")
	waitB()
	require.Nil(t, s.whales.Load())
}
