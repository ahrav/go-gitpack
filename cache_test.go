package objstore

import (
	cryptorand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeHash(s string) Hash {
	var h Hash
	copy(h[:], []byte(s))
	return h
}

func TestRefCountedDeltaWindow(t *testing.T) {
	t.Run("Core Functionality", testCoreFunctionality)
	t.Run("Reference Counting", testReferenceCounting)
	t.Run("Eviction Policy", testEvictionPolicy)
	t.Run("Updates and Data", testUpdates)
	t.Run("Errors and Edge Cases", testErrorsAndEdgeCases)
	t.Run("Concurrency", testConcurrency)
}

func testCoreFunctionality(t *testing.T) {
	t.Run("Add-Acquire-Release Cycle", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		oid := makeHash("happy-path")
		data := []byte("hello world")

		err := w.add(oid, data, ObjBlob)
		require.NoError(t, err, "add should not fail")

		h, ok := w.acquire(oid)
		require.True(t, ok, "acquire should succeed for a recently added object")
		assert.Equal(t, data, h.Data(), "Data() should return the original data")

		h.Release()
		assert.Equal(t, data, h.Data(), "Handle's data should remain valid after Release")

		h2, ok := w.acquire(oid)
		require.True(t, ok, "should be able to re-acquire object after release")
		h2.Release()
	})

	t.Run("Acquire Non-Existent", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		_, ok := w.acquire(makeHash("non-existent"))
		assert.False(t, ok, "acquire should return false for a non-existent object")
	})

	t.Run("Data Mutability", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		oid := makeHash("mutable")
		originalData := []byte("original")

		err := w.add(oid, originalData, ObjBlob)
		require.NoError(t, err, "add should not fail")

		originalData[0] = 'X'

		h, ok := w.acquire(oid)
		require.True(t, ok, "acquire should succeed")
		defer h.Release()

		assert.Equal(t, []byte("Xriginal"), h.Data(), "cache data should reflect external changes to the source slice")
	})
}

func testReferenceCounting(t *testing.T) {
	t.Run("Ref-Count and Evictable Accounting", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		oid := makeHash("ref-counting")
		w.add(oid, []byte("data"), ObjBlob)

		assert.Equal(t, 1, w.evictable, "evictable should be 1 after add")

		h1, _ := w.acquire(oid)
		assert.Equal(t, 0, w.evictable, "evictable should be 0 after first acquire")

		h2, _ := w.acquire(oid)
		assert.Equal(t, 0, w.evictable, "evictable should be 0 after second acquire")

		h1.Release()
		assert.Equal(t, 0, w.evictable, "evictable should be 0 when one handle is still outstanding")

		h2.Release()
		assert.Equal(t, 1, w.evictable, "evictable should be 1 after all handles are released")
	})

	t.Run("Double Release is Idempotent", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		oid := makeHash("double-release")
		w.add(oid, []byte("data"), ObjBlob)
		h, _ := w.acquire(oid)

		h.Release()
		assert.Equal(t, 1, w.evictable, "evictable count should be 1 after first release")

		h.Release()
		assert.Equal(t, 1, w.evictable, "evictable count should remain 1 after second release")
	})
}

func testEvictionPolicy(t *testing.T) {
	t.Run("LRU Item is Evicted", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		w.budget = 100
		oids := []Hash{makeHash("A"), makeHash("B"), makeHash("C")}

		w.add(oids[0], make([]byte, 40), ObjBlob)
		w.add(oids[1], make([]byte, 40), ObjBlob)
		w.add(oids[2], make([]byte, 40), ObjBlob)

		_, ok := w.acquire(oids[0])
		assert.False(t, ok, "LRU object A should be evicted")

		_, ok = w.acquire(oids[1])
		assert.True(t, ok, "object B should be present")
	})

	t.Run("Acquire Promotes to MRU", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		w.budget = 100
		oids := []Hash{makeHash("A"), makeHash("B"), makeHash("C"), makeHash("D")}

		w.add(oids[0], make([]byte, 50), ObjBlob)
		w.add(oids[1], make([]byte, 50), ObjBlob)

		h, _ := w.acquire(oids[0])
		h.Release()

		w.add(oids[2], make([]byte, 50), ObjBlob)

		_, ok := w.acquire(oids[0])
		assert.True(t, ok, "recently used object A should be present")

		_, ok = w.acquire(oids[1])
		assert.False(t, ok, "least recently used object B should be evicted")
	})

	t.Run("Eviction Skips Referenced Entries", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		w.budget = 100
		oidA, oidB, oidC := makeHash("A"), makeHash("B"), makeHash("C")

		w.add(oidA, make([]byte, 60), ObjBlob)

		h, ok := w.acquire(oidA)
		require.True(t, ok, "should be able to acquire object A")
		defer func() {
			if h != nil {
				h.Release()
			}
		}()

		w.add(oidB, make([]byte, 60), ObjBlob)

		err := w.add(oidC, make([]byte, 30), ObjBlob)
		require.NoError(t, err, "add should not fail")

		_, ok = w.acquire(oidA)
		assert.True(t, ok, "referenced object A should not be evicted")

		_, ok = w.acquire(oidB)
		assert.False(t, ok, "unreferenced object B should have been evicted")
	})
}

func testUpdates(t *testing.T) {
	t.Run("Update Existing Entry", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		oid := makeHash("update")
		data1 := []byte("initial data")
		data2 := []byte("updated data")

		w.add(oid, data1, ObjBlob)
		h1, _ := w.acquire(oid)

		w.add(oid, data2, ObjBlob)
		h2, _ := w.acquire(oid)

		assert.Equal(t, data2, h2.Data(), "new handle should see the updated data")

		h1.Release()
		h2.Release()
	})

	t.Run("Memory Reuse on Update", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		oid := makeHash("mem-reuse")
		largeData := make([]byte, 1000)
		smallData := make([]byte, 100)

		w.add(oid, largeData, ObjBlob)
		h1, _ := w.acquire(oid)
		h1.Release()

		w.add(oid, smallData, ObjBlob)
		h2, _ := w.acquire(oid)

		assert.Equal(t, len(smallData), len(h2.Data()), "data length should match the new data")
		h2.Release()
	})
}

func makeBlob(n int) []byte { b := make([]byte, n); _, _ = cryptorand.Read(b); return b } // randRead: helper

func testErrorsAndEdgeCases(t *testing.T) {
	t.Run("Object Larger Than Budget", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		w.budget = 100
		oversized := makeBlob(101)

		err := w.add(makeHash("oversized"), oversized, ObjBlob)
		assert.Error(t, err, "should get error when adding object larger than budget")
	})

	t.Run("Debug WindowFull", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		w.budget = 100

		err := w.add(makeHash("A"), make([]byte, 80), ObjBlob)
		require.NoError(t, err, "should be able to add A")
		t.Logf("After adding A: used=%d, evictable=%d", w.used, w.evictable)

		h, _ := w.acquire(makeHash("A")) // Pin the entry.
		t.Logf("After acquiring A: used=%d, evictable=%d", w.used, w.evictable)
		defer h.Release()

		// This should exceed budget and fail.
		err = w.add(makeHash("B"), make([]byte, 80), ObjBlob)
		t.Logf("After attempting to add B: used=%d, evictable=%d, err=%v", w.used, w.evictable, err)
		assert.ErrorIs(t, err, ErrWindowFull, "should get ErrWindowFull")
	})

	t.Run("WindowFull When All Entries Pinned", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		w.budget = 100

		w.add(makeHash("A"), make([]byte, 80), ObjBlob) // Use 80 bytes instead of 60
		h, _ := w.acquire(makeHash("A"))                // Pin the entry.
		defer h.Release()

		// This add should fail because it exceeds the budget (80+80=160 > 100),
		// and the existing item cannot be evicted.
		err := w.add(makeHash("B"), make([]byte, 80), ObjBlob)
		assert.ErrorIs(t, err, ErrWindowFull, "should get ErrWindowFull when all entries are pinned")
	})

	t.Run("Zero-Sized and Nil Data", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		oidZero := makeHash("zero")
		oidNil := makeHash("nil")

		err := w.add(oidZero, []byte{}, ObjBlob)
		require.NoError(t, err, "add with empty slice should not fail")

		err = w.add(oidNil, nil, ObjBlob)
		require.NoError(t, err, "add with nil slice should not fail")

		h, ok := w.acquire(oidZero)
		require.True(t, ok, "should be able to acquire zero-sized data")
		assert.Equal(t, 0, len(h.Data()), "zero-sized data should have length 0")
		h.Release()

		h, ok = w.acquire(oidNil)
		require.True(t, ok, "should be able to acquire nil data")
		assert.Nil(t, h.Data(), "nil data should remain nil")
		h.Release()
	})
}

// testConcurrency stress-tests the implementation for race conditions.
func testConcurrency(t *testing.T) {
	t.Parallel()

	t.Run("Hammer Mixed Workload", func(t *testing.T) {
		t.Parallel()
		w := newRefCountedDeltaWindow()
		const objects = 16
		chunk := makeBlob(windowBudget / objects / 4) // Use small chunks

		for i := range objects {
			_ = w.add(makeHash(fmt.Sprintf("obj-%d", i)), chunk, ObjBlob)
		}

		var wg sync.WaitGroup
		stop := make(chan struct{})
		numGoroutines := runtime.GOMAXPROCS(0) * 4

		for i := range numGoroutines {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				// Use a simple LCG for pseudo-randomness without locking.
				rng := uint64(id*32 + 1)
				for {
					select {
					case <-stop:
						return
					default:
						oid := makeHash(fmt.Sprintf("obj-%d", rng%objects))
						// 80% reads, 20% writes
						if rng%5 < 4 {
							if h, ok := w.acquire(oid); ok {
								_ = h.Data()[0] // Simulate touching the data.
								h.Release()
							}
						} else {
							// Errors are expected and ignored (e.g., ErrWindowFull)
							_ = w.add(oid, chunk, ObjBlob)
						}
						rng = rng*1664525 + 1013904223
					}
				}
			}(i)
		}

		time.Sleep(200 * time.Millisecond)
		close(stop)
		wg.Wait()

		// Final check: ensure all reference counts are zero after the test.
		w.mu.Lock()
		defer w.mu.Unlock()
		for _, elem := range w.index {
			entry := elem.Value.(*refCountedEntry)
			cnt := entry.refCnt.Load()
			assert.Equal(t, int32(0), cnt, "no leaked reference counts should remain")
		}
	})
}

// Benchmark configuration.
const (
	// Cache sizes - typical for git operations.
	defaultBudget = 64 << 20 // 8 MiB

	// Object sizes based on real git repos.
	tinyObject   = 128       // Commit objects.
	smallObject  = 1 << 10   // 1 KiB - small text files
	mediumObject = 16 << 10  // 16 KiB - typical source files
	largeObject  = 256 << 10 // 256 KiB - larger files

	// Simulated decompression costs.
	tinyInflate   = 10 * time.Microsecond
	smallInflate  = 50 * time.Microsecond
	mediumInflate = 200 * time.Microsecond
	largeInflate  = 1 * time.Millisecond
)

// Pre-generated access patterns to ensure reproducible benchmarks.
type accessStep struct {
	chainIdx int
	walkLen  int
}

type treeWalkStep struct {
	numSubtrees int
	subtreeIdx  []int
	numFiles    []int
	fileIdx     [][]int
}

type chainStep struct {
	chainIdx    int
	releaseProb []bool // Pre-computed release decisions.
}

type packStep struct {
	pattern int // 0=sequential, 1=random, 2=locality.
	indices []int
}

func generateHotBasesPattern(numSteps int) []accessStep {
	rng := rand.New(rand.NewPCG(12345, 67890))
	steps := make([]accessStep, numSteps)

	numChains := 40
	chainLength := 10

	for i := range steps {
		steps[i] = accessStep{
			chainIdx: rng.IntN(numChains),
			walkLen:  rng.IntN(chainLength) + 1,
		}
	}
	return steps
}

func generateTreeWalkPattern(numSteps int) []treeWalkStep {
	rng := rand.New(rand.NewPCG(12345, 67890))
	steps := make([]treeWalkStep, numSteps)

	for i := range steps {
		numSubtrees := rng.IntN(3) + 1
		step := treeWalkStep{
			numSubtrees: numSubtrees,
			subtreeIdx:  make([]int, numSubtrees),
			numFiles:    make([]int, numSubtrees),
			fileIdx:     make([][]int, numSubtrees),
		}

		for j := range numSubtrees {
			step.subtreeIdx[j] = rng.IntN(10) // 10 subtrees.
			step.numFiles[j] = rng.IntN(5) + 1
			step.fileIdx[j] = make([]int, step.numFiles[j])
			for k := 0; k < step.numFiles[j]; k++ {
				step.fileIdx[j][k] = rng.IntN(20) // 20 files per tree.
			}
		}
		steps[i] = step
	}
	return steps
}

func generateConcurrentChainsPattern(numSteps int) []chainStep {
	rng := rand.New(rand.NewPCG(12345, 67890))
	steps := make([]chainStep, numSteps)

	numChains := 16

	for i := range steps {
		chainLen := 15 + (i%numChains)*2
		steps[i] = chainStep{
			chainIdx:    rng.IntN(numChains),
			releaseProb: make([]bool, chainLen),
		}

		// Pre-compute release decisions.
		for j := range steps[i].releaseProb {
			steps[i].releaseProb[j] = j > 3 && rng.Float32() < 0.3
		}
	}
	return steps
}

func generatePackTraversalPattern(numSteps int) []packStep {
	rng := rand.New(rand.NewPCG(12345, 67890))
	steps := make([]packStep, numSteps)

	numObjects := 1000

	for i := range steps {
		pattern := rng.IntN(3)
		step := packStep{pattern: pattern}

		switch pattern {
		case 0: // Sequential scan.
			start := rng.IntN(numObjects - 50)
			step.indices = make([]int, 50)
			for j := range 50 {
				step.indices[j] = start + j
			}
		case 1: // Random access.
			step.indices = make([]int, 30)
			for j := range 30 {
				step.indices[j] = rng.IntN(numObjects)
			}
		case 2: // Locality-based.
			center := rng.IntN(numObjects)
			step.indices = make([]int, 0, 20)
			for j := 0; j < 20; j++ {
				offset := rng.IntN(100) - 50
				idx := center + offset
				if idx >= 0 && idx < numObjects {
					step.indices = append(step.indices, idx)
				}
			}
		}
		steps[i] = step
	}
	return steps
}

// Cache interface for benchmarking different implementations.
type benchCache interface {
	add(Hash, []byte, ObjectType) error
	acquire(Hash) (*Handle, bool)
}

func newRefCache(budget int) benchCache {
	w := newRefCountedDeltaWindow()
	w.budget = budget
	return w
}

type lruWrap struct {
	*lru.Cache[string, []byte]
}

func newLRUCache(budget, avgObjSize int) benchCache {
	capEntries := budget / avgObjSize
	if capEntries == 0 {
		capEntries = 1
	}
	c, _ := lru.New[string, []byte](capEntries)
	return &lruWrap{c}
}

func (l *lruWrap) add(h Hash, b []byte, _ ObjectType) error {
	l.Add(string(h[:]), b)
	return nil
}

func (l *lruWrap) acquire(h Hash) (*Handle, bool) {
	if v, ok := l.Get(string(h[:])); ok {
		return &Handle{data: v}, true // No reference counting for LRU.
	}
	return nil, false
}

// Benchmark metrics collection.
type metrics struct {
	hits      atomic.Uint64
	misses    atomic.Uint64
	inflates  atomic.Uint64
	conflicts atomic.Uint64 // Concurrent access to same object.
}

func (m *metrics) report(b *testing.B) {
	total := m.hits.Load() + m.misses.Load()
	hitRate := float64(m.hits.Load()) / float64(total) * 100
	b.ReportMetric(hitRate, "hit%")
	b.ReportMetric(float64(m.misses.Load())/float64(b.N), "miss/op")
	b.ReportMetric(float64(m.inflates.Load())/float64(b.N), "inflate/op")
	b.ReportMetric(float64(m.conflicts.Load())/float64(b.N), "conflict/op")
}

// Object generation with simulated decompression costs.
type object struct {
	oid  Hash
	data []byte
	cost time.Duration
}

func makeObject(name string, size int) object {
	data := make([]byte, size)
	cryptorand.Read(data)

	var cost time.Duration
	switch {
	case size <= tinyObject:
		cost = tinyInflate
	case size <= smallObject:
		cost = smallInflate
	case size <= mediumObject:
		cost = mediumInflate
	default:
		cost = largeInflate
	}

	return object{
		oid:  makeHash(name),
		data: data,
		cost: cost,
	}
}

// Scenario 1: Hot Base Objects
// Simulates multiple delta chains sharing common base objects.
// This is typical when many files share similar content.
func benchmarkHotBases(b *testing.B, newCache func() benchCache) {
	cache := newCache()
	m := &metrics{}

	// Create a few hot base objects.
	bases := []object{
		makeObject("base-1", largeObject),
		makeObject("base-2", largeObject),
		makeObject("base-3", mediumObject),
	}

	// Create many delta objects referencing these bases.
	numChains := 40
	chainLength := 10
	chains := make([][]object, numChains)
	for i := range numChains {
		chains[i] = make([]object, chainLength)
		for j := range chainLength {
			chains[i][j] = makeObject(fmt.Sprintf("delta-%d-%d", i, j), smallObject)
		}
	}

	pattern := generateHotBasesPattern(10000)

	var inflating sync.Map // Tracks objects currently being inflated.

	acquire := func(obj object) *Handle {
		if h, ok := cache.acquire(obj.oid); ok {
			m.hits.Add(1)
			return h
		}

		m.misses.Add(1)

		// Check if another goroutine is already inflating this object.
		if _, loaded := inflating.LoadOrStore(string(obj.oid[:]), true); loaded {
			m.conflicts.Add(1)
			// Wait for the other goroutine to finish.
			for range 100 {
				if h, ok := cache.acquire(obj.oid); ok {
					return h
				}
				time.Sleep(time.Microsecond)
			}
		}

		// Simulate decompression.
		m.inflates.Add(1)
		time.Sleep(obj.cost)
		cache.add(obj.oid, obj.data, ObjBlob)
		inflating.Delete(string(obj.oid[:]))

		h, _ := cache.acquire(obj.oid)
		return h
	}

	b.ResetTimer()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		stepIdx := 0
		for pb.Next() {
			step := pattern[stepIdx%len(pattern)]
			chainIdx := step.chainIdx
			walkLen := step.walkLen
			baseIdx := chainIdx % len(bases)

			// Resolve the base (hot object).
			if h := acquire(bases[baseIdx]); h != nil {
				h.Release()
			}

			// Walk some of the chain.
			for i := range walkLen {
				if h := acquire(chains[chainIdx][i]); h != nil {
					h.Release()
				}
			}

			stepIdx++
		}
	})

	m.report(b)
}

// Scenario 2: Tree Walking
// Simulates walking git trees where parent directories are frequently reused.
func benchmarkTreeWalk(b *testing.B, newCache func() benchCache) {
	cache := newCache()
	m := &metrics{}

	// Create tree structure.
	rootTree := makeObject("root", smallObject)
	subTrees := make([]object, 10)
	for i := range subTrees {
		subTrees[i] = makeObject(fmt.Sprintf("subtree-%d", i), smallObject)
	}

	// Files in each subtree.
	filesPerTree := 20
	files := make([][]object, len(subTrees))
	for i := range files {
		files[i] = make([]object, filesPerTree)
		for j := range files[i] {
			size := mediumObject
			if j%5 == 0 {
				size = largeObject // Occasional large file.
			}
			files[i][j] = makeObject(fmt.Sprintf("file-%d-%d", i, j), size)
		}
	}

	pattern := generateTreeWalkPattern(10000)

	var inflating sync.Map

	acquire := func(obj object) *Handle {
		if h, ok := cache.acquire(obj.oid); ok {
			m.hits.Add(1)
			return h
		}

		m.misses.Add(1)

		if _, loaded := inflating.LoadOrStore(string(obj.oid[:]), true); loaded {
			m.conflicts.Add(1)
			for range 100 {
				if h, ok := cache.acquire(obj.oid); ok {
					return h
				}
				time.Sleep(time.Microsecond)
			}
		}

		m.inflates.Add(1)
		time.Sleep(obj.cost)
		cache.add(obj.oid, obj.data, ObjBlob)
		inflating.Delete(string(obj.oid[:]))

		h, _ := cache.acquire(obj.oid)
		return h
	}

	b.ResetTimer()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		stepIdx := 0
		for pb.Next() {
			// Always start with root.
			if h := acquire(rootTree); h != nil {
				h.Release()
			}

			step := pattern[stepIdx%len(pattern)]

			// Walk the predetermined subtrees.
			for i := 0; i < step.numSubtrees; i++ {
				treeIdx := step.subtreeIdx[i]

				if h := acquire(subTrees[treeIdx]); h != nil {
					// Look at predetermined files in this tree.
					for j := 0; j < step.numFiles[i]; j++ {
						fileIdx := step.fileIdx[i][j]
						if fh := acquire(files[treeIdx][fileIdx]); fh != nil {
							fh.Release()
						}
					}
					h.Release()
				}
			}

			stepIdx++
		}
	})

	m.report(b)
}

// Scenario 3: Concurrent Resolution
// Simulates multiple goroutines resolving overlapping delta chains.
// This tests the value of reference counting.
func benchmarkConcurrentChains(b *testing.B, newCache func() benchCache) {
	cache := newCache()
	m := &metrics{}

	// Create overlapping delta chains.
	sharedBase := makeObject("shared-base", largeObject)

	numChains := 16
	chains := make([][]object, numChains)
	for i := range numChains {
		chainLen := 15 + i*2 // Varying lengths.
		chains[i] = make([]object, chainLen)
		for j := range chainLen {
			chains[i][j] = makeObject(fmt.Sprintf("obj-%d-%d", i, j), mediumObject)
		}
	}

	pattern := generateConcurrentChainsPattern(10000)

	var (
		inflating sync.Map
		holding   atomic.Int32 // Number of handles currently held.
	)

	acquire := func(obj object) *Handle {
		if h, ok := cache.acquire(obj.oid); ok {
			m.hits.Add(1)
			holding.Add(1)
			return h
		}

		m.misses.Add(1)

		if _, loaded := inflating.LoadOrStore(string(obj.oid[:]), true); loaded {
			m.conflicts.Add(1)
			for range 100 {
				if h, ok := cache.acquire(obj.oid); ok {
					holding.Add(1)
					return h
				}
				time.Sleep(time.Microsecond)
			}
		}

		m.inflates.Add(1)
		time.Sleep(obj.cost)
		cache.add(obj.oid, obj.data, ObjBlob)
		inflating.Delete(string(obj.oid[:]))

		h, _ := cache.acquire(obj.oid)
		if h != nil {
			holding.Add(1)
		}
		return h
	}

	release := func(h *Handle) {
		if h != nil {
			h.Release()
			holding.Add(-1)
		}
	}

	b.ResetTimer()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		stepIdx := 0
		for pb.Next() {
			step := pattern[stepIdx%len(pattern)]
			chainIdx := step.chainIdx
			chain := chains[chainIdx]

			// Always need the shared base.
			baseH := acquire(sharedBase)

			// Hold onto some objects while acquiring others.
			// This simulates real delta resolution.
			held := make([]*Handle, 0, len(chain))

			for i, obj := range chain {
				h := acquire(obj)
				if h != nil {
					held = append(held, h)
				}

				// Use pre-computed release decision.
				if i < len(step.releaseProb) && step.releaseProb[i] && len(held) > 1 {
					// Release the first held object (instead of random).
					release(held[0])
					held = held[1:]
				}
			}

			// Clean up.
			release(baseH)
			for _, h := range held {
				release(h)
			}

			stepIdx++
		}
	})

	m.report(b)
	b.ReportMetric(float64(holding.Load()), "holding_final")
}

// Scenario 4: Pack Traversal
// Simulates scanning a packfile with mixed object types and sizes.
func benchmarkPackTraversal(b *testing.B, newCache func() benchCache) {
	cache := newCache()
	m := &metrics{}

	// Create a mix of objects like in a real pack.
	numObjects := 10_000
	objects := make([]object, numObjects)

	// Distribution based on typical git repos.
	for i := range numObjects {
		var size int
		switch {
		case i < 100: // 10% large blobs.
			size = largeObject
		case i < 300: // 20% medium files.
			size = mediumObject
		case i < 700: // 40% small files.
			size = smallObject
		default: // 30% tiny (commits/trees).
			size = tinyObject
		}
		objects[i] = makeObject(fmt.Sprintf("pack-obj-%d", i), size)
	}

	pattern := generatePackTraversalPattern(10_000)

	var inflating sync.Map

	acquire := func(obj object) *Handle {
		if h, ok := cache.acquire(obj.oid); ok {
			m.hits.Add(1)
			return h
		}

		m.misses.Add(1)

		if _, loaded := inflating.LoadOrStore(string(obj.oid[:]), true); loaded {
			m.conflicts.Add(1)
			for range 100 {
				if h, ok := cache.acquire(obj.oid); ok {
					return h
				}
				time.Sleep(time.Microsecond)
			}
		}

		m.inflates.Add(1)
		time.Sleep(obj.cost)
		cache.add(obj.oid, obj.data, ObjBlob)
		inflating.Delete(string(obj.oid[:]))

		h, _ := cache.acquire(obj.oid)
		return h
	}

	b.ResetTimer()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		stepIdx := 0
		for pb.Next() {
			step := pattern[stepIdx%len(pattern)]

			// Execute the predetermined access pattern.
			for _, idx := range step.indices {
				if h := acquire(objects[idx]); h != nil {
					h.Release()
				}
			}

			stepIdx++
		}
	})

	m.report(b)
}

// Scenario 5: Delta Chain Resolution
// Simulates resolving full delta chains where all objects in the chain
// must be held in memory simultaneously until the final object is reconstructed.
// This is the core operation when Git needs to reconstruct a file from a packfile.
func benchmarkDeltaChainResolution(b *testing.B, newCache func() benchCache) {
	cache := newCache()
	m := &metrics{}

	// Create delta chains of varying depths
	numChains := 100
	maxChainDepth := 20
	chains := make([][]object, numChains)

	rng := rand.New(rand.NewPCG(12345, 67890))

	for i := range numChains {
		// Vary chain depths to simulate real packfiles
		depth := 5 + rng.IntN(maxChainDepth-5)
		chains[i] = make([]object, depth)

		// Base object (larger, as it contains the full content)
		chains[i][0] = makeObject(fmt.Sprintf("base-%d", i), largeObject)

		// Delta objects (smaller, as they only contain differences)
		for j := 1; j < depth; j++ {
			// Deltas get progressively smaller deeper in the chain
			size := smallObject
			if j > depth/2 {
				size = tinyObject
			}
			chains[i][j] = makeObject(fmt.Sprintf("delta-%d-%d", i, j), size)
		}
	}

	// Pre-generate access pattern for reproducibility
	type chainResolutionStep struct {
		chainIdx       int
		partialResolve bool          // Sometimes we only need part of the chain
		resolveDepth   int           // How deep to resolve (if partial)
		holdDuration   time.Duration // How long to hold the final result
	}

	numSteps := 10000
	pattern := make([]chainResolutionStep, numSteps)
	for i := range pattern {
		chainIdx := rng.IntN(numChains)
		partialResolve := rng.Float32() < 0.2 // 20% partial resolutions

		step := chainResolutionStep{
			chainIdx:       chainIdx,
			partialResolve: partialResolve,
			holdDuration:   time.Duration(rng.IntN(100)) * time.Microsecond,
		}

		if partialResolve {
			step.resolveDepth = 1 + rng.IntN(len(chains[chainIdx])-1)
		} else {
			step.resolveDepth = len(chains[chainIdx])
		}

		pattern[i] = step
	}

	var (
		inflating     sync.Map
		activeHandles atomic.Int64
		maxHandles    atomic.Int64 // Track peak handle usage
	)

	acquire := func(obj object) *Handle {
		if h, ok := cache.acquire(obj.oid); ok {
			m.hits.Add(1)
			current := activeHandles.Add(1)

			// Update max handles if needed
			for {
				max := maxHandles.Load()
				if current <= max || maxHandles.CompareAndSwap(max, current) {
					break
				}
			}

			return h
		}

		m.misses.Add(1)

		// Check if another goroutine is already inflating this object
		if _, loaded := inflating.LoadOrStore(string(obj.oid[:]), true); loaded {
			m.conflicts.Add(1)
			// Wait for the other goroutine to finish inflating
			for range 100 {
				if h, ok := cache.acquire(obj.oid); ok {
					current := activeHandles.Add(1)

					// Update max handles
					for {
						max := maxHandles.Load()
						if current <= max || maxHandles.CompareAndSwap(max, current) {
							break
						}
					}

					return h
				}
				time.Sleep(time.Microsecond)
			}
			// If we still can't get it, proceed to inflate ourselves
		}

		// Simulate decompression
		m.inflates.Add(1)
		time.Sleep(obj.cost)

		// Add to cache
		if err := cache.add(obj.oid, obj.data, ObjBlob); err != nil {
			inflating.Delete(string(obj.oid[:]))
			return nil
		}

		inflating.Delete(string(obj.oid[:]))

		h, _ := cache.acquire(obj.oid)
		if h != nil {
			current := activeHandles.Add(1)

			// Update max handles
			for {
				max := maxHandles.Load()
				if current <= max || maxHandles.CompareAndSwap(max, current) {
					break
				}
			}
		}
		return h
	}

	release := func(h *Handle) {
		if h != nil {
			h.Release()
			activeHandles.Add(-1)
		}
	}

	b.ResetTimer()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		stepIdx := 0

		// Track failed resolutions
		var failedResolutions int64

		for pb.Next() {
			step := pattern[stepIdx%len(pattern)]
			chain := chains[step.chainIdx]

			// Resolve the chain - need ALL objects in memory simultaneously
			handles := make([]*Handle, 0, step.resolveDepth)

			// First acquire base - this is always needed
			base := acquire(chain[0])
			if base == nil {
				failedResolutions++
				stepIdx++
				continue
			}
			handles = append(handles, base)

			// Then acquire each delta in sequence
			// In real Git, we need all previous deltas to apply the next one
			failed := false
			for i := 1; i < step.resolveDepth; i++ {
				delta := acquire(chain[i])
				if delta == nil {
					// Can't complete chain resolution without this delta
					failed = true
					failedResolutions++
					break
				}
				handles = append(handles, delta)

				// Simulate applying delta
				// This requires reading from base and all previous deltas
				applyTime := time.Microsecond * time.Duration(i*2)
				time.Sleep(applyTime)
			}

			if !failed {
				// Successfully resolved the chain
				// Simulate using the final reconstructed object
				time.Sleep(step.holdDuration)
			}

			// Release in reverse order (deltas before base)
			// This simulates how Git releases intermediate objects
			for i := len(handles) - 1; i >= 0; i-- {
				release(handles[i])
			}

			stepIdx++
		}

		// Report thread-local failed resolutions
		if failedResolutions > 0 {
			m.conflicts.Add(uint64(failedResolutions))
		}
	})

	m.report(b)
	b.ReportMetric(float64(activeHandles.Load()), "active_handles_final")
	b.ReportMetric(float64(maxHandles.Load()), "max_handles")
}

// Main benchmark runner.
func BenchmarkDeltaCache(b *testing.B) {
	scenarios := []struct {
		name    string
		fn      func(*testing.B, func() benchCache)
		desc    string
		avgSize int // Average object size for LRU capacity calculation.
	}{
		{
			name:    "HotBases",
			fn:      benchmarkHotBases,
			desc:    "Multiple chains sharing hot base objects",
			avgSize: 64 << 10, // 64 KiB - mix of large bases and small deltas
		},
		{
			name:    "TreeWalk",
			fn:      benchmarkTreeWalk,
			desc:    "Walking git trees with parent reuse",
			avgSize: 32 << 10, // 32 KiB - mix of small trees and medium/large files
		},
		{
			name:    "ConcurrentChains",
			fn:      benchmarkConcurrentChains,
			desc:    "Overlapping delta chains with held references",
			avgSize: 32 << 10, // 32 KiB - large shared base + medium chain objects
		},
		{
			name:    "PackTraversal",
			fn:      benchmarkPackTraversal,
			desc:    "Mixed object sizes and access patterns",
			avgSize: 16 << 10, // 16 KiB - weighted average of mixed sizes
		},
		{
			name:    "DeltaChainResolution",
			fn:      benchmarkDeltaChainResolution,
			desc:    "Full delta chain resolution with all objects held",
			avgSize: 32 << 10, // 32 KiB - large bases + many small deltas
		},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			b.Run("RefCount", func(b *testing.B) {
				scenario.fn(b, func() benchCache {
					return newRefCache(defaultBudget)
				})
			})
			b.Run("LRU", func(b *testing.B) {
				scenario.fn(b, func() benchCache {
					return newLRUCache(defaultBudget, scenario.avgSize)
				})
			})
		})
	}
}
