package objstore

import (
	"crypto/rand"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeHash(s string) Hash {
	var h Hash
	copy(h[:], []byte(s))
	return h
}

func makeBlob(size int) ([]byte, error) {
	blob := make([]byte, size)
	if _, err := rand.Read(blob); err != nil {
		return nil, fmt.Errorf("failed to generate random blob: %w", err)
	}
	return blob, nil
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

		err := w.add(oid, data)
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

		err := w.add(oid, originalData)
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
		w.add(oid, []byte("data"))

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
		w.add(oid, []byte("data"))
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

		w.add(oids[0], make([]byte, 40))
		w.add(oids[1], make([]byte, 40))
		w.add(oids[2], make([]byte, 40))

		_, ok := w.acquire(oids[0])
		assert.False(t, ok, "LRU object A should be evicted")

		_, ok = w.acquire(oids[1])
		assert.True(t, ok, "object B should be present")
	})

	t.Run("Acquire Promotes to MRU", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		w.budget = 100
		oids := []Hash{makeHash("A"), makeHash("B"), makeHash("C"), makeHash("D")}

		w.add(oids[0], make([]byte, 50))
		w.add(oids[1], make([]byte, 50))

		h, _ := w.acquire(oids[0])
		h.Release()

		w.add(oids[2], make([]byte, 50))

		_, ok := w.acquire(oids[0])
		assert.True(t, ok, "recently used object A should be present")

		_, ok = w.acquire(oids[1])
		assert.False(t, ok, "least recently used object B should be evicted")
	})

	t.Run("Eviction Skips Referenced Entries", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		w.budget = 100
		oidA, oidB, oidC := makeHash("A"), makeHash("B"), makeHash("C")

		w.add(oidA, make([]byte, 60))

		h, ok := w.acquire(oidA)
		require.True(t, ok, "should be able to acquire object A")
		defer func() {
			if h != nil {
				h.Release()
			}
		}()

		w.add(oidB, make([]byte, 60))

		err := w.add(oidC, make([]byte, 30))
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

		w.add(oid, data1)
		h1, _ := w.acquire(oid)

		w.add(oid, data2)
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

		w.add(oid, largeData)
		h1, _ := w.acquire(oid)
		h1.Release()

		w.add(oid, smallData)
		h2, _ := w.acquire(oid)

		assert.Equal(t, len(smallData), len(h2.Data()), "data length should match the new data")
		h2.Release()
	})
}

func testErrorsAndEdgeCases(t *testing.T) {
	t.Run("Object Larger Than Budget", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		w.budget = 100
		oversized, _ := makeBlob(101)

		err := w.add(makeHash("oversized"), oversized)
		assert.Error(t, err, "should get error when adding object larger than budget")
	})

	t.Run("Debug WindowFull", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		w.budget = 100

		err := w.add(makeHash("A"), make([]byte, 80))
		require.NoError(t, err, "should be able to add A")
		t.Logf("After adding A: used=%d, evictable=%d", w.used, w.evictable)

		h, _ := w.acquire(makeHash("A")) // Pin the entry.
		t.Logf("After acquiring A: used=%d, evictable=%d", w.used, w.evictable)
		defer h.Release()

		// This should exceed budget and fail.
		err = w.add(makeHash("B"), make([]byte, 80))
		t.Logf("After attempting to add B: used=%d, evictable=%d, err=%v", w.used, w.evictable, err)
		assert.ErrorIs(t, err, ErrWindowFull, "should get ErrWindowFull")
	})

	t.Run("WindowFull When All Entries Pinned", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		w.budget = 100

		w.add(makeHash("A"), make([]byte, 80)) // Use 80 bytes instead of 60
		h, _ := w.acquire(makeHash("A"))       // Pin the entry.
		defer h.Release()

		// This add should fail because it exceeds the budget (80+80=160 > 100),
		// and the existing item cannot be evicted.
		err := w.add(makeHash("B"), make([]byte, 80))
		assert.ErrorIs(t, err, ErrWindowFull, "should get ErrWindowFull when all entries are pinned")
	})

	t.Run("Zero-Sized and Nil Data", func(t *testing.T) {
		w := newRefCountedDeltaWindow()
		oidZero := makeHash("zero")
		oidNil := makeHash("nil")

		err := w.add(oidZero, []byte{})
		require.NoError(t, err, "add with empty slice should not fail")

		err = w.add(oidNil, nil)
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
		chunk, _ := makeBlob(windowBudget / objects / 4) // Use small chunks

		for i := range objects {
			_ = w.add(makeHash(fmt.Sprintf("obj-%d", i)), chunk)
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
							_ = w.add(oid, chunk)
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
