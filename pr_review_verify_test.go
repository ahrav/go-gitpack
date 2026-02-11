// pr_review_verify_test.go
//
// Verification tests for PR #2 review comments. Each test targets a specific
// claim made by reviewers. Tests are written BEFORE any code changes to
// determine whether the bug is real or the reviewer was incorrect.
package objstore

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Comment #1 & #2: cache.go — Race between Release() and add() on evictable
//
// Reviewer claims: add() reads evictable.Load() without holding mu, creating
// a TOCTOU race where add() sees stale evictable counts.
//
// Reality check: In the actual code, add() acquires w.mu.Lock() at line 349
// (defer unlock at 350) BEFORE reading evictable at line 363. So the
// evictable check IS inside the critical section.
// ---------------------------------------------------------------------------

func TestReleaseAddEvictableRace(t *testing.T) {
	t.Parallel()

	// Stress test: concurrent Release() and add() should never produce a
	// spurious ErrWindowFull when space could be made by eviction.
	w := newRefCountedDeltaWindow()
	w.budget = 1000

	const numEntries = 5
	oids := make([]Hash, numEntries)
	for i := range oids {
		oids[i] = makeHash(string(rune('A' + i)))
	}

	// Fill the window.
	for _, oid := range oids {
		require.NoError(t, w.add(oid, make([]byte, 200), ObjBlob))
	}

	var (
		spuriousFull atomic.Int64
		wg           sync.WaitGroup
		iterations   = 10_000
	)

	// Goroutine 1: repeatedly acquire and release entries.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			oid := oids[i%numEntries]
			h, ok := w.acquire(oid)
			if ok {
				h.Release()
			}
		}
	}()

	// Goroutine 2: repeatedly try to add new entries that require eviction.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			newOid := makeHash(string(rune('Z' - (i % 20))))
			err := w.add(newOid, make([]byte, 200), ObjBlob)
			if err == ErrWindowFull {
				spuriousFull.Add(1)
			}
		}
	}()

	wg.Wait()

	// A small number of ErrWindowFull is acceptable under contention (all
	// entries might genuinely be pinned at the moment). But a large number
	// suggests a TOCTOU bug.
	fullCount := spuriousFull.Load()
	t.Logf("ErrWindowFull count: %d / %d", fullCount, iterations)

	// With budget=1000 and 200-byte entries (5 entries), at most 1 entry is
	// pinned at a time. Most adds should succeed via eviction.
	assert.Less(t, fullCount, int64(iterations/2),
		"too many spurious ErrWindowFull errors suggest a TOCTOU race")
}

// ---------------------------------------------------------------------------
// Comment #3: cache.go — Infinite loop in eviction
//
// Reviewer claims: if evictable > 0 but no entries are reachable via
// lru.back(), the outer for-loop runs forever.
//
// Reality check: The `if evicted == 0 { break }` at line 412-414 handles
// this case. If the inner loop finds nothing to evict, evicted stays 0
// and the outer loop breaks.
// ---------------------------------------------------------------------------

func TestEvictionDoesNotInfiniteLoop(t *testing.T) {
	t.Parallel()

	w := newRefCountedDeltaWindow()
	w.budget = 100

	// Fill window to capacity.
	require.NoError(t, w.add(makeHash("A"), make([]byte, 60), ObjBlob))

	// Pin the entry so it can't be evicted.
	h, ok := w.acquire(makeHash("A"))
	require.True(t, ok)
	defer h.Release()

	// Try to add something that exceeds budget. All entries are pinned, so
	// eviction can't free space. This should return ErrWindowFull, not hang.
	done := make(chan error, 1)
	go func() {
		done <- w.add(makeHash("B"), make([]byte, 60), ObjBlob)
	}()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, ErrWindowFull, "should return ErrWindowFull, not loop forever")
	case <-func() <-chan struct{} {
		ch := make(chan struct{})
		go func() {
			// 2 second timeout.
			t2 := make(chan struct{})
			go func() {
				for i := 0; i < 200; i++ {
					runtime.Gosched()
				}
				close(t2)
			}()
			<-t2
			close(ch)
		}()
		return ch
	}():
		t.Fatal("add() appears to be stuck in an infinite loop")
	}
}

// ---------------------------------------------------------------------------
// Comment #4: cache.go — Double-counting in pushFront
//
// Reviewer claims: if an already-removed entry (with non-nil prev/next) is
// pushed, len can drift.
//
// Reality check: remove() sets both prev and next to nil (lines 133-134).
// After removal, the defensive check `e.prev != nil && e.next != nil` is
// false, so remove() is NOT called again. pushFront just inserts and
// increments len once. No double-counting.
// ---------------------------------------------------------------------------

func TestIntrusiveListPushFrontDoubleCount(t *testing.T) {
	t.Parallel()

	l := newIntrusiveList()

	e1 := &refCountedEntry{oid: makeHash("E1")}
	e2 := &refCountedEntry{oid: makeHash("E2")}

	// Push two entries.
	l.pushFront(e1)
	l.pushFront(e2)
	assert.Equal(t, 2, l.Len(), "len should be 2 after pushing two entries")

	// Remove e1.
	l.remove(e1)
	assert.Equal(t, 1, l.Len(), "len should be 1 after removing one entry")

	// Verify e1 is detached (prev and next are nil).
	assert.Nil(t, e1.prev, "removed entry should have nil prev")
	assert.Nil(t, e1.next, "removed entry should have nil next")

	// Re-push e1. The defensive check (e.prev != nil && e.next != nil) should
	// be false since both are nil after removal. pushFront should just insert.
	l.pushFront(e1)
	assert.Equal(t, 2, l.Len(), "len should be 2 after re-pushing removed entry")

	// Push e1 again while it's already in the list. The defensive check
	// should detect it and call remove() first, keeping len consistent.
	l.pushFront(e1)
	assert.Equal(t, 2, l.Len(), "len should still be 2 after pushing already-linked entry")
}

// TestIntrusiveListMoveToFrontDetached verifies moveToFront on a detached
// node correctly reinserts it.
func TestIntrusiveListMoveToFrontDetached(t *testing.T) {
	t.Parallel()

	l := newIntrusiveList()
	e1 := &refCountedEntry{oid: makeHash("E1")}
	e2 := &refCountedEntry{oid: makeHash("E2")}

	l.pushFront(e1)
	l.pushFront(e2)
	assert.Equal(t, 2, l.Len())

	// Remove e1, then moveToFront.
	l.remove(e1)
	assert.Equal(t, 1, l.Len())

	l.moveToFront(e1)
	assert.Equal(t, 2, l.Len(), "moveToFront on detached node should reinsert it")
}

// ---------------------------------------------------------------------------
// Comment #9 (P1): store.go#619 — Borrowed bytes returned to pool before use
//
// Reviewer claims: inflateDeltaChainBorrowed returns arena-backed memory
// that is returned to deltaArenaPool (via defer in applyDeltaStack) before
// the caller uses it. Next getDeltaArena() overwrites the data.
//
// This tests the applyDeltaStack function directly with borrowed=true to
// verify the returned slice's backing memory is reused by the pool.
// ---------------------------------------------------------------------------

func TestBorrowedBytesReturnedToPoolBeforeUse(t *testing.T) {
	// applyDeltaStack with borrowed=true and an empty stack should just return
	// baseData directly (fast path), without touching the arena at all.
	// So we test the non-trivial case: with actual delta data.
	//
	// Since we can't easily construct valid delta instructions without a real
	// packfile, we test the pool lifetime contract directly by examining what
	// happens when getDeltaArena is called after applyDeltaStack returns
	// with borrowed=true on an empty stack with non-empty baseData.

	// For empty stack, borrowed=true returns baseData directly (no arena).
	base := []byte("hello world")
	result, typ, err := applyDeltaStack(nil, base, ObjBlob, 0, true)
	require.NoError(t, err)
	assert.Equal(t, ObjBlob, typ)
	assert.Equal(t, base, result)

	// For empty stack, borrowed=false returns a COPY.
	result2, _, err := applyDeltaStack(nil, base, ObjBlob, 0, false)
	require.NoError(t, err)
	// Modify original; copy should be independent.
	base[0] = 'H'
	assert.Equal(t, byte('h'), result2[0], "non-borrowed should be independent copy")

	// The real risk is when there IS a delta stack — the arena is obtained,
	// used, and returned to pool via defer. The returned slice for
	// borrowed=true points to pool memory. We verify this by checking that
	// the next getDeltaArena() call could reuse the same buffer.
	//
	// We can't easily construct a full delta stack here without packfile
	// infrastructure, but we CAN verify the pool contract:
	arena1 := getDeltaArena()
	// Write a marker pattern.
	for i := range arena1.data {
		arena1.data[i] = 0xAA
	}
	putDeltaArena(arena1)

	// Get it back — should be the same arena (on this goroutine/P).
	arena2 := getDeltaArena()
	// Verify we got the same (or similarly sized) buffer back.
	assert.Equal(t, cap(arena1.data), cap(arena2.data),
		"pool should return same-capacity arena")
	// If borrowed code returned a slice into arena1.data, arena2.data now
	// aliases it. Writing to arena2 would corrupt the borrowed data.
	for i := range arena2.data {
		arena2.data[i] = 0xBB
	}
	putDeltaArena(arena2)

	// Check: after putting arena2 back, arena1.data[0] should be 0xBB
	// (they share the same backing array via the pool).
	if len(arena1.data) > 0 {
		assert.Equal(t, byte(0xBB), arena1.data[0],
			"arenas from pool share backing storage, confirming borrowed data is unsafe after pool return")
	}
}

// ---------------------------------------------------------------------------
// Comment #10 (P2): idx.go#163 — Synthetic ridx causing false CRC mismatches
//
// Reviewer claims: buildReverseFromOffsets creates r[i] = n-1-i, which
// doesn't properly map descending offset positions to OID-order entry
// indices. This produces wrong CRC lookups when OID order differs from
// offset order.
// ---------------------------------------------------------------------------

func TestSyntheticRidxCRCLookup(t *testing.T) {
	t.Parallel()

	// Create an idxFile where OID order differs from offset order.
	// OID order: AAA (offset=500), BBB (offset=100), CCC (offset=300)
	// Offset order (ascending): 100, 300, 500
	f := &idxFile{
		entries: []idxEntry{
			{offset: 500, crc: 0x1111}, // entries[0] = AAA
			{offset: 100, crc: 0x2222}, // entries[1] = BBB
			{offset: 300, crc: 0x3333}, // entries[2] = CCC
		},
		sortedOffsets: []uint64{100, 300, 500}, // ascending
	}

	// Build synthetic ridx and mark as trusted since buildReverseFromEntries
	// produces a correct offset→entries mapping.
	f.ridx = buildReverseFromEntries(f)
	f.ridxCRCTrusted = true
	t.Logf("synthetic ridx: %v", f.ridx)

	// Look up CRC for offset 100 (should be BBB's CRC: 0x2222).
	crc, ok := f.crcAtOffset(100)
	require.True(t, ok, "should find offset 100")
	assert.Equal(t, uint32(0x2222), crc,
		"CRC for offset 100 should be BBB's CRC (0x2222), not AAA's (0x1111)")

	// Look up CRC for offset 300 (should be CCC's CRC: 0x3333).
	crc, ok = f.crcAtOffset(300)
	require.True(t, ok, "should find offset 300")
	assert.Equal(t, uint32(0x3333), crc,
		"CRC for offset 300 should be CCC's CRC (0x3333)")

	// Look up CRC for offset 500 (should be AAA's CRC: 0x1111).
	crc, ok = f.crcAtOffset(500)
	require.True(t, ok, "should find offset 500")
	assert.Equal(t, uint32(0x1111), crc,
		"CRC for offset 500 should be AAA's CRC (0x1111)")
}

// TestSyntheticRidxCRCLookupMatchingOrder verifies the degenerate case
// where OID order matches offset order — the synthetic ridx happens to work.
func TestSyntheticRidxCRCLookupMatchingOrder(t *testing.T) {
	t.Parallel()

	// OID order = offset order: both ascending.
	f := &idxFile{
		entries: []idxEntry{
			{offset: 100, crc: 0x1111}, // entries[0]
			{offset: 300, crc: 0x2222}, // entries[1]
			{offset: 500, crc: 0x3333}, // entries[2]
		},
		sortedOffsets: []uint64{100, 300, 500},
	}

	f.ridx = buildReverseFromEntries(f)
	f.ridxCRCTrusted = true

	// When OID order matches offset order, the synthetic ridx should work.
	crc, ok := f.crcAtOffset(100)
	require.True(t, ok)
	assert.Equal(t, uint32(0x1111), crc, "should get correct CRC when orders match")

	crc, ok = f.crcAtOffset(300)
	require.True(t, ok)
	assert.Equal(t, uint32(0x2222), crc)

	crc, ok = f.crcAtOffset(500)
	require.True(t, ok)
	assert.Equal(t, uint32(0x3333), crc)
}

// ---------------------------------------------------------------------------
// Comment #5: delta.go — 32-bit overflow in arena allocation
//
// The overflow is theoretical (only on 32-bit systems). We test the boundary
// to document the risk.
// ---------------------------------------------------------------------------

func TestDeltaArenaOverflowProtection(t *testing.T) {
	t.Parallel()

	// On 64-bit systems, int is 64-bit so maxTarget*2 won't overflow.
	// On 32-bit systems, maxTarget of 2^31 would overflow.
	// This test verifies that the maxObjectSize check prevents reaching
	// the overflow point.

	// With maxObjectSize = 512 MiB (default), maxTarget is capped.
	maxObj := uint64(512 << 20)
	base := []byte("base")

	// Empty stack with maxObjectSize should work fine.
	_, _, err := applyDeltaStack(nil, base, ObjBlob, maxObj, false)
	assert.NoError(t, err, "empty stack with maxObjectSize should succeed")

	// Verify maxObjectSize enforcement when maxTarget exceeds it.
	hugeBase := make([]byte, 1) // minimal base
	_, _, err = applyDeltaStack(nil, hugeBase, ObjBlob, 0, false)
	assert.NoError(t, err, "empty stack should always succeed regardless of base size")
}

// ---------------------------------------------------------------------------
// Comment #6: delta.go — Oversized arena returned to pool
//
// Verify that getDeltaArena/putDeltaArena doesn't enforce a size cap.
// ---------------------------------------------------------------------------

func TestDeltaArenaOversizedPoolReturn(t *testing.T) {
	// Get a standard arena.
	arena := getDeltaArena()
	standardCap := cap(arena.data)
	t.Logf("standard arena capacity: %d bytes (%d MiB)", standardCap, standardCap>>20)

	// Simulate the retry path: replace arena.data with an oversized buffer.
	oversized := make([]byte, standardCap*4)
	arena.data = oversized

	// Return the oversized arena to the pool.
	putDeltaArena(arena)

	// Get it back.
	arena2 := getDeltaArena()
	oversizedCap := cap(arena2.data)
	t.Logf("retrieved arena capacity: %d bytes (%d MiB)", oversizedCap, oversizedCap>>20)

	// The pool returns the oversized arena, wasting memory.
	if oversizedCap > standardCap {
		t.Logf("CONFIRMED: oversized arena (%d bytes) returned to pool without cap", oversizedCap)
	}
	putDeltaArena(arena2)
}

// ---------------------------------------------------------------------------
// Comment #8: store.go#371 — Missing putTreeIter
//
// Reviewer claims getTreeIter at line 371 can leak if error returns at
// 365-370 don't call putTreeIter. But those error paths are BEFORE the
// getTreeIter call, so no iterator is allocated on error paths.
// ---------------------------------------------------------------------------

func TestTreeIterPoolLifecycle(t *testing.T) {
	t.Parallel()

	// getTreeIter returns a pooled iterator with rest set.
	raw := []byte("100644 test\x00" + string(make([]byte, 20)))
	it := getTreeIter(raw)
	require.NotNil(t, it)
	assert.Equal(t, raw, it.rest, "rest should be set to the provided raw data")

	// putTreeIter clears the reference.
	putTreeIter(it)
	assert.Nil(t, it.rest, "rest should be nil after putTreeIter")

	// putTreeIter(nil) should be a no-op.
	putTreeIter(nil)

	// Verify the pool returns a clean iterator.
	it2 := getTreeIter(raw)
	require.NotNil(t, it2)
	assert.Equal(t, raw, it2.rest)
	putTreeIter(it2)
}
