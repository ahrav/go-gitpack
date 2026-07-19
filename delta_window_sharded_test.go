// delta_window_sharded_test.go tests the sharded delta window, which
// distributes cached delta base objects across multiple independent shards to
// reduce lock contention during concurrent pack resolution. Each shard owns a
// portion of the total memory budget and objects are routed to shards based on
// the first byte of their OID.

package objstore

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardedDeltaWindow_BasicAddAcquire verifies the fundamental add/acquire
// round-trip through the sharded delta window.
//
// Configuration values used in this test:
//   - 8 shards: a power-of-two count that exercises the bitmask-based shard
//     selection (shard = oid[0] & (numShards-1)).
//   - 8<<20 (8 MiB) total budget: the aggregate memory limit shared equally
//     across all shards (1 MiB per shard).
//   - oid[0] = 0x7f: the first byte of the object ID, which selects shard 7
//     (0x7f & 0x07 == 7) and ensures we exercise a non-zero shard index.
//
// The test adds a small blob and then acquires it back, asserting that both
// the data content and object type survive the round-trip through the cache.
func TestShardedDeltaWindow_BasicAddAcquire(t *testing.T) {
	w := newShardedDeltaWindow(8, 8<<20)

	var oid Hash
	oid[0] = 0x7f
	data := []byte("hello")
	if err := w.add(oid, data, ObjBlob); err != nil {
		t.Fatalf("add: %v", err)
	}

	h, ok := w.acquire(oid)
	if !ok {
		t.Fatalf("expected cache hit")
	}
	defer h.Release()

	if got := string(h.Data()); got != "hello" {
		t.Fatalf("unexpected data: %q", got)
	}
	if got := h.Type(); got != ObjBlob {
		t.Fatalf("unexpected type: %v", got)
	}
}

func TestReleaseAddEvictableRace(t *testing.T) {
	t.Parallel()

	w := newRefCountedDeltaWindow()
	w.budget = 1000

	const numEntries = 5
	oids := make([]Hash, numEntries)
	for i := range oids {
		oids[i] = makeHash(string(rune('A' + i)))
	}

	for _, oid := range oids {
		require.NoError(t, w.add(oid, make([]byte, 200), ObjBlob))
	}

	var (
		spuriousFull atomic.Int64
		wg           sync.WaitGroup
		iterations   = 10_000
	)

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

	fullCount := spuriousFull.Load()
	t.Logf("ErrWindowFull count: %d / %d", fullCount, iterations)

	assert.Less(t, fullCount, int64(iterations/2),
		"too many spurious ErrWindowFull errors suggest a TOCTOU race")
}

func TestEvictionDoesNotInfiniteLoop(t *testing.T) {
	t.Parallel()

	w := newRefCountedDeltaWindow()
	w.budget = 100

	require.NoError(t, w.add(makeHash("A"), make([]byte, 60), ObjBlob))

	h, ok := w.acquire(makeHash("A"))
	require.True(t, ok)
	defer h.Release()

	done := make(chan error, 1)
	go func() {
		done <- w.add(makeHash("B"), make([]byte, 60), ObjBlob)
	}()

	// A generous wall-clock deadline: add() either returns ErrWindowFull
	// promptly or is genuinely looping. The previous watchdog raced 200
	// runtime.Gosched() yields against goroutine scheduling, which falsely
	// fired on loaded CI runners before add() ever ran.
	select {
	case err := <-done:
		assert.ErrorIs(t, err, ErrWindowFull, "should return ErrWindowFull, not loop forever")
	case <-time.After(10 * time.Second):
		t.Fatal("add() appears to be stuck in an infinite loop")
	}
}

func TestIntrusiveListPushFrontDoubleCount(t *testing.T) {
	t.Parallel()

	l := newIntrusiveList()

	e1 := &refCountedEntry{oid: makeHash("E1")}
	e2 := &refCountedEntry{oid: makeHash("E2")}

	l.pushFront(e1)
	l.pushFront(e2)
	assert.Equal(t, 2, l.Len(), "len should be 2 after pushing two entries")

	l.remove(e1)
	assert.Equal(t, 1, l.Len(), "len should be 1 after removing one entry")

	assert.Nil(t, e1.prev, "removed entry should have nil prev")
	assert.Nil(t, e1.next, "removed entry should have nil next")

	l.pushFront(e1)
	assert.Equal(t, 2, l.Len(), "len should be 2 after re-pushing removed entry")

	l.pushFront(e1)
	assert.Equal(t, 2, l.Len(), "len should still be 2 after pushing already-linked entry")
}

func TestIntrusiveListMoveToFrontDetached(t *testing.T) {
	t.Parallel()

	l := newIntrusiveList()
	e1 := &refCountedEntry{oid: makeHash("E1")}
	e2 := &refCountedEntry{oid: makeHash("E2")}

	l.pushFront(e1)
	l.pushFront(e2)
	assert.Equal(t, 2, l.Len())

	l.remove(e1)
	assert.Equal(t, 1, l.Len())

	l.moveToFront(e1)
	assert.Equal(t, 2, l.Len(), "moveToFront on detached node should reinsert it")
}
