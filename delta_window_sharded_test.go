// delta_window_sharded_test.go tests the sharded delta window, which
// distributes cached delta base objects across multiple independent shards to
// reduce lock contention during concurrent pack resolution. Each shard owns a
// portion of the total memory budget and objects are routed to shards based on
// the first byte of their OID.

package objstore

import "testing"

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
