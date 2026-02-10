// delta_window_sharded.go
//
// Sharded delta-base window for reducing lock contention during concurrent
// pack-object resolution.
//
// When multiple goroutines resolve delta-compressed objects simultaneously,
// a single shared window becomes a serialization bottleneck because every
// acquire and add operation must take the same lock. Sharding the window by
// the first byte of the object ID distributes that contention across N
// independent refCountedDeltaWindow instances, each with its own lock and
// LRU budget. Because Git object IDs are uniformly distributed, the load
// across shards is approximately even without any additional hashing.
package objstore

// deltaWindow is the contract that any delta-base cache must satisfy.
//
// Semantics:
//   - acquire looks up an already-cached base object by OID. On a hit it
//     returns a Handle whose reference count has been incremented; the caller
//     must call Handle.Release when done. On a miss it returns (nil, false).
//   - add inserts a resolved base object into the window. If the window is
//     over budget the implementation may evict entries with zero references.
//     The caller transfers ownership of buf to the window on success.
//
// Implementations must be safe for concurrent use from multiple goroutines.
type deltaWindow interface {
	acquire(oid Hash) (*Handle, bool)
	add(oid Hash, buf []byte, typ ObjectType) error
}

// shardedDeltaWindow fans out deltaWindow operations across a power-of-two
// number of independent refCountedDeltaWindow instances. Each shard has its
// own mutex and eviction budget, so concurrent goroutines that happen to
// resolve objects in different shards never contend on the same lock.
//
// Thread safety: all exported and unexported methods are safe for concurrent
// use because they delegate to per-shard locks; the shardedDeltaWindow
// struct itself is immutable after construction.
type shardedDeltaWindow struct {
	shards []*refCountedDeltaWindow
	// mask is numShards-1 and is used as a bitmask to map the first byte of
	// an OID to a shard index (oid[0] & mask). This works because numShards
	// is required to be a power of two.
	mask byte
}

// newShardedDeltaWindow creates a sharded delta window with numShards
// independent refCountedDeltaWindow instances, splitting totalBudget evenly
// among them.
//
// numShards must be a power of two in [1, 256]; the function panics
// otherwise.
//
// Budget floor: if totalBudget / numShards falls below 1 MiB the per-shard
// budget is clamped to 1 MiB. This prevents pathologically small windows
// that would thrash eviction on every add.
func newShardedDeltaWindow(numShards, totalBudget int) *shardedDeltaWindow {
	if numShards <= 0 || numShards > 256 || numShards&(numShards-1) != 0 {
		panic("numShards must be a power-of-two in [1, 256]")
	}

	perShard := totalBudget / numShards
	const minBudgetPerShard = 1 << 20 // 1 MiB
	if perShard < minBudgetPerShard {
		perShard = minBudgetPerShard
	}

	shards := make([]*refCountedDeltaWindow, numShards)
	for i := range shards {
		w := newRefCountedDeltaWindow()
		w.budget = perShard
		shards[i] = w
	}

	return &shardedDeltaWindow{
		shards: shards,
		mask:   byte(numShards - 1),
	}
}

// shard maps an object ID to its owning refCountedDeltaWindow by masking the
// first byte of the OID. Because SHA-1 (and SHA-256) object IDs are uniformly
// distributed, the first byte alone provides sufficient entropy to balance
// load across up to 256 shards.
func (w *shardedDeltaWindow) shard(oid Hash) *refCountedDeltaWindow {
	return w.shards[int(oid[0]&w.mask)]
}

func (w *shardedDeltaWindow) acquire(oid Hash) (*Handle, bool) {
	return w.shard(oid).acquire(oid)
}

func (w *shardedDeltaWindow) add(oid Hash, buf []byte, typ ObjectType) error {
	return w.shard(oid).add(oid, buf, typ)
}
