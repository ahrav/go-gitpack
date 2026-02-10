package objstore

type deltaWindow interface {
	acquire(oid Hash) (*Handle, bool)
	add(oid Hash, buf []byte, typ ObjectType) error
}

type shardedDeltaWindow struct {
	shards []*refCountedDeltaWindow
	mask   byte
}

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

func (w *shardedDeltaWindow) shard(oid Hash) *refCountedDeltaWindow {
	return w.shards[int(oid[0]&w.mask)]
}

func (w *shardedDeltaWindow) acquire(oid Hash) (*Handle, bool) {
	return w.shard(oid).acquire(oid)
}

func (w *shardedDeltaWindow) add(oid Hash, buf []byte, typ ObjectType) error {
	return w.shard(oid).add(oid, buf, typ)
}
