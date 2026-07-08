package objstore

// WithPairCacheBudget bounds the retained diff-hunk memo for hunk scans.
//
// A zero or negative budget disables the cache. The budget is approximate:
// entries are charged for retained line bytes plus slice/string headers, and
// eviction is shard-local to keep cache hits cheap under parallel hunk workers.
func WithPairCacheBudget(bytes int) ScannerOption {
	return func(hs *HistoryScanner) {
		if hs.pairs == nil {
			hs.pairs = newPairCacheWithBudget(bytes)
			return
		}
		hs.pairs.setBudget(bytes)
	}
}

// WithOffsetCacheBudget bounds the pack-offset materialization cache.
//
// The cache is keyed by (pack, offset) and primarily accelerates ofs-delta
// chain walks. A zero or negative budget disables new insertions while keeping
// lookups safe for in-flight readers.
func WithOffsetCacheBudget(bytes int) ScannerOption {
	return func(hs *HistoryScanner) {
		if hs.store == nil {
			return
		}
		if hs.store.offCache == nil {
			hs.store.offCache = newOffsetCacheWithBudget(bytes)
			return
		}
		hs.store.offCache.setBudget(bytes)
	}
}

// WithDeltaArenaBudget bounds idle process-wide delta arena retention.
//
// Multi-hop delta reconstruction borrows 32 MiB ping-pong arenas. The arena
// free-list deliberately survives GC to avoid repeated large allocations, but
// without a budget it can retain one arena per active delta worker after a
// scan. This option caps only idle retained arenas; in-flight delta resolutions
// may still allocate the arenas they actively use. The budget is rounded down
// to a whole arena count. A zero or negative budget disables idle retention.
func WithDeltaArenaBudget(bytes int) ScannerOption {
	return func(hs *HistoryScanner) {
		_ = hs // The arena pool is process-wide, not scanner-local.
		setDeltaArenaRetainLimit(bytes / defaultDeltaArenaSize)
	}
}
