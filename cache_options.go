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

// DeltaArenaSize is the size in bytes of one delta ping-pong arena. Budgets
// passed to SetDeltaArenaBudget are whole multiples of it.
const DeltaArenaSize = defaultDeltaArenaSize

// SetDeltaArenaBudget bounds the memory held idle by the delta arena free-list
// and returns the budget that was in effect before the call, in bytes.
//
// Multi-hop delta reconstruction borrows DeltaArenaSize ping-pong arenas from a
// free-list that deliberately survives GC, because re-allocating (and zeroing)
// an arena per resolution costs ~45% of a bulk scan's allocated bytes. The
// budget caps only the idle population: an in-flight resolution still allocates
// the arena it works in, so it bounds the floor, not the peak.
//
// The budget is rounded DOWN to a whole number of DeltaArenaSize arenas, so any
// budget below DeltaArenaSize — like a plain 1<<20 — disables idle retention
// entirely and makes every multi-hop resolution pay for a fresh arena. It is
// also clamped to 1 GiB. Compute budgets as a multiple of DeltaArenaSize.
//
// The free-list is process-wide, not per-scanner: this call affects every
// HistoryScanner and every other user of the package in the process, and it
// stays in effect until changed again. Pass the returned value back to restore
// the previous budget.
func SetDeltaArenaBudget(bytes int) (previous int) {
	return setDeltaArenaRetainLimit(bytes/DeltaArenaSize) * DeltaArenaSize
}
