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
