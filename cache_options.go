package objstore

// WithHunkDedupBudget bounds the line-fingerprint table retained during one
// deduplicating hunk scan. The table grows on demand up to this budget and
// then fails open, emitting rather than suppressing uncertain hunks.
func WithHunkDedupBudget(bytes int) ScannerOption {
	bytes = max(bytes, 0)
	return func(hs *HistoryScanner) {
		hs.hunkDedupBudget = bytes
	}
}

// WithHunkDedupRetainedBudget bounds the hunk bytes a deduplicating scan
// holds between a worker computing them and the consumer callback seeing
// them. The sequencer stops handing out new pairs while the charge is above
// this budget, so the bound trades scan throughput for peak memory. The
// default of 512 MiB keeps every worker busy through a multi-megabyte blob
// on large hosts. Values below 1 MiB are raised to 1 MiB so a single large
// hunk can always be admitted.
func WithHunkDedupRetainedBudget(bytes int) ScannerOption {
	bytes = max(bytes, 1<<20)
	return func(hs *HistoryScanner) {
		hs.dedupLimits.retainedBytesCap = int64(bytes)
	}
}
