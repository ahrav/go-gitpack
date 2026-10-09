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
