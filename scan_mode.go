// scan_mode.go
//
// Scanning strategy selection for HistoryScanner.
//
// Two modes are supported:
//   - ScanModeBlob  (default) -- iterates every unique blob introduced across
//     the commit history in pack-file offset order, yielding the full blob
//     body exactly once per OID. This is the recommended and fastest path.
//   - ScanModeHunks (legacy)  -- computes per-commit diffs and yields only the
//     added-line hunks. Retained for backward compatibility with callers that
//     require line-level attribution, but significantly slower because it must
//     diff every parent-child commit pair.
package objstore

import (
	"bytes"
	"fmt"
	"strings"
)

// ScanMode selects the high-level scanning strategy used by HistoryScanner.Scan.
type ScanMode uint8

const (
	// ScanModeBlob scans full blob objects, deduplicating by OID and visiting
	// them in pack-file offset order. Pack-sorted iteration minimizes random
	// I/O because entries stored contiguously in the pack are read
	// sequentially, which is especially beneficial on spinning disks and
	// over NFS.
	ScanModeBlob ScanMode = iota

	// ScanModeHunks is the legacy scanning mode that computes parent-child
	// diffs for every commit and yields only the added-line hunks. It exists
	// for backward compatibility with callers that need line-level
	// granularity. Prefer ScanModeBlob for new integrations because it
	// avoids the overhead of diff computation and tree comparison.
	ScanModeHunks
)

func (m ScanMode) String() string {
	switch m {
	case ScanModeBlob:
		return "blob"
	case ScanModeHunks:
		return "hunks"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(m))
	}
}

// WithScanMode configures the default mode used by HistoryScanner.Scan.
func WithScanMode(mode ScanMode) ScannerOption {
	return func(hs *HistoryScanner) {
		hs.scanMode = mode
	}
}

// WithSkipMergeDiffs makes hunk scans emit no diffs for merge commits,
// matching `git log -p` semantics. See HistoryScanner.skipMergeDiffs.
func WithSkipMergeDiffs(skip bool) ScannerOption {
	return func(hs *HistoryScanner) {
		hs.skipMergeDiffs = skip
	}
}

// WithHunkLineDedup makes hunk scans suppress hunks whose added lines were
// all seen earlier in history, so each unique added line is emitted at its
// first introduction.
//
// The option affects hunk scans only (DiffHistoryHunks, DiffHistoryHunksFunc,
// and Scan in ScanModeHunks); blob-mode scans are unchanged. When enabled:
//
//   - Dedup granularity is the whole hunk: a hunk is emitted intact iff it
//     contains at least one line not seen at an earlier point in the
//     deterministic parent-first order over the commit history. Suppression
//     means every line of the hunk was already seen at an earlier point in
//     that order; consumers that need every per-commit occurrence should
//     use the default mode.
//   - Hunks are never split: StartLine/EndLine/Lines semantics are
//     unchanged from the default mode, and a multi-line secret contiguous
//     within one hunk is never truncated by dedup.
//   - fn may still be invoked concurrently: only the dedup decisions are
//     serialized internally. Output is deterministic as a multiset of
//     emissions, never as an order.
//   - If the internal fingerprint table saturates, dedup fails open: hunks
//     are emitted rather than dropped, and the scan never errors for this
//     reason.
//   - With WithHunkPathFilter, filtered pairs never mark the fingerprint
//     set, so each unique line is emitted at its first unskipped
//     introduction.
//   - Merging in foreign history with older timestamps can insert commits
//     earlier in the parent-first order and re-attribute a line's first
//     introduction — inherent to first-introduction semantics.
//
// Fingerprint-table size and the pipeline's look-ahead window are internal
// constants.
//
// The default (false) preserves the per-commit emission behavior of the
// plain scanner byte-for-byte.
func WithHunkLineDedup(dedup bool) ScannerOption {
	return func(hs *HistoryScanner) {
		hs.hunkLineDedup = dedup
	}
}

// WithHunkPathFilter installs a pair-level path filter for hunk scans.
//
// The option affects hunk scans only (DiffHistoryHunks, DiffHistoryHunksFunc,
// and Scan in ScanModeHunks); blob-mode scans are unchanged. When skip is
// non-nil, every changed blob pair is offered to skip with the introducing
// commit and the post-image path — the same path the resulting
// HunkAddition.Path() would report — and pairs reporting true are dropped
// BEFORE diffing, so filtered paths also skip diff cost in both dedup and
// non-dedup hunk scans.
//
// In dedup mode (WithHunkLineDedup), dropped pairs never mark the
// fingerprint set, so each unique line is emitted at its first UNSKIPPED
// introduction. Without this, a consumer that discards emissions from
// filtered paths after the fact would lose lines whose first introduction
// happens to be in a filtered path: the set would already have marked them
// seen, suppressing every later occurrence in paths the consumer keeps.
//
// skip MUST be a pure, deterministic function of its arguments and safe for
// concurrent calls — it is invoked from multiple workers, and the scanner's
// determinism guarantees only hold under that contract.
//
// A nil skip means no filtering (the default) and preserves existing
// behavior byte-for-byte.
func WithHunkPathFilter(skip func(commit Hash, path string) bool) ScannerOption {
	return func(hs *HistoryScanner) {
		hs.hunkPathFilter = skip
	}
}

// ScanMode returns the scanner's currently configured scan mode.
func (hs *HistoryScanner) ScanMode() ScanMode {
	return hs.scanMode
}

// SetScanMode updates the scanner's scan mode for subsequent Scan calls.
//
// Thread safety: SetScanMode is not safe for concurrent use with Scan. The
// caller must ensure no Scan is in progress when changing the mode.
func (hs *HistoryScanner) SetScanMode(mode ScanMode) {
	hs.scanMode = mode
}

// Scan runs the scanning strategy selected by the scanner's current ScanMode.
//
// Blob mode (ScanModeBlob, the default) is the recommended path for secret
// scanning. It visits every unique blob exactly once, in pack-offset order,
// and passes its full content to scanner.ScanBlob.
//
// Hunk mode (ScanModeHunks) diffs each commit against its parent and yields
// only the added lines. It is retained for backward compatibility.
func (hs *HistoryScanner) Scan(seen SeenSet, scanner BlobScanner) error {
	if scanner == nil {
		return fmt.Errorf("scanner is nil")
	}

	switch hs.scanMode {
	case ScanModeBlob:
		return hs.scanBlobsStreaming(seen, scanner)
	case ScanModeHunks:
		return hs.scanHunks(scanner)
	default:
		return fmt.Errorf("unsupported scan mode: %s", hs.scanMode)
	}
}

// scanHunks implements the legacy hunk-based scanning mode.
//
// It calls DiffHistoryHunks, which produces hunks on a channel and sends
// a single error on errC when the walk completes. The loop drains the hunks
// channel to completion even after the first scan error, because the
// producer goroutine blocks on sends and would leak if the consumer stopped
// reading early.
//
// Error precedence: a scan-side error (scanErr) takes priority over the
// walk-side error (runErr) so the caller sees the first failure in the
// scanning pipeline rather than a secondary channel-close error.
func (hs *HistoryScanner) scanHunks(scanner BlobScanner) error {
	hunks, errC := hs.DiffHistoryHunks()

	var scanErr error
	for hunk := range hunks {
		if scanErr != nil {
			continue
		}

		meta := ScanMeta{
			Commit: hunk.commit,
			Path:   hunk.path,
		}
		var err error
		if hunk.isBinary && len(hunk.lines) == 1 {
			err = scanner.ScanBlob(strings.NewReader(hunk.lines[0]), meta)
		} else {
			var payload bytes.Buffer
			for i, line := range hunk.lines {
				if i > 0 {
					payload.WriteByte('\n')
				}
				payload.WriteString(line)
			}
			err = scanner.ScanBlob(bytes.NewReader(payload.Bytes()), meta)
		}
		if err != nil {
			scanErr = fmt.Errorf("scan hunk %s:%s:%d-%d: %w",
				hunk.commit, hunk.path, hunk.startLine, hunk.endLine, err)
		}
	}

	runErr := <-errC
	if scanErr != nil {
		return scanErr
	}
	return runErr
}
