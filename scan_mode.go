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

		var payload bytes.Buffer
		for i, line := range hunk.lines {
			if i > 0 {
				payload.WriteByte('\n')
			}
			payload.WriteString(line)
		}

		meta := ScanMeta{
			Commit: hunk.commit,
			Path:   hunk.path,
		}
		if err := scanner.ScanBlob(bytes.NewReader(payload.Bytes()), meta); err != nil {
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
