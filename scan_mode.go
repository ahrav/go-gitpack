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
	"sync"
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
// It drives DiffHistoryHunksFunc, so no queue sits between a blob worker and
// scanner.ScanBlob. DiffHistoryHunksFunc invokes the callback concurrently
// from every blob worker, and one mutex serializes the whole per-hunk body.
// Both halves need it: the payload buffer is reused across hunks, which
// BlobScanner permits because an implementation may not retain the reader's
// bytes past the call, and ScanBlob is handed one hunk at a time to match
// every other ScanBlob call site in this package.
//
// A scan error aborts the walk, and DiffHistoryHunksFunc returns the first
// error observed, whether it came from a scan or from the walk itself.
func (hs *HistoryScanner) scanHunks(scanner BlobScanner) error {
	var (
		mu      sync.Mutex
		payload bytes.Buffer
	)

	return hs.DiffHistoryHunksFunc(func(hunk HunkAddition) error {
		mu.Lock()
		defer mu.Unlock()

		payload.Reset()
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
			return fmt.Errorf("scan hunk %s:%s:%d-%d: %w",
				hunk.commit, hunk.path, hunk.startLine, hunk.endLine, err)
		}
		return nil
	})
}
