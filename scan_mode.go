package objstore

import (
	"bytes"
	"fmt"
)

// ScanMode selects the high-level scanning strategy used by HistoryScanner.Scan.
type ScanMode uint8

const (
	// ScanModeBlob scans full new blob objects once (OID-deduped, pack-sorted).
	ScanModeBlob ScanMode = iota

	// ScanModeHunks scans legacy added-line hunks from commit diffs.
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
func (hs *HistoryScanner) SetScanMode(mode ScanMode) {
	hs.scanMode = mode
}

// Scan runs the configured scan mode.
//
// Blob mode is the default and is the recommended path for secret scanning.
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
