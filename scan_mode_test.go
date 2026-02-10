// scan_mode_test.go tests the scan mode abstraction of HistoryScanner.
//
// A scan mode controls what granularity of data the scanner emits:
//   - ScanModeBlob (default): emits whole blob objects with their OIDs.
//   - ScanModeHunks: emits diff hunks with commit/path metadata but without
//     individual blob OIDs.
//
// These tests verify mode selection (default, option, runtime switch), the
// metadata shape produced by each mode, and the error path for unsupported
// mode values.
//
// Cross-file dependencies:
//   - createScannerForRepo (history_scanner_test.go): constructs a
//     HistoryScanner pointed at a repository under testdata/repos/<name>.
//   - recordingBlobScanner (scan_plan_test.go): a test double that records
//     every BlobMeta it receives, used here to inspect scan output.
//   - "simple-linear" (testdata/repos/simple-linear): a small Git repository
//     with a linear commit history used as the fixture for all scan-mode tests.

package objstore

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestHistoryScanner_DefaultScanModeIsBlob verifies that a newly created
// HistoryScanner defaults to ScanModeBlob when no WithScanMode option is
// provided.
func TestHistoryScanner_DefaultScanModeIsBlob(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	if got := scanner.ScanMode(); got != ScanModeBlob {
		t.Fatalf("default scan mode = %s, want %s", got, ScanModeBlob)
	}
}

// TestHistoryScanner_WithScanModeOption confirms that the WithScanMode
// functional option correctly overrides the default scan mode at construction
// time.
func TestHistoryScanner_WithScanModeOption(t *testing.T) {
	repoPath := filepath.Join("testdata", "repos", "simple-linear")
	scanner, err := NewHistoryScanner(repoPath, WithScanMode(ScanModeHunks))
	if err != nil {
		t.Fatalf("NewHistoryScanner: %v", err)
	}
	defer scanner.Close()

	if got := scanner.ScanMode(); got != ScanModeHunks {
		t.Fatalf("scan mode = %s, want %s", got, ScanModeHunks)
	}
}

// TestHistoryScanner_Scan_DefaultUsesBlobMode performs a full scan using the
// default blob mode and verifies that every emitted BlobMeta has a non-zero
// Blob OID, confirming that complete blob objects were resolved.
func TestHistoryScanner_Scan_DefaultUsesBlobMode(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	seen := &memSeenSet{m: make(map[Hash]struct{})}
	rec := &recordingBlobScanner{}
	if err := scanner.Scan(seen, rec); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(rec.metas) == 0 {
		t.Fatalf("expected at least one scanned item")
	}
	for _, meta := range rec.metas {
		if meta.Blob.IsZero() {
			t.Fatalf("expected blob mode to provide non-zero blob OID")
		}
	}
}

// TestHistoryScanner_Scan_HunkMode switches the scanner to ScanModeHunks and
// verifies the metadata shape: blob OIDs should be zero (hunks are not
// associated with individual blob objects), while commit OIDs and file paths
// must be populated.
func TestHistoryScanner_Scan_HunkMode(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()
	scanner.SetScanMode(ScanModeHunks)

	rec := &recordingBlobScanner{}
	if err := scanner.Scan(nil, rec); err != nil {
		t.Fatalf("Scan in hunk mode: %v", err)
	}
	if len(rec.metas) == 0 {
		t.Fatalf("expected at least one scanned hunk")
	}
	for _, meta := range rec.metas {
		if !meta.Blob.IsZero() {
			t.Fatalf("expected hunk mode meta blob OID to be zero, got %s", meta.Blob)
		}
		if meta.Commit.IsZero() {
			t.Fatalf("expected hunk mode meta commit OID")
		}
		if meta.Path == "" {
			t.Fatalf("expected hunk mode meta path")
		}
	}
}

// TestHistoryScanner_Scan_UnsupportedMode verifies that calling Scan with an
// invalid ScanMode value (e.g., ScanMode(99)) returns an "unsupported scan
// mode" error rather than panicking or silently succeeding.
func TestHistoryScanner_Scan_UnsupportedMode(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()
	scanner.SetScanMode(ScanMode(99))

	err := scanner.Scan(nil, &recordingBlobScanner{})
	if err == nil || !strings.Contains(err.Error(), "unsupported scan mode") {
		t.Fatalf("expected unsupported scan mode error, got %v", err)
	}
}
