package objstore

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestHistoryScanner_DefaultScanModeIsBlob(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	if got := scanner.ScanMode(); got != ScanModeBlob {
		t.Fatalf("default scan mode = %s, want %s", got, ScanModeBlob)
	}
}

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

func TestHistoryScanner_Scan_UnsupportedMode(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()
	scanner.SetScanMode(ScanMode(99))

	err := scanner.Scan(nil, &recordingBlobScanner{})
	if err == nil || !strings.Contains(err.Error(), "unsupported scan mode") {
		t.Fatalf("expected unsupported scan mode error, got %v", err)
	}
}
