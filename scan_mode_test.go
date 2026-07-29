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
//     with a linear commit history used as the fixture for the mode-selection
//     and metadata-shape tests.
//   - "large-repo" (testdata/repos/large-repo) and "very-large-repo-1k": 100-
//     and 1,000-commit repositories, used where a test needs enough concurrent
//     hunk work to expose an ordering or synchronisation defect.

package objstore

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// blobScannerFunc adapts a function to BlobScanner so a test can assert on the
// bytes ScanBlob actually receives without declaring a named double.
type blobScannerFunc func(r io.Reader, meta ScanMeta) error

func (f blobScannerFunc) ScanBlob(r io.Reader, meta ScanMeta) error { return f(r, meta) }

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

// probeBlobScanner is a BlobScanner test double that counts its calls, records
// the highest number of them ever in flight at once, and rejects every call
// when fail is non-nil.
//
// The yields before a successful return widen the window in which an
// overlapping call would be seen, and they cannot manufacture a failure: a
// caller that holds a lock across the call reports a peak of one however often
// the body yields.
type probeBlobScanner struct {
	fail     error
	inFlight atomic.Int32
	peak     atomic.Int32
	calls    atomic.Int64
}

func (s *probeBlobScanner) ScanBlob(r io.Reader, _ ScanMeta) error {
	s.calls.Add(1)

	inFlight := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	for {
		peak := s.peak.Load()
		if inFlight <= peak || s.peak.CompareAndSwap(peak, inFlight) {
			break
		}
	}

	if s.fail != nil {
		return s.fail
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		return err
	}
	for range 64 {
		runtime.Gosched()
	}
	return nil
}

// TestHistoryScanner_Scan_HunkModeSerializesScanBlob proves hunk mode calls
// ScanBlob one hunk at a time.
//
// Hunk mode feeds its callback from every blob worker, so without
// synchronisation it would be the only concurrent ScanBlob caller in the
// package: every other call site delivers one blob at a time, and the
// package's own recordingBlobScanner appends to a slice with no lock. The
// 1,000-commit fixture supplies enough hunks for real overlap.
func TestHistoryScanner_Scan_HunkModeSerializesScanBlob(t *testing.T) {
	scanner := createScannerForRepo(t, "very-large-repo-1k")
	defer scanner.Close()
	scanner.SetScanMode(ScanModeHunks)

	probe := &probeBlobScanner{}
	if err := scanner.Scan(nil, probe); err != nil {
		t.Fatalf("Scan in hunk mode: %v", err)
	}

	if probe.calls.Load() == 0 {
		t.Fatal("fixture produced no hunks, so serialisation was never exercised")
	}
	if got := probe.peak.Load(); got != 1 {
		t.Fatalf("peak concurrent ScanBlob calls = %d, want 1", got)
	}
}

// TestHistoryScanner_Scan_HunkModeAbortsOnScanError proves a scan error ends the
// walk and reaches the caller.
//
// The callback's error unwinds the blob worker that produced the hunk and stops
// the pipeline, so at most one failing call per blob worker reaches the scanner
// no matter how many hunks the history still holds. Every wrap on the path from
// ScanBlob to Scan uses %w, so the scanner's own error stays reachable with
// errors.Is.
func TestHistoryScanner_Scan_HunkModeAbortsOnScanError(t *testing.T) {
	scanner := createScannerForRepo(t, "very-large-repo-1k")
	defer scanner.Close()
	scanner.SetScanMode(ScanModeHunks)

	// A full scan supplies the hunk count the aborted scan is measured
	// against, so the bound below is derived from the fixture rather than
	// hard-coded.
	full := &probeBlobScanner{}
	if err := scanner.Scan(nil, full); err != nil {
		t.Fatalf("Scan in hunk mode: %v", err)
	}
	total := full.calls.Load()

	workers := int64(runtime.NumCPU())
	if total <= 2*workers {
		t.Skipf("fixture delivers %d hunks to %d blob workers: too few to tell an abort from a full scan",
			total, workers)
	}

	want := errors.New("scan failed")
	aborting := &probeBlobScanner{fail: want}
	err := scanner.Scan(nil, aborting)
	if !errors.Is(err, want) {
		t.Fatalf("Scan error = %v, want an error wrapping %v", err, want)
	}
	if got := aborting.calls.Load(); got < 1 || got > workers {
		t.Fatalf("ScanBlob calls before the abort = %d, want 1..%d (one per blob worker) out of %d hunks",
			got, workers, total)
	}
}

// TestHistoryScanner_Scan_HunkModeMatchesHunkStream proves hunk mode delivers
// exactly the hunks DiffHistoryHunks streams: one ScanBlob call per hunk, with
// the same commit and path.
//
// Counts per (commit, path) pair are compared rather than sequences, because
// neither path promises an order across files or commits.
func TestHistoryScanner_Scan_HunkModeMatchesHunkStream(t *testing.T) {
	scanner := createScannerForRepo(t, "large-repo")
	defer scanner.Close()

	type pair struct {
		commit Hash
		path   string
	}

	streamed := make(map[pair]int)
	hunks, errC := scanner.DiffHistoryHunks()
	for hunk := range hunks {
		streamed[pair{commit: hunk.Commit(), path: hunk.Path()}]++
	}
	if err := <-errC; err != nil {
		t.Fatalf("DiffHistoryHunks: %v", err)
	}
	if len(streamed) == 0 {
		t.Fatal("expected the hunk stream to deliver at least one hunk")
	}

	scanner.SetScanMode(ScanModeHunks)
	rec := &recordingBlobScanner{}
	if err := scanner.Scan(nil, rec); err != nil {
		t.Fatalf("Scan in hunk mode: %v", err)
	}

	scanned := make(map[pair]int, len(streamed))
	for _, meta := range rec.metas {
		scanned[pair{commit: meta.Commit, path: meta.Path}]++
	}

	for key, want := range streamed {
		if got := scanned[key]; got != want {
			t.Errorf("%s:%s scanned %d times, streamed %d times", key.commit, key.path, got, want)
		}
	}
	for key, got := range scanned {
		if _, ok := streamed[key]; !ok {
			t.Errorf("%s:%s scanned %d times but never streamed", key.commit, key.path, got)
		}
	}
}

// TestReleaseOversizedPayload covers the retention bound scanHunks relies on:
// a payload buffer is reused across hunks, so without this the largest hunk a
// scan ever assembled would stay resident until the scan returned.
func TestReleaseOversizedPayload(t *testing.T) {
	t.Run("keeps a buffer at the limit", func(t *testing.T) {
		var payload bytes.Buffer
		payload.Grow(maxReusedHunkPayloadBytes)
		payload.WriteString("small")
		capBefore := payload.Cap()

		if released := releaseOversizedPayload(&payload); released {
			t.Fatalf("released a buffer of capacity %d, limit is %d",
				capBefore, maxReusedHunkPayloadBytes)
		}
		if payload.Cap() != capBefore {
			t.Fatalf("capacity changed from %d to %d", capBefore, payload.Cap())
		}
	})

	t.Run("releases a buffer past the limit", func(t *testing.T) {
		var payload bytes.Buffer
		payload.Write(make([]byte, maxReusedHunkPayloadBytes+1))
		if payload.Cap() <= maxReusedHunkPayloadBytes {
			t.Fatalf("fixture did not exceed the limit: cap %d", payload.Cap())
		}

		if released := releaseOversizedPayload(&payload); !released {
			t.Fatalf("retained a buffer of capacity %d, limit is %d",
				payload.Cap(), maxReusedHunkPayloadBytes)
		}
		if payload.Cap() != 0 {
			t.Fatalf("expected the array to be dropped, cap is %d", payload.Cap())
		}
		if payload.Len() != 0 {
			t.Fatalf("expected an empty buffer, len is %d", payload.Len())
		}
	})

	// The released buffer must still be usable: scanHunks assembles the next
	// hunk into it without re-initializing.
	t.Run("released buffer is reusable", func(t *testing.T) {
		var payload bytes.Buffer
		payload.Write(make([]byte, maxReusedHunkPayloadBytes+1))
		releaseOversizedPayload(&payload)

		payload.WriteString("next hunk")
		if got := payload.String(); got != "next hunk" {
			t.Fatalf("payload = %q, want %q", got, "next hunk")
		}
	})
}

// TestHistoryScanner_Scan_HunkModeDeliversPayloadAfterRelease drives hunk mode
// over an addition larger than maxReusedHunkPayloadBytes followed by a small
// one, so the release path runs between two real ScanBlob calls. Both payloads
// must arrive intact — a release that dropped bytes still in flight, or left
// the buffer unusable, shows up here.
func TestHistoryScanner_Scan_HunkModeDeliversPayloadAfterRelease(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git executable not found in PATH")
	}

	repo := t.TempDir()
	run := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t",
			"GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t",
			"GIT_COMMITTER_EMAIL=t@example.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v failed: %s", args, string(out))
		}
	}
	run("init", "--quiet")

	// One line per 8 bytes keeps the assembled payload (lines joined by '\n')
	// just over the release threshold.
	var big bytes.Buffer
	for big.Len() <= maxReusedHunkPayloadBytes+(1<<20) {
		big.WriteString("payload\n")
	}
	if err := os.WriteFile(filepath.Join(repo, "big.txt"), big.Bytes(), 0o644); err != nil {
		t.Fatalf("write big.txt: %v", err)
	}
	run("add", "big.txt")
	run("commit", "-m", "big", "--quiet")

	const smallContent = "tiny\n"
	if err := os.WriteFile(filepath.Join(repo, "small.txt"), []byte(smallContent), 0o644); err != nil {
		t.Fatalf("write small.txt: %v", err)
	}
	run("add", "small.txt")
	run("commit", "-m", "small", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"), WithScanMode(ScanModeHunks))
	if err != nil {
		t.Fatalf("NewHistoryScanner: %v", err)
	}
	defer scanner.Close()

	// Record the assembled payload length per path; ScanBlob must see the
	// whole hunk regardless of which side of the release it landed on.
	var (
		mu          sync.Mutex
		bytesByPath = make(map[string]int)
		textByPath  = make(map[string]string)
	)
	rec := blobScannerFunc(func(r io.Reader, meta ScanMeta) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		bytesByPath[meta.Path] += len(data)
		if meta.Path == "small.txt" {
			textByPath[meta.Path] = string(data)
		}
		return nil
	})

	if err := scanner.Scan(nil, rec); err != nil {
		t.Fatalf("Scan in hunk mode: %v", err)
	}

	// big.txt is a whole-file addition, so its hunks carry every line.
	if got, want := bytesByPath["big.txt"], big.Len()-1; got != want {
		t.Errorf("big.txt payload bytes = %d, want %d (lines joined by newline)", got, want)
	}
	if got, want := textByPath["small.txt"], strings.TrimSuffix(smallContent, "\n"); got != want {
		t.Errorf("small.txt payload = %q, want %q", got, want)
	}
}
