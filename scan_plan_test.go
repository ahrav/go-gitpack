// scan_plan_test.go tests the scan-plan pipeline that walks commit history,
// plans per-pack blob scan jobs sorted by offset, deduplicates blobs via a
// seen-set, and streams decompressed blob data to a BlobScanner callback.
// Tests cover deduplication within a run, seen-set respect across runs,
// error propagation from both the scanner and the seen-set, mixed packed
// and loose objects, blob-record serialisation round-trips, monotonic offset
// ordering per pack, and deterministic output under varying GOMAXPROCS.
package objstore

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"golang.org/x/exp/mmap"
)

// memSeenSet is an in-memory test double for the SeenSet interface. It
// optionally returns injected errors from Has and Put, allowing tests to
// exercise error-propagation paths without needing a real external store.
type memSeenSet struct {
	m      map[Hash]struct{}
	hasErr error
	putErr error
}

func (s *memSeenSet) Has(oid Hash) (bool, error) {
	if s.hasErr != nil {
		return false, s.hasErr
	}
	_, ok := s.m[oid]
	return ok, nil
}

func (s *memSeenSet) Put(oid Hash) error {
	if s.putErr != nil {
		return s.putErr
	}
	s.m[oid] = struct{}{}
	return nil
}

// recordingBlobScanner is a test double that implements BlobScanner by
// discarding the blob content and recording every ScanMeta it receives. When
// alwaysErr is non-nil, ScanBlob returns that error immediately, which lets
// tests verify error propagation from the scanner callback.
type recordingBlobScanner struct {
	metas     []ScanMeta
	alwaysErr error
}

func (s *recordingBlobScanner) ScanBlob(r io.Reader, meta ScanMeta) error {
	if s.alwaysErr != nil {
		return s.alwaysErr
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		return err
	}
	s.metas = append(s.metas, meta)
	return nil
}

// flattenJobs concatenates all per-pack job slices into a single slice so
// that tests can iterate over every scheduled job regardless of pack origin.
func flattenJobs(jobsByPack map[*mmap.ReaderAt][]ScanJob) []ScanJob {
	total := 0
	for _, jobs := range jobsByPack {
		total += len(jobs)
	}

	out := make([]ScanJob, 0, total)
	for _, jobs := range jobsByPack {
		out = append(out, jobs...)
	}
	return out
}

// TestPlanScanJobs_DedupWithinRun verifies that a single call to
// planScanJobs never schedules the same blob OID twice, and that every
// returned job has non-zero Blob, non-zero Commit, and a non-empty Path.
func TestPlanScanJobs_DedupWithinRun(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	seen := &memSeenSet{m: make(map[Hash]struct{})}
	jobsByPack, err := scanner.planScanJobs(seen)
	if err != nil {
		t.Fatalf("PlanScanJobs: %v", err)
	}

	jobs := flattenJobs(jobsByPack)
	if len(jobs) == 0 {
		t.Fatalf("expected at least one scan job")
	}

	unique := make(map[Hash]struct{}, len(jobs))
	for _, job := range jobs {
		if job.Blob.IsZero() {
			t.Fatalf("job has zero blob OID")
		}
		if job.Commit.IsZero() {
			t.Fatalf("job has zero commit OID")
		}
		if job.Path == "" {
			t.Fatalf("job path is empty")
		}
		if _, dup := unique[job.Blob]; dup {
			t.Fatalf("duplicate blob scheduled in one run: %s", job.Blob)
		}
		unique[job.Blob] = struct{}{}
	}
}

// TestPlanScanJobs_RespectsSeenSet marks one blob as seen between two
// planning passes and asserts that it does not appear in the second pass.
func TestPlanScanJobs_RespectsSeenSet(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	seen := &memSeenSet{m: make(map[Hash]struct{})}

	first, err := scanner.planScanJobs(seen)
	if err != nil {
		t.Fatalf("first PlanScanJobs: %v", err)
	}
	firstJobs := flattenJobs(first)
	if len(firstJobs) == 0 {
		t.Fatalf("expected at least one scan job in first pass")
	}

	skipBlob := firstJobs[0].Blob
	if err := seen.Put(skipBlob); err != nil {
		t.Fatalf("put seen: %v", err)
	}

	second, err := scanner.planScanJobs(seen)
	if err != nil {
		t.Fatalf("second PlanScanJobs: %v", err)
	}
	for _, job := range flattenJobs(second) {
		if job.Blob == skipBlob {
			t.Fatalf("expected seen blob %s to be skipped", skipBlob)
		}
	}
}

// TestScanPlannedBlobs_ScansAndMarksSeen runs a full scan pass, confirms
// that at least one blob was scanned, verifies that every scanned blob was
// recorded in the seen-set, and then runs a second pass to assert that no
// blobs are rescanned.
func TestScanPlannedBlobs_ScansAndMarksSeen(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	seen := &memSeenSet{m: make(map[Hash]struct{})}
	first := &recordingBlobScanner{}
	if err := scanner.scanBlobsStreaming(seen, first); err != nil {
		t.Fatalf("ScanPlannedBlobs first pass: %v", err)
	}
	if len(first.metas) == 0 {
		t.Fatalf("expected at least one scanned blob in first pass")
	}

	unique := make(map[Hash]struct{}, len(first.metas))
	for _, meta := range first.metas {
		if meta.Blob.IsZero() {
			t.Fatalf("scanned blob has zero OID")
		}
		if meta.Commit.IsZero() {
			t.Fatalf("scanned metadata has zero commit OID")
		}
		if meta.Path == "" {
			t.Fatalf("scanned metadata has empty path")
		}
		if _, dup := unique[meta.Blob]; dup {
			t.Fatalf("blob scanned more than once in a pass: %s", meta.Blob)
		}
		unique[meta.Blob] = struct{}{}
		if _, ok := seen.m[meta.Blob]; !ok {
			t.Fatalf("blob %s was not marked seen", meta.Blob)
		}
	}

	second := &recordingBlobScanner{}
	if err := scanner.scanBlobsStreaming(seen, second); err != nil {
		t.Fatalf("ScanPlannedBlobs second pass: %v", err)
	}
	if got := len(second.metas); got != 0 {
		t.Fatalf("expected no scans in second pass, got %d", got)
	}
}

// TestScanPlannedBlobs_NilScanner confirms that passing a nil BlobScanner
// returns a descriptive error rather than panicking.
func TestScanPlannedBlobs_NilScanner(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	err := scanner.scanBlobsStreaming(nil, nil)
	if err == nil || err.Error() != "scanner is nil" {
		t.Fatalf("expected scanner is nil error, got %v", err)
	}
}

// TestScanPlannedBlobs_AllowsNilSeenSet verifies that a nil seen-set is
// accepted and all blobs are scanned (no deduplication is performed).
func TestScanPlannedBlobs_AllowsNilSeenSet(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	rec := &recordingBlobScanner{}
	if err := scanner.scanBlobsStreaming(nil, rec); err != nil {
		t.Fatalf("ScanPlannedBlobs with nil seen set: %v", err)
	}
	if len(rec.metas) == 0 {
		t.Fatalf("expected at least one scanned blob with nil seen set")
	}
}

// TestScanPlannedBlobs_PropagatesScannerError injects an error into the
// BlobScanner callback and asserts that scanBlobsStreaming propagates it
// and leaves the seen-set empty (no blobs should be marked seen on failure).
func TestScanPlannedBlobs_PropagatesScannerError(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	want := errors.New("scan failed")
	seen := &memSeenSet{m: make(map[Hash]struct{})}
	err := scanner.scanBlobsStreaming(seen, &recordingBlobScanner{alwaysErr: want})
	if !errors.Is(err, want) {
		t.Fatalf("expected scanner error %v, got %v", want, err)
	}
	if len(seen.m) != 0 {
		t.Fatalf("expected no blobs marked seen on scanner error, got %d", len(seen.m))
	}
}

// TestScanPlannedBlobs_SeenSetErrors injects errors into the Has and Put
// methods of the seen-set and confirms that they are propagated.
func TestScanPlannedBlobs_SeenSetErrors(t *testing.T) {
	t.Run("has", func(t *testing.T) {
		scanner := createScannerForRepo(t, "simple-linear")
		defer scanner.Close()

		want := errors.New("has failed")
		seen := &memSeenSet{
			m:      make(map[Hash]struct{}),
			hasErr: want,
		}
		err := scanner.scanBlobsStreaming(seen, &recordingBlobScanner{})
		if !errors.Is(err, want) {
			t.Fatalf("expected seen.Has error %v, got %v", want, err)
		}
	})

	t.Run("put", func(t *testing.T) {
		scanner := createScannerForRepo(t, "simple-linear")
		defer scanner.Close()

		want := errors.New("put failed")
		seen := &memSeenSet{
			m:      make(map[Hash]struct{}),
			putErr: want,
		}
		err := scanner.scanBlobsStreaming(seen, &recordingBlobScanner{})
		if !errors.Is(err, want) {
			t.Fatalf("expected seen.Put error %v, got %v", want, err)
		}
	})
}

// TestScanPlannedBlobs_MixedPackedAndLooseCommits creates a repository
// where the first commit is repacked and the second is loose, runs a full
// blob scan, and asserts that both blobs are found -- one from the pack
// and one from the loose object directory.
func TestScanPlannedBlobs_MixedPackedAndLooseCommits(t *testing.T) {
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
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %s", args, string(out))
		}
	}

	run("init", "--quiet")
	write := func(name, content string) {
		path := filepath.Join(repo, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	write("file.txt", "first\n")
	run("add", "file.txt")
	run("commit", "-m", "first", "--quiet")
	run("repack", "-adq")

	write("file.txt", "first\nsecond\n")
	run("add", "file.txt")
	run("commit", "-m", "second", "--quiet")

	scanner, err := NewHistoryScanner(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatalf("NewHistoryScanner: %v", err)
	}
	defer scanner.Close()

	seen := &memSeenSet{m: make(map[Hash]struct{})}
	rec := &recordingBlobScanner{}
	if err := scanner.scanBlobsStreaming(seen, rec); err != nil {
		t.Fatalf("ScanPlannedBlobs: %v", err)
	}

	if len(rec.metas) != 2 {
		t.Fatalf("expected 2 scanned blobs, got %d", len(rec.metas))
	}
	if len(seen.m) != 2 {
		t.Fatalf("expected 2 seen blobs, got %d", len(seen.m))
	}

	looseFound := false
	for _, meta := range rec.metas {
		if meta.Blob.IsZero() {
			t.Fatalf("scan meta has zero blob OID")
		}
		if meta.Commit.IsZero() {
			t.Fatalf("scan meta has zero commit OID")
		}
		if meta.Path == "" {
			t.Fatalf("scan meta has empty path")
		}
		if _, _, ok := scanner.store.findPackedObject(meta.Blob); !ok {
			looseFound = true
		}
	}
	if !looseFound {
		t.Fatalf("expected at least one scanned blob to be loose")
	}
}

// TestBlobRecord_RoundTrip serialises a blobRecord to a buffer and reads it
// back, asserting that Blob, Commit, and Path survive the round trip.
func TestBlobRecord_RoundTrip(t *testing.T) {
	blob := newHash("blob")
	commit := newHash("commit")
	want := blobRecord{
		Blob:   blob,
		Commit: commit,
		Path:   "path/to/file.txt",
	}

	var buf bytes.Buffer
	if err := writeBlobRecord(&buf, want); err != nil {
		t.Fatalf("writeBlobRecord: %v", err)
	}

	got, err := readBlobRecord(&buf)
	if err != nil {
		t.Fatalf("readBlobRecord: %v", err)
	}
	if got.Blob != want.Blob {
		t.Fatalf("blob = %s, want %s", got.Blob, want.Blob)
	}
	if got.Commit != want.Commit {
		t.Fatalf("commit = %s, want %s", got.Commit, want.Commit)
	}
	if got.Path != want.Path {
		t.Fatalf("path = %q, want %q", got.Path, want.Path)
	}
}

// TestPackedBlobRecord_RoundTrip serialises a packedBlobRecord (which
// includes a pack-file byte offset) and reads it back, verifying all fields.
func TestPackedBlobRecord_RoundTrip(t *testing.T) {
	blob := newHash("blob")
	commit := newHash("commit")
	want := packedBlobRecord{
		Offset: 12345,
		Blob:   blob,
		Commit: commit,
		Path:   "path/to/file.txt",
	}

	var buf bytes.Buffer
	if err := writePackedBlobRecord(&buf, want); err != nil {
		t.Fatalf("writePackedBlobRecord: %v", err)
	}

	got, err := readPackedBlobRecord(&buf)
	if err != nil {
		t.Fatalf("readPackedBlobRecord: %v", err)
	}
	if got.Offset != want.Offset {
		t.Fatalf("offset = %d, want %d", got.Offset, want.Offset)
	}
	if got.Blob != want.Blob {
		t.Fatalf("blob = %s, want %s", got.Blob, want.Blob)
	}
	if got.Commit != want.Commit {
		t.Fatalf("commit = %s, want %s", got.Commit, want.Commit)
	}
	if got.Path != want.Path {
		t.Fatalf("path = %q, want %q", got.Path, want.Path)
	}
}

// TestScanBlobsStreaming_PackedOffsetsMonotonicPerPack streams blobs from a
// 100-commit repository with GOMAXPROCS=4 and verifies the monotonic offset
// invariant: within any single pack file, blobs must be scanned in
// non-decreasing offset order. This invariant is critical for sequential
// I/O performance because it avoids backward seeks within the pack.
func TestScanBlobsStreaming_PackedOffsetsMonotonicPerPack(t *testing.T) {
	prevProcs := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(prevProcs)

	scanner := createScannerForRepo(t, "large-repo")
	defer scanner.Close()

	rec := &recordingBlobScanner{}
	if err := scanner.scanBlobsStreaming(nil, rec); err != nil {
		t.Fatalf("scanBlobsStreaming: %v", err)
	}
	if len(rec.metas) == 0 {
		t.Fatalf("expected at least one scanned blob")
	}

	lastOffset := make(map[*mmap.ReaderAt]uint64)
	for _, meta := range rec.metas {
		pack, off, ok := scanner.store.findPackedObject(meta.Blob)
		if !ok {
			continue
		}
		if prev, exists := lastOffset[pack]; exists && off < prev {
			t.Fatalf("offset regression for pack: got %d after %d", off, prev)
		}
		lastOffset[pack] = off
	}
}

// TestScanBlobsStreaming_ParallelDecodeMatchesSerialOrder runs the same
// large-repo scan twice -- once with GOMAXPROCS=1 and once with GOMAXPROCS=4
// -- and asserts that both runs produce identical ScanMeta sequences. This
// guarantees that parallel decompression does not introduce non-determinism
// in the output order, which callers may rely on for reproducible results.
func TestScanBlobsStreaming_ParallelDecodeMatchesSerialOrder(t *testing.T) {
	run := func(procs int) []ScanMeta {
		prev := runtime.GOMAXPROCS(procs)
		defer runtime.GOMAXPROCS(prev)

		scanner := createScannerForRepo(t, "large-repo")
		defer scanner.Close()

		rec := &recordingBlobScanner{}
		if err := scanner.scanBlobsStreaming(nil, rec); err != nil {
			t.Fatalf("scanBlobsStreaming (GOMAXPROCS=%d): %v", procs, err)
		}

		out := make([]ScanMeta, len(rec.metas))
		copy(out, rec.metas)
		return out
	}

	serial := run(1)
	parallel := run(4)

	if len(serial) != len(parallel) {
		t.Fatalf("scan count mismatch: serial=%d parallel=%d", len(serial), len(parallel))
	}
	for i := range serial {
		if serial[i] != parallel[i] {
			t.Fatalf("scan order mismatch at %d: serial=%+v parallel=%+v", i, serial[i], parallel[i])
		}
	}
}
