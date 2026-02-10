package objstore

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"golang.org/x/exp/mmap"
)

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

func TestScanPlannedBlobs_NilScanner(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	err := scanner.scanBlobsStreaming(nil, nil)
	if err == nil || err.Error() != "scanner is nil" {
		t.Fatalf("expected scanner is nil error, got %v", err)
	}
}

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

func TestScanBlobsStreaming_PackedOffsetsMonotonicPerPack(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
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
