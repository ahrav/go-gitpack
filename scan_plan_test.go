package objstore

import (
	"errors"
	"io"
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
	jobsByPack, err := scanner.PlanScanJobs(seen)
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

	first, err := scanner.PlanScanJobs(seen)
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

	second, err := scanner.PlanScanJobs(seen)
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
	if err := scanner.ScanPlannedBlobs(seen, first); err != nil {
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
	if err := scanner.ScanPlannedBlobs(seen, second); err != nil {
		t.Fatalf("ScanPlannedBlobs second pass: %v", err)
	}
	if got := len(second.metas); got != 0 {
		t.Fatalf("expected no scans in second pass, got %d", got)
	}
}

func TestScanPlannedBlobs_NilScanner(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	err := scanner.ScanPlannedBlobs(nil, nil)
	if err == nil || err.Error() != "scanner is nil" {
		t.Fatalf("expected scanner is nil error, got %v", err)
	}
}

func TestScanPlannedBlobs_AllowsNilSeenSet(t *testing.T) {
	scanner := createScannerForRepo(t, "simple-linear")
	defer scanner.Close()

	rec := &recordingBlobScanner{}
	if err := scanner.ScanPlannedBlobs(nil, rec); err != nil {
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
	err := scanner.ScanPlannedBlobs(seen, &recordingBlobScanner{alwaysErr: want})
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
		err := scanner.ScanPlannedBlobs(seen, &recordingBlobScanner{})
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
		err := scanner.ScanPlannedBlobs(seen, &recordingBlobScanner{})
		if !errors.Is(err, want) {
			t.Fatalf("expected seen.Put error %v, got %v", want, err)
		}
	})
}
