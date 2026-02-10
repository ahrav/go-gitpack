package objstore

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"sort"

	"golang.org/x/exp/mmap"
)

// SeenSet tracks globally scanned blobs.
type SeenSet interface {
	Has(oid Hash) (bool, error)
	Put(oid Hash) error
}

// ScanJob is one unit of blob-centric scan work.
type ScanJob struct {
	Blob   Hash
	Commit Hash
	Path   string

	Pack   *mmap.ReaderAt
	Offset uint64
}

// ScanMeta carries attribution and identity for a blob scan.
type ScanMeta struct {
	Blob   Hash
	Commit Hash
	Path   string
}

// BlobScanner consumes blob content plus metadata for each planned blob.
type BlobScanner interface {
	ScanBlob(r io.Reader, meta ScanMeta) error
}

// ScanPlannedBlobs executes blob-centric scans planned by PlanScanJobs.
//
// The current implementation materializes each blob via store.get and passes a
// bytes.Reader to scanner. This wires the planner into production usage while a
// future streaming object path is introduced.
func (hs *HistoryScanner) ScanPlannedBlobs(seen SeenSet, scanner BlobScanner) error {
	if scanner == nil {
		return fmt.Errorf("scanner is nil")
	}

	jobsByPack, err := hs.PlanScanJobs(seen)
	if err != nil {
		return err
	}

	packs := make([]*mmap.ReaderAt, 0, len(jobsByPack))
	for p := range jobsByPack {
		packs = append(packs, p)
	}

	// Deterministic pack order for reproducible scans.
	sort.Slice(packs, func(i, j int) bool {
		ai := jobsByPack[packs[i]]
		aj := jobsByPack[packs[j]]
		if len(ai) == 0 || len(aj) == 0 {
			return len(ai) < len(aj)
		}
		if ai[0].Offset != aj[0].Offset {
			return ai[0].Offset < aj[0].Offset
		}
		return ai[0].Blob.String() < aj[0].Blob.String()
	})

	for _, p := range packs {
		for _, job := range jobsByPack[p] {
			data, objType, err := hs.store.get(job.Blob)
			if err != nil {
				return fmt.Errorf("load blob %s for %s:%s: %w", job.Blob, job.Commit, job.Path, err)
			}
			if objType != ObjBlob {
				// Planner should only schedule blob-like paths; tolerate unexpected
				// objects to keep scans moving.
				continue
			}

			meta := ScanMeta{
				Blob:   job.Blob,
				Commit: job.Commit,
				Path:   job.Path,
			}
			if err := scanner.ScanBlob(bytes.NewReader(data), meta); err != nil {
				return fmt.Errorf("scan blob %s for %s:%s: %w", job.Blob, job.Commit, job.Path, err)
			}
			if seen != nil {
				if err := seen.Put(job.Blob); err != nil {
					return fmt.Errorf("mark seen %s: %w", job.Blob, err)
				}
			}
		}
	}

	return nil
}

// PlanScanJobs builds pack-sorted blob scan jobs with per-run OID dedupe.
func (hs *HistoryScanner) PlanScanJobs(seen SeenSet) (map[*mmap.ReaderAt][]ScanJob, error) {
	commits, err := hs.LoadAllCommits()
	if err != nil {
		return nil, err
	}

	treeByCommit := make(map[Hash]Hash, len(commits))
	for _, c := range commits {
		treeByCommit[c.OID] = c.TreeOID
	}

	jobsByPack := make(map[*mmap.ReaderAt][]ScanJob, 16)
	scheduled := make(map[Hash]struct{}, 1024)

	for _, c := range commits {
		parentTree := Hash{}
		if len(c.ParentOIDs) > 0 {
			if t, ok := treeByCommit[c.ParentOIDs[0]]; ok {
				parentTree = t
			}
		}

		err := walkDiff(hs.store, parentTree, c.TreeOID, "", func(path string, oldOID, newOID Hash, mode uint32) error {
			if !isBlobMode(mode) {
				return nil
			}
			if newOID.IsZero() || newOID == oldOID {
				return nil
			}
			if _, ok := scheduled[newOID]; ok {
				return nil
			}
			if seen != nil {
				ok, err := seen.Has(newOID)
				if err != nil {
					return err
				}
				if ok {
					return nil
				}
			}

			p, off, ok := hs.store.findPackedObject(newOID)
			if !ok {
				return nil
			}

			scheduled[newOID] = struct{}{}
			jobsByPack[p] = append(jobsByPack[p], ScanJob{
				Blob:   newOID,
				Commit: c.OID,
				Path:   filepath.ToSlash(path),
				Pack:   p,
				Offset: off,
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	for p, jobs := range jobsByPack {
		sort.Slice(jobs, func(i, j int) bool { return jobs[i].Offset < jobs[j].Offset })
		jobsByPack[p] = jobs
	}

	return jobsByPack, nil
}
