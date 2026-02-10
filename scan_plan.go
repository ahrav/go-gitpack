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

// scanBlobsStreaming executes blob-centric scans in streaming mode.
func (hs *HistoryScanner) scanBlobsStreaming(seen SeenSet, scanner BlobScanner) error {
	if scanner == nil {
		return fmt.Errorf("scanner is nil")
	}

	var dedupe map[Hash]struct{}
	if seen == nil {
		dedupe = make(map[Hash]struct{}, 1024)
	}

	return hs.walkCommitsFromRefs(func(c commitInfo) error {
		parentTree, err := hs.firstParentTree(c)
		if err != nil {
			return err
		}
		return hs.scanCommitBlobs(c, parentTree, seen, dedupe, scanner)
	})
}

func (hs *HistoryScanner) scanCommitBlobs(
	c commitInfo,
	parentTree Hash,
	seen SeenSet,
	dedupe map[Hash]struct{},
	scanner BlobScanner,
) error {
	return walkDiff(hs.store, parentTree, c.TreeOID, "", func(path string, oldOID, newOID Hash, mode uint32) error {
		if !isBlobMode(mode) {
			return nil
		}
		if newOID.IsZero() || newOID == oldOID {
			return nil
		}

		if dedupe != nil {
			if _, ok := dedupe[newOID]; ok {
				return nil
			}
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

		data, objType, err := hs.store.getNoCache(newOID)
		if err != nil {
			return fmt.Errorf("load blob %s for %s:%s: %w", newOID, c.OID, path, err)
		}
		if objType != ObjBlob {
			// Tree diffs can surface mode changes; only blobs are scannable.
			return nil
		}

		meta := ScanMeta{
			Blob:   newOID,
			Commit: c.OID,
			Path:   filepath.ToSlash(path),
		}
		if err := scanner.ScanBlob(bytes.NewReader(data), meta); err != nil {
			return fmt.Errorf("scan blob %s for %s:%s: %w", newOID, c.OID, path, err)
		}

		if seen != nil {
			if err := seen.Put(newOID); err != nil {
				return fmt.Errorf("mark seen %s: %w", newOID, err)
			}
		}
		if dedupe != nil {
			dedupe[newOID] = struct{}{}
		}
		return nil
	})
}

// planScanJobs builds pack-sorted blob scan jobs with per-run OID dedupe.
// It is internal and only used by package tests/benchmarks.
func (hs *HistoryScanner) planScanJobs(seen SeenSet) (map[*mmap.ReaderAt][]ScanJob, error) {
	jobsByPack := make(map[*mmap.ReaderAt][]ScanJob, 16)
	scheduled := make(map[Hash]struct{}, 1024)

	err := hs.walkCommitsFromRefs(func(c commitInfo) error {
		parentTree, err := hs.firstParentTree(c)
		if err != nil {
			return err
		}
		return walkDiff(hs.store, parentTree, c.TreeOID, "", func(path string, oldOID, newOID Hash, mode uint32) error {
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
	})
	if err != nil {
		return nil, err
	}

	for p, jobs := range jobsByPack {
		sort.Slice(jobs, func(i, j int) bool { return jobs[i].Offset < jobs[j].Offset })
		jobsByPack[p] = jobs
	}

	return jobsByPack, nil
}
